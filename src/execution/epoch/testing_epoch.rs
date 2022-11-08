use std::collections::HashMap;
use std::task::Poll;

use pyo3::prelude::*;
use timely::dataflow::{
    operators::generic::builder_rc::OperatorBuilder, ProbeHandle, Scope, Stream,
};

use crate::recovery::model::*;
use crate::recovery::operators::FlowChangeStream;
use crate::{common::StringResult, inputs::InputReader};

use super::{EpochBuilder, EpochConfig};

/// Use for deterministic epochs in tests. Increment epoch by 1 after
/// each item.
///
/// _This requires all workers to have exactly the same number of
/// input items! Otherwise the dataflow will hang!_
///
/// You almost assuredly do not want to use this unless you are
/// writing tests of the recovery system.
///
/// Returns:
///
///   Config object. Pass this as the `epoch_config` parameter of
///   your execution entry point.
#[pyclass(module="bytewax.execution", extends=EpochConfig)]
#[pyo3(text_signature = "()")]
#[derive(Clone)]
pub(crate) struct TestingEpochConfig {}

#[pymethods]
impl TestingEpochConfig {
    /// Tell pytest to ignore this class, even though it starts with
    /// the name "Test".
    #[allow(non_upper_case_globals)]
    #[classattr]
    const __test__: bool = false;

    #[new]
    #[args()]
    fn new() -> (Self, EpochConfig) {
        (Self {}, EpochConfig {})
    }

    /// Return a representation of this class as a PyDict.
    fn __getstate__(&self) -> HashMap<&str, Py<PyAny>> {
        Python::with_gil(|py| {        
            HashMap::from([
                ("type", "TestingEpochConfig".into_py(py))
            ])
        })
    }

    /// Unpickle from a PyDict.
    fn __setstate__(&mut self, _state: &PyAny) -> PyResult<()> {
        Ok(())
    }
}

impl<S> EpochBuilder<S> for TestingEpochConfig
where
    S: Scope<Timestamp = u64>,
{
    fn build(
        &self,
        _py: Python,
        scope: &S,
        step_id: StepId,
        state_key: StateKey,
        mut reader: Box<dyn InputReader<crate::pyo3_extensions::TdPyAny>>,
        start_at: S::Timestamp,
        probe: &ProbeHandle<S::Timestamp>,
    ) -> StringResult<(
        Stream<S, crate::pyo3_extensions::TdPyAny>,
        FlowChangeStream<S>,
    )> {
        let mut op_builder = OperatorBuilder::new(format!("{step_id}"), scope.clone());

        let (mut output_wrapper, output_stream) = op_builder.new_output();
        let (mut change_wrapper, change_stream) = op_builder.new_output();

        let probe = probe.clone();
        let info = op_builder.operator_info();
        let activator = scope.activator_for(&info.address[..]);

        let flow_key = FlowKey(step_id, state_key);

        op_builder.build(move |mut init_caps| {
            let mut change_cap = init_caps.pop().map(|cap| cap.delayed(&start_at));
            let mut output_cap = init_caps.pop().map(|cap| cap.delayed(&start_at));

            let mut eof = false;

            move |_input_frontiers| {
                if let (Some(output_cap), Some(change_cap)) =
                    (output_cap.as_mut(), change_cap.as_mut())
                {
                    assert!(output_cap.time() == change_cap.time());
                    let epoch = output_cap.time();

                    if !probe.less_than(epoch) {
                        match reader.next() {
                            Poll::Pending => {}
                            Poll::Ready(None) => {
                                eof = true;
                            }
                            Poll::Ready(Some(item)) => {
                                output_wrapper.activate().session(&output_cap).give(item);

                                // Snapshot just before incrementing epoch
                                // to get the "end of the epoch" state.
                                change_wrapper.activate().session(&change_cap).give(KChange(
                                    flow_key.clone(),
                                    Change::Upsert(reader.snapshot()),
                                ));

                                let next_epoch = epoch + 1;

                                output_cap.downgrade(&next_epoch);
                                change_cap.downgrade(&next_epoch);
                            }
                        }
                    }
                }

                if eof {
                    output_cap = None;
                    change_cap = None;
                } else {
                    activator.activate();
                }
            }
        });

        Ok((output_stream, change_stream))
    }
}
