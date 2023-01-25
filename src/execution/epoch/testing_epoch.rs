use std::collections::HashMap;
use std::task::Poll;

use pyo3::prelude::*;
use timely::dataflow::{
    operators::generic::builder_rc::OperatorBuilder, ProbeHandle, Scope, Stream,
};

use crate::inputs::PartBundle;
use crate::pyo3_extensions::TdPyAny;
use crate::recovery::model::*;
use crate::recovery::operators::FlowChangeStream;

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
        Python::with_gil(|py| HashMap::from([("type", "TestingEpochConfig".into_py(py))]))
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
        mut parts: PartBundle,
        start_at: ResumeEpoch,
        probe: &ProbeHandle<u64>,
    ) -> PyResult<(Stream<S, TdPyAny>, FlowChangeStream<S>)> {
        let mut op_builder = OperatorBuilder::new(step_id.0.to_string(), scope.clone());

        let (mut output_wrapper, output_stream) = op_builder.new_output();
        let (mut change_wrapper, change_stream) = op_builder.new_output();

        let probe = probe.clone();
        let info = op_builder.operator_info();
        let activator = scope.activator_for(&info.address[..]);

        op_builder.build(move |mut init_caps| {
            let change_cap = init_caps.pop().map(|cap| cap.delayed(&start_at.0)).unwrap();
            let output_cap = init_caps.pop().map(|cap| cap.delayed(&start_at.0)).unwrap();

            let mut caps = Some((output_cap, change_cap));

            move |_input_frontiers| {
                caps = if let Some((output_cap, change_cap)) = caps.clone() {
                    assert!(output_cap.time() == change_cap.time());
                    let epoch = output_cap.time();

                    let mut eof = false;
                    let mut advance = false;

                    if !probe.less_than(epoch) {
                        match parts.next() {
                            Poll::Pending => {}
                            Poll::Ready(None) => {
                                eof = true;
                                advance = true;
                            }
                            Poll::Ready(Some(item)) => {
                                output_wrapper.activate().session(&output_cap).give(item);
                                advance = true;
                            }
                        }
                    }

                    // If the the current epoch will be over, snapshot
                    // to get "end of the epoch state".
                    if advance || eof {
                        let kchanges = parts
                            .snapshot()
                            .into_iter()
                            .map(|(state_key, snap)| (FlowKey(step_id.clone(), state_key), snap))
                            .map(|(flow_key, snap)| KChange(flow_key, Change::Upsert(snap)));
                        change_wrapper
                            .activate()
                            .session(&change_cap)
                            .give_iterator(kchanges);
                    }

                    if eof {
                        tracing::trace!("Input {step_id:?} reached EOF");
                        None
                    } else if advance {
                        let next_epoch = epoch + 1;
                        tracing::trace!("Input {step_id:?} advancing to epoch {next_epoch:?}");
                        Some((
                            output_cap.delayed(&next_epoch),
                            change_cap.delayed(&next_epoch),
                        ))
                    } else {
                        Some((output_cap, change_cap))
                    }
                } else {
                    None
                };

                // Wake up constantly, because we never know when
                // input will have new data.
                if caps.is_some() {
                    activator.activate();
                }
            }
        });

        Ok((output_stream, change_stream))
    }
}
