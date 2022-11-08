use std::collections::HashMap;
use std::task::Poll;
use std::time::Instant;

use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3::PyResult;
use pyo3::types::PyDict;
use timely::dataflow::operators::generic::builder_rc::OperatorBuilder;
use timely::dataflow::ProbeHandle;
use timely::dataflow::Scope;
use timely::dataflow::Stream;

use crate::common::StringResult;
use crate::inputs::InputReader;
use crate::pickle_extract;
use crate::recovery::model::*;
use crate::recovery::operators::FlowChangeStream;

use super::{EpochBuilder, EpochConfig};

/// Increment epochs at regular system time intervals.
///
/// This is the default with 10 second epoch intervals if no
/// `epoch_config` is passed to your execution entry point.
///
/// Args:
///
///   epoch_length (datetime.timedelta): System time length of each
///       epoch.
///
/// Returns:
///
///   Config object. Pass this as the `epoch_config` parameter of
///   your execution entry point.
#[pyclass(module="bytewax.execution", extends=EpochConfig)]
#[pyo3(text_signature = "(epoch_length)")]
#[derive(Clone)]
pub(crate) struct PeriodicEpochConfig {
    #[pyo3(get)]
    pub(crate) epoch_length: chrono::Duration,
}

#[pymethods]
impl PeriodicEpochConfig {
    #[new]
    #[args(epoch_length)]
    pub(crate) fn new(epoch_length: chrono::Duration) -> (Self, EpochConfig) {
        (Self { epoch_length }, EpochConfig {})
    }

    /// Return a representation of this class as a PyDict.
    fn __getstate__(&self) -> HashMap<&str, Py<PyAny>> {
        Python::with_gil(|py| {        
            HashMap::from([
                ("type", "PeriodicEpochConfig".into_py(py)),
                ("epoch_length", self.epoch_length.into_py(py))
            ])
        })
    }

    /// Egregious hack see [`SqliteRecoveryConfig::__getnewargs__`].
    fn __getnewargs__(&self) -> (chrono::Duration,) {
        (chrono::Duration::zero(),)
    }

    /// Unpickle from a PyDict.
    fn __setstate__(&mut self, state: &PyAny) -> PyResult<()> {
        let dict: &PyDict = state.downcast()?;
        pickle_extract!(self, dict, epoch_length);
        Ok(())
    }
}

impl<S> EpochBuilder<S> for PeriodicEpochConfig
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
        let epoch_length = self
            .epoch_length
            .to_std()
            .map_err(|err| format!("Invalid epoch length: {err:?}"))?;

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
            let mut epoch_started = Instant::now();

            move |_input_frontiers| {
                if let (Some(output_cap), Some(change_cap)) =
                    (output_cap.as_mut(), change_cap.as_mut())
                {
                    assert!(output_cap.time() == change_cap.time());
                    let epoch = output_cap.time();

                    if !probe.less_than(epoch) {
                        if epoch_started.elapsed() > epoch_length {
                            // Snapshot just before incrementing epoch to
                            // get the "end of the epoch state".
                            change_wrapper
                                .activate()
                                .session(&change_cap)
                                .give(KChange(flow_key.clone(), Change::Upsert(reader.snapshot())));

                            let next_epoch = epoch + 1;

                            output_cap.downgrade(&next_epoch);
                            change_cap.downgrade(&next_epoch);

                            epoch_started = Instant::now();
                        }

                        match reader.next() {
                            Poll::Pending => {}
                            Poll::Ready(None) => {
                                eof = true;
                            }
                            Poll::Ready(Some(item)) => {
                                output_wrapper.activate().session(&output_cap).give(item);
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
