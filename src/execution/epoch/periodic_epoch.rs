use std::task::Poll;
use std::time::Instant;

use chrono::Duration;
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use timely::dataflow::operators::generic::builder_rc::OperatorBuilder;
use timely::dataflow::ProbeHandle;
use timely::dataflow::Scope;
use timely::dataflow::Stream;

use crate::add_pymethods;
use crate::inputs::PartBundle;
use crate::pyo3_extensions::TdPyAny;
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

add_pymethods!(
    PeriodicEpochConfig,
    parent: EpochConfig,
    py_args: (epoch_length),
    args {
        epoch_length: Duration => Duration::zero()
    }
);

impl<S> EpochBuilder<S> for PeriodicEpochConfig
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
        let epoch_length = self
            .epoch_length
            .to_std()
            .map_err(|err| PyValueError::new_err(format!("Invalid epoch length: {err:?}")))?;

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
            let mut epoch_started = Instant::now();

            move |_input_frontiers| {
                caps = if let Some((output_cap, change_cap)) = caps.clone() {
                    assert!(output_cap.time() == change_cap.time());
                    let epoch = output_cap.time();

                    let mut eof = false;

                    if !probe.less_than(epoch) {
                        match parts.next() {
                            Poll::Pending => {}
                            Poll::Ready(None) => {
                                eof = true;
                            }
                            Poll::Ready(Some(item)) => {
                                output_wrapper.activate().session(&output_cap).give(item);
                            }
                        }
                    }
                    let advance = epoch_started.elapsed() > epoch_length;

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
                        epoch_started = Instant::now();
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
