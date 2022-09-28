use std::fmt::Debug;
use std::task::Poll;

use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use timely::{
    dataflow::{operators::generic::builder_rc::OperatorBuilder, ProbeHandle, Scope, Stream},
    Data,
};

use crate::{
    inputs::InputReader,
    recovery::{State, StateKey, StateOp, StateRecoveryKey, StateUpdate, StepId},
};

use super::EpochConfig;

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

    /// Pickle as a tuple.
    fn __getstate__(&self) -> (&str,) {
        ("TestingEpochConfig",)
    }

    /// Unpickle from tuple of arguments.
    fn __setstate__(&mut self, state: &PyAny) -> PyResult<()> {
        if let Ok(("TestingEpochConfig",)) = state.extract() {
            Ok(())
        } else {
            Err(PyValueError::new_err(format!(
                "bad pickle contents for TestingEpochConfig: {state:?}"
            )))
        }
    }
}

/// Input source that increments the epoch after each item.
pub(crate) fn testing_epoch_source<S, D>(
    scope: &S,
    step_id: StepId,
    state_key: StateKey,
    mut reader: Box<dyn InputReader<D>>,
    start_at: S::Timestamp,
    probe: &ProbeHandle<S::Timestamp>,
) -> (Stream<S, D>, Stream<S, StateUpdate<S::Timestamp>>)
where
    S: Scope<Timestamp = u64>,
    D: Data + Debug,
{
    let mut op_builder = OperatorBuilder::new(format!("{step_id}"), scope.clone());

    let (mut output_wrapper, output_stream) = op_builder.new_output();
    let (mut state_update_wrapper, state_update_stream) = op_builder.new_output();

    let probe = probe.clone();
    let info = op_builder.operator_info();
    let activator = scope.activator_for(&info.address[..]);

    op_builder.build(move |mut init_caps| {
        let mut state_update_cap = init_caps.pop().map(|cap| cap.delayed(&start_at));
        let mut output_cap = init_caps.pop().map(|cap| cap.delayed(&start_at));

        let mut eof = false;

        move |_input_frontiers| {
            if let (Some(output_cap), Some(state_update_cap)) =
                (output_cap.as_mut(), state_update_cap.as_mut())
            {
                assert!(output_cap.time() == state_update_cap.time());
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
                            let state_bytes = reader.snapshot();
                            let recovery_key = StateRecoveryKey {
                                step_id: step_id.clone(),
                                state_key: state_key.clone(),
                                epoch: epoch.clone(),
                            };
                            let op = StateOp::Upsert(State {
                                state_bytes,
                                next_awake: None,
                            });
                            let update = StateUpdate(recovery_key, op);
                            state_update_wrapper
                                .activate()
                                .session(&state_update_cap)
                                .give(update);

                            let next_epoch = epoch + 1;

                            output_cap.downgrade(&next_epoch);
                            state_update_cap.downgrade(&next_epoch);
                        }
                    }
                }
            }

            if eof {
                output_cap = None;
                state_update_cap = None;
            } else {
                activator.activate();
            }
        }
    });

    (output_stream, state_update_stream)
}
