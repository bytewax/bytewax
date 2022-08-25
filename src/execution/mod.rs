//! Internal code for dataflow execution.
//!
//! For a user-centric version of how to execute dataflows, read the
//! the `bytewax.execution` Python module docstring. Read that first.
//!
//! [`worker_main()`] for the root of all the internal action here.
//!
//! Dataflow Building
//! -----------------
//!
//! The "blueprint" of a dataflow in [`crate::dataflow::Dataflow`] is
//! compiled into a Timely dataflow in [`build_production_dataflow`].
//!
//! See [`crate::recovery`] for a description of the recovery
//! components added to the Timely dataflow.
//!
//! Source Architecture
//! -------------------
//!
//! The input system described in [`crate::inputs`] only deals with
//! "what is the next item of data for this worker?" The source
//! operators here control the epochs used in the dataflow. They call
//! out to [`crate::inputs::InputReader`] impls to actually get the
//! next item.
//!
//! This system follows our standard pattern of having parallel Python
//! config objects and Rust impl structs for each trait of behavior we
//! want. E.g. [`PeriodicEpochConfig`] represents a token in Python
//! for how to create a [`periodic_epoch_source`].

use crate::dataflow::{Dataflow, Step};
use crate::StringResult;
use crate::inputs::build_input_reader;
use crate::inputs::InputReader;
use crate::operators::*;
use crate::outputs::build_output_writer;
use crate::outputs::capture;
use crate::pyo3_extensions::{extract_state_pair, wrap_state_pair, TdPyAny};
use crate::recovery::StatefulUnary;
use crate::recovery::WriteProgress;
use crate::recovery::WriteState;
use crate::recovery::{
    build_recovery_readers, build_recovery_writers, build_resume_epoch_calc_dataflow,
    build_state_loading_dataflow,
};
use crate::recovery::{default_recovery_config, ProgressWriter};
use crate::recovery::{CollectGarbage, EpochData};
use crate::recovery::{ProgressReader, StateCollector};
use crate::recovery::{RecoveryConfig, StepId};
use crate::recovery::{RecoveryStoreSummary, StateWriter};
use crate::recovery::{StateBytes, StateKey};
use crate::recovery::{StateReader, StateUpdate};
use crate::window::{build_clock_builder, build_windower_builder, StatefulWindowUnary};
use log::debug;
use pyo3::exceptions::{PyRuntimeError, PyValueError};
use pyo3::prelude::*;
use std::collections::HashMap;
use std::fmt::Debug;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::task::Poll;
use std::thread;
use std::time::{Duration, Instant};
use timely::communication::Allocate;
use timely::dataflow::operators::generic::builder_rc::OperatorBuilder;
use timely::dataflow::{operators::*, Stream};
use timely::dataflow::{ProbeHandle, Scope};
use timely::progress::Timestamp;
use timely::worker::Worker;
use timely::Data;

/// Base class for an epoch config.
///
/// These define how epochs are assigned on source input data. You
/// should only need to set this if you are testing the recovery
/// system or are doing deep exactly-once integration work. Changing
/// this does not change the semantics of any of the operators.
///
/// Use a specific subclass of this for the epoch definition you need.
#[pyclass(module = "bytewax.execution", subclass)]
#[pyo3(text_signature = "()")]
pub(crate) struct EpochConfig;

impl EpochConfig {
    /// Create an "empty" [`Self`] just for use in `__getnewargs__`.
    #[allow(dead_code)]
    pub(crate) fn pickle_new(py: Python) -> Py<Self> {
        PyCell::new(py, EpochConfig {}).unwrap().into()
    }
}

#[pymethods]
impl EpochConfig {
    #[new]
    fn new() -> Self {
        Self {}
    }

    /// Pickle as a tuple.
    fn __getstate__(&self) -> (&str,) {
        ("EpochConfig",)
    }

    /// Unpickle from tuple of arguments.
    fn __setstate__(&mut self, state: &PyAny) -> PyResult<()> {
        if let Ok(("EpochConfig",)) = state.extract() {
            Ok(())
        } else {
            Err(PyValueError::new_err(format!(
                "bad pickle contents for EpochConfig: {state:?}"
            )))
        }
    }
}

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
struct TestingEpochConfig {}

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
#[pyclass(module="bytewax.window", extends=EpochConfig)]
#[pyo3(text_signature = "(epoch_length)")]
struct PeriodicEpochConfig {
    #[pyo3(get)]
    epoch_length: pyo3_chrono::Duration,
}

#[pymethods]
impl PeriodicEpochConfig {
    #[new]
    #[args(epoch_length)]
    fn new(epoch_length: pyo3_chrono::Duration) -> (Self, EpochConfig) {
        (Self { epoch_length }, EpochConfig {})
    }

    /// Pickle as a tuple.
    fn __getstate__(&self) -> (&str, pyo3_chrono::Duration) {
        ("PeriodicEpochConfig", self.epoch_length)
    }

    /// Egregious hack see [`SqliteRecoveryConfig::__getnewargs__`].
    fn __getnewargs__(&self) -> (pyo3_chrono::Duration,) {
        (pyo3_chrono::Duration(chrono::Duration::zero()),)
    }

    /// Unpickle from tuple of arguments.
    fn __setstate__(&mut self, state: &PyAny) -> PyResult<()> {
        if let Ok(("PeriodicEpochConfig", epoch_length)) = state.extract() {
            self.epoch_length = epoch_length;
            Ok(())
        } else {
            Err(PyValueError::new_err(format!(
                "bad pickle contents for PeriodicEpochConfig: {state:?}"
            )))
        }
    }
}

/// Default to 10 second periodic epochs.
pub(crate) fn default_epoch_config() -> Py<EpochConfig> {
    Python::with_gil(|py| {
        PyCell::new(
            py,
            PeriodicEpochConfig::new(pyo3_chrono::Duration(chrono::Duration::seconds(10))),
        )
        .unwrap()
        .extract()
        .unwrap()
    })
}

/// Input source that increments the epoch after each item.
pub(crate) fn testing_epoch_source<S: Scope<Timestamp = u64>, D: Data + Debug>(
    scope: &S,
    step_id: StepId,
    key: StateKey,
    mut reader: Box<dyn InputReader<D>>,
    start_at: S::Timestamp,
    probe: &ProbeHandle<S::Timestamp>,
) -> (Stream<S, D>, Stream<S, EpochData>) {
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
                            let update = (
                                key.clone(),
                                (step_id.clone(), StateUpdate::Upsert(state_bytes)),
                            );
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

/// Input source that increments the epoch periodically by system
/// time.
pub(crate) fn periodic_epoch_source<S, D: Data + Debug>(
    scope: &S,
    step_id: StepId,
    key: StateKey,
    mut reader: Box<dyn InputReader<D>>,
    start_at: S::Timestamp,
    probe: &ProbeHandle<S::Timestamp>,
    epoch_length: Duration,
) -> (Stream<S, D>, Stream<S, EpochData>)
where
    S: Scope<Timestamp = u64>,
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
        let mut epoch_started = Instant::now();

        move |_input_frontiers| {
            if let (Some(output_cap), Some(state_update_cap)) =
                (output_cap.as_mut(), state_update_cap.as_mut())
            {
                assert!(output_cap.time() == state_update_cap.time());
                let epoch = output_cap.time();

                if !probe.less_than(epoch) {
                    if epoch_started.elapsed() > epoch_length {
                        // Snapshot just before incrementing epoch to
                        // get the "end of the epoch state".
                        let state_bytes = reader.snapshot();
                        let update = (
                            key.clone(),
                            (step_id.clone(), StateUpdate::Upsert(state_bytes)),
                        );
                        state_update_wrapper
                            .activate()
                            .session(&state_update_cap)
                            .give(update);

                        let next_epoch = epoch + 1;

                        output_cap.downgrade(&next_epoch);
                        state_update_cap.downgrade(&next_epoch);

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
                state_update_cap = None;
            } else {
                activator.activate();
            }
        }
    });

    (output_stream, state_update_stream)
}

/// Generate the [`StateKey`] that represents this worker and
/// determine if there's any resume state.
fn resume_input_state(
    worker_index: usize,
    _worker_count: usize,
    mut step_to_resume_state_bytes: HashMap<StateKey, StateBytes>,
) -> (Option<StateBytes>, StateKey) {
    let key = StateKey::Worker(worker_index);

    let resume_state_bytes = step_to_resume_state_bytes.remove(&key);

    (resume_state_bytes, key)
}

fn build_source<S>(
    py: Python,
    epoch_config: Py<EpochConfig>,
    scope: &S,
    step_id: StepId,
    key: StateKey,
    reader: Box<dyn InputReader<TdPyAny>>,
    start_at: S::Timestamp,
    probe: &ProbeHandle<S::Timestamp>,
) -> StringResult<(Stream<S, TdPyAny>, Stream<S, EpochData>)>
where
    S: Scope<Timestamp = u64>,
{
    let epoch_config = epoch_config.as_ref(py);

    if let Ok(testing_config) = epoch_config.downcast::<PyCell<TestingEpochConfig>>() {
        let _testing_config = testing_config.borrow();

        Ok(testing_epoch_source(
            scope, step_id, key, reader, start_at, probe,
        ))
    } else if let Ok(periodic_config) = epoch_config.downcast::<PyCell<PeriodicEpochConfig>>() {
        let periodic_config = periodic_config.borrow();

        let epoch_length = periodic_config
            .epoch_length
            .0
            .to_std()
            .map_err(|err| format!("Invalid epoch length: {err:?}"))?;

        Ok(periodic_epoch_source(
            scope,
            step_id,
            key,
            reader,
            start_at,
            probe,
            epoch_length,
        ))
    } else {
        let pytype = epoch_config.get_type();
        Err(format!("Unknown epoch_config type: {pytype}"))
    }
}

/// Turn the abstract blueprint for a dataflow into a Timely dataflow
/// so it can be executed.
///
/// This is more complicated than a 1:1 translation of Bytewax
/// concepts to Timely, as we are using Timely as a basis to implement
/// more-complicated Bytewax features like input builders and
/// recovery.
fn build_production_dataflow<A: Allocate>(
    py: Python,
    worker: &mut Worker<A>,
    epoch_config: Py<EpochConfig>,
    resume_epoch: u64,
    mut step_to_key_to_resume_state_bytes: HashMap<StepId, HashMap<StateKey, StateBytes>>,
    recovery_store_summary: RecoveryStoreSummary<u64>,
    flow: Py<Dataflow>,
    progress_writer: Box<dyn ProgressWriter<u64>>,
    state_writer: Box<dyn StateWriter<u64>>,
    state_collector: Box<dyn StateCollector<u64>>,
) -> StringResult<ProbeHandle<u64>> {
    let worker_index = worker.index();
    let worker_count = worker.peers();

    worker.dataflow(|scope| {
        let flow = flow.as_ref(py).borrow();

        let mut probe = ProbeHandle::new();

        let mut input_streams = Vec::new();
        let mut state_update_streams = Vec::new();
        let mut capture_streams = Vec::new();

        // Start with an "empty" stream. We might overwrite it with
        // input later.
        let mut stream = None.to_stream(scope);

        for step in &flow.steps {
            // All these closure lifetimes are static, so tell
            // Python's GC that there's another pointer to the
            // mapping function that's going to hang around
            // for a while when it's moved into the closure.
            let step = step.clone();
            match step {
                Step::Input {
                    step_id,
                    input_config,
                } => {
                    let (resume_state_bytes, recovery_key) = resume_input_state(
                        worker_index,
                        worker_count,
                        step_to_key_to_resume_state_bytes
                            .remove(&step_id)
                            .unwrap_or_default(),
                    );

                    let input_reader = build_input_reader(
                        py,
                        input_config,
                        worker_index,
                        worker_count,
                        resume_state_bytes,
                    )?;
                    let (downstream, update_stream) = build_source(
                        py,
                        epoch_config.clone_ref(py),
                        scope,
                        step_id,
                        recovery_key,
                        input_reader,
                        resume_epoch,
                        &probe,
                    )?;
                    input_streams.push(downstream.clone());
                    stream = downstream;
                    state_update_streams.push(update_stream);
                }
                Step::Map { mapper } => {
                    stream = stream.map(move |item| map(&mapper, item));
                }
                Step::FlatMap { mapper } => {
                    stream = stream.flat_map(move |item| flat_map(&mapper, item));
                }
                Step::Filter { predicate } => {
                    stream = stream.filter(move |item| filter(&predicate, item));
                }
                Step::FoldWindow {
                    step_id,
                    clock_config,
                    window_config,
                    builder,
                    folder,
                } => {
                    let key_to_resume_state_bytes = step_to_key_to_resume_state_bytes
                        .remove(&step_id)
                        .unwrap_or_default();

                    let clock_builder = build_clock_builder(py, clock_config)?;
                    let windower_builder = build_windower_builder(py, window_config)?;

                    let (downstream, update_stream) =
                        stream.map(extract_state_pair).stateful_window_unary(
                            step_id,
                            clock_builder,
                            windower_builder,
                            FoldWindowLogic::new(builder, folder),
                            key_to_resume_state_bytes,
                        );

                    stream = downstream
                        .map(|(key, result)| {
                            result
                                .map(|value| (key.clone(), value))
                                .map_err(|err| (key.clone(), err))
                        })
                        // For now, filter to just reductions and
                        // ignore late values.
                        .ok()
                        .map(wrap_state_pair);
                    state_update_streams.push(update_stream);
                }
                Step::Inspect { inspector } => {
                    stream = stream.inspect(move |item| inspect(&inspector, item));
                }
                Step::InspectEpoch { inspector } => {
                    stream = stream
                        .inspect_time(move |epoch, item| inspect_epoch(&inspector, epoch, item));
                }
                Step::Reduce {
                    step_id,
                    reducer,
                    is_complete,
                } => {
                    let key_to_resume_state_bytes = step_to_key_to_resume_state_bytes
                        .remove(&step_id)
                        .unwrap_or_default();

                    let (downstream, update_stream) =
                        stream.map(extract_state_pair).stateful_unary(
                            step_id,
                            ReduceLogic::builder(reducer, is_complete),
                            key_to_resume_state_bytes,
                        );
                    stream = downstream.map(wrap_state_pair);
                    state_update_streams.push(update_stream);
                }
                Step::ReduceWindow {
                    step_id,
                    clock_config,
                    window_config,
                    reducer,
                } => {
                    let key_to_resume_state_bytes = step_to_key_to_resume_state_bytes
                        .remove(&step_id)
                        .unwrap_or_default();

                    let clock_builder = build_clock_builder(py, clock_config)?;
                    let windower_builder = build_windower_builder(py, window_config)?;

                    let (downstream, update_stream) =
                        stream.map(extract_state_pair).stateful_window_unary(
                            step_id,
                            clock_builder,
                            windower_builder,
                            ReduceWindowLogic::builder(reducer),
                            key_to_resume_state_bytes,
                        );

                    stream = downstream
                        .map(|(key, result)| {
                            result
                                .map(|value| (key.clone(), value))
                                .map_err(|err| (key.clone(), err))
                        })
                        // For now, filter to just reductions and
                        // ignore late values.
                        .ok()
                        .map(wrap_state_pair);
                    state_update_streams.push(update_stream);
                }
                Step::StatefulMap {
                    step_id,
                    builder,
                    mapper,
                } => {
                    let key_to_resume_state_bytes = step_to_key_to_resume_state_bytes
                        .remove(&step_id)
                        .unwrap_or_default();

                    let (downstream, update_stream) =
                        stream.map(extract_state_pair).stateful_unary(
                            step_id,
                            StatefulMapLogic::builder(builder, mapper),
                            key_to_resume_state_bytes,
                        );
                    stream = downstream.map(wrap_state_pair);
                    state_update_streams.push(update_stream);
                }
                Step::Capture { output_config } => {
                    let mut writer =
                        build_output_writer(py, output_config, worker_index, worker_count)?;

                    let capture =
                        stream.inspect_time(move |epoch, item| capture(&mut writer, epoch, item));

                    capture_streams.push(capture.clone());
                    stream = capture;
                }
            }
        }

        if input_streams.is_empty() {
            return Err("Dataflow needs to contain at least one input".to_string());
        }
        if capture_streams.is_empty() {
            return Err("Dataflow needs to contain at least one capture".to_string());
        }
        if !step_to_key_to_resume_state_bytes.is_empty() {
            return Err(format!(
                "Recovery data had unknown step IDs: {:?}",
                step_to_key_to_resume_state_bytes.keys(),
            ));
        }

        let state_backup_stream = scope
            .concatenate(state_update_streams)
            .write_state_with(state_writer);

        let capture_stream = scope.concatenate(capture_streams);

        let dataflow_frontier_backup_stream = capture_stream
            // TODO: Can we only downstream progress messages? Doing this
            // flat_map trick results in nothing (not even progress)
            // downstream.
            //.flat_map(|_| Option::<()>::None)
            //.concat(&state_backup_stream.flat_map(|_| Option::<()>::None))
            .map(|_| ())
            .concat(&state_backup_stream.map(|_| ()))
            .write_progress_with(progress_writer)
            .broadcast();

        state_backup_stream
            .collect_garbage(
                recovery_store_summary,
                state_collector,
                dataflow_frontier_backup_stream,
            )
            .probe_with(&mut probe);

        Ok(probe)
    })
}

/// Run a dataflow which uses sources until complete.
fn run_until_done<A: Allocate, T: Timestamp>(
    worker: &mut Worker<A>,
    interrupt_flag: &AtomicBool,
    probe: ProbeHandle<T>,
) {
    while !interrupt_flag.load(Ordering::Relaxed) && !probe.done() {
        worker.step();
    }
}

fn build_and_run_resume_epoch_calc_dataflow<A: Allocate>(
    worker: &mut Worker<A>,
    interrupt_flag: &AtomicBool,
    progress_reader: Box<dyn ProgressReader<u64>>,
) -> StringResult<u64> {
    let (resume_epoch_tx, resume_epoch_rx) = std::sync::mpsc::channel();
    let probe = build_resume_epoch_calc_dataflow(worker, progress_reader, resume_epoch_tx)?;

    run_until_done(worker, interrupt_flag, probe);

    let resume_epoch = match resume_epoch_rx.recv() {
        Ok(resume_epoch) => {
            debug!("Loaded resume epoch {resume_epoch}");
            resume_epoch
        }
        Err(_) => {
            let default_epoch = <u64 as Timestamp>::minimum();
            debug!("No resume epoch calculated; probably empty recovery store; starting at minimum epoch {default_epoch}");
            default_epoch
        }
    };

    Ok(resume_epoch)
}

fn build_and_run_state_loading_dataflow<A: Allocate>(
    worker: &mut Worker<A>,
    interrupt_flag: &AtomicBool,
    resume_epoch: u64,
    state_reader: Box<dyn StateReader<u64>>,
) -> StringResult<(
    HashMap<StepId, HashMap<StateKey, StateBytes>>,
    RecoveryStoreSummary<u64>,
)> {
    let (step_to_key_to_resume_state_bytes_tx, step_to_key_to_resume_state_bytes_rx) =
        std::sync::mpsc::channel();
    let (recovery_store_summary_tx, recovery_store_summary_rx) = std::sync::mpsc::channel();

    let probe = build_state_loading_dataflow(
        worker,
        state_reader,
        resume_epoch,
        step_to_key_to_resume_state_bytes_tx,
        recovery_store_summary_tx,
    )?;

    run_until_done(worker, interrupt_flag, probe);

    let mut step_to_key_to_resume_state_bytes = HashMap::new();
    while let Ok((step_id, key_to_resume_state_bytes)) = step_to_key_to_resume_state_bytes_rx.recv()
    {
        step_to_key_to_resume_state_bytes.insert(step_id, key_to_resume_state_bytes);
    }

    let recovery_store_summary = recovery_store_summary_rx
        .recv()
        .expect("Recovery store summary not returned from loading dataflow");

    Ok((step_to_key_to_resume_state_bytes, recovery_store_summary))
}

fn build_and_run_production_dataflow<A: Allocate>(
    worker: &mut Worker<A>,
    interrupt_flag: &AtomicBool,
    flow: Py<Dataflow>,
    epoch_config: Py<EpochConfig>,
    resume_epoch: u64,
    step_to_key_to_resume_state_bytes: HashMap<StepId, HashMap<StateKey, StateBytes>>,
    recovery_store_summary: RecoveryStoreSummary<u64>,
    progress_writer: Box<dyn ProgressWriter<u64>>,
    state_writer: Box<dyn StateWriter<u64>>,
    state_collector: Box<dyn StateCollector<u64>>,
) -> StringResult<()> {
    let probe = Python::with_gil(|py| {
        build_production_dataflow(
            py,
            worker,
            epoch_config,
            resume_epoch,
            step_to_key_to_resume_state_bytes,
            recovery_store_summary,
            flow,
            progress_writer,
            state_writer,
            state_collector,
        )
    })?;

    run_until_done(worker, interrupt_flag, probe);

    Ok(())
}

/// Terminate all dataflows in this worker.
///
/// We need this because otherwise all of Timely's entry points
/// (e.g. [`timely::execute::execute_from`]) wait until all work is
/// complete and we will hang.
fn shutdown_worker<A: Allocate>(worker: &mut Worker<A>) {
    for dataflow_id in worker.installed_dataflows() {
        worker.drop_dataflow(dataflow_id);
    }
}

/// What a worker thread should do during its lifetime.
fn worker_main<A: Allocate>(
    worker: &mut Worker<A>,
    interrupt_flag: &AtomicBool,
    flow: Py<Dataflow>,
    epoch_config: Option<Py<EpochConfig>>,
    recovery_config: Option<Py<RecoveryConfig>>,
) -> StringResult<()> {
    let epoch_config = epoch_config.unwrap_or_else(default_epoch_config);
    let recovery_config = recovery_config.unwrap_or_else(default_recovery_config);

    let worker_index = worker.index();
    let worker_count = worker.peers();

    let (progress_reader, state_reader) = Python::with_gil(|py| {
        build_recovery_readers(py, worker_index, worker_count, recovery_config.clone())
    })?;
    let (progress_writer, state_writer, state_collector) = Python::with_gil(|py| {
        build_recovery_writers(py, worker_index, worker_count, recovery_config)
    })?;

    let resume_epoch =
        build_and_run_resume_epoch_calc_dataflow(worker, interrupt_flag, progress_reader)?;

    let (step_to_key_to_resume_state_bytes, recovery_store_summary) =
        build_and_run_state_loading_dataflow(worker, interrupt_flag, resume_epoch, state_reader)?;

    build_and_run_production_dataflow(
        worker,
        interrupt_flag,
        flow,
        epoch_config,
        resume_epoch,
        step_to_key_to_resume_state_bytes,
        recovery_store_summary,
        progress_writer,
        state_writer,
        state_collector,
    )?;

    shutdown_worker(worker);

    Ok(())
}

// TODO: pytest --doctest-modules does not find doctests in PyO3 code.
/// Execute a dataflow in the current thread.
///
/// Blocks until execution is complete.
///
/// You'd commonly use this for prototyping custom input and output
/// builders with a single worker before using them in a cluster
/// setting.
///
/// >>> from bytewax.dataflow import Dataflow
/// >>> from bytewax.inputs import TestingInputConfig
/// >>> from bytewax.outputs import StdOutputConfig
/// >>> flow = Dataflow()
/// >>> flow.input("inp", TestingInputConfig(range(3)))
/// >>> flow.capture(StdOutputConfig())
/// >>> run_main(flow)
/// 0
/// 1
/// 2
///
/// See `bytewax.spawn_cluster()` for starting a cluster on this
/// machine with full control over inputs and outputs.
///
/// Args:
///
///   flow: Dataflow to run.
///
///   epoch_config: A custom epoch config. You probably don't need
///       this. See `EpochConfig` for more info.
///
///   recovery_config: State recovery config. See
///       `bytewax.recovery`. If `None`, state will not be
///       persisted.
///
#[pyfunction(flow, "*", epoch_length = "None", recovery_config = "None")]
#[pyo3(text_signature = "(flow, *, epoch_config, recovery_config)")]
pub(crate) fn run_main(
    py: Python,
    flow: Py<Dataflow>,
    epoch_config: Option<Py<EpochConfig>>,
    recovery_config: Option<Py<RecoveryConfig>>,
) -> PyResult<()> {
    let result = py.allow_threads(move || {
        std::panic::catch_unwind(|| {
            // TODO: See if we can PR Timely to not cast result error
            // to a String. Then we could "raise" Python errors from
            // the builder directly. Probably also as part of the
            // panic recast issue below.
            timely::execute::execute_directly::<Result<(), String>, _>(move |worker| {
                let interrupt_flag = AtomicBool::new(false);

                worker_main(worker, &interrupt_flag, flow, epoch_config, recovery_config)
            })
        })
    });

    match result {
        Ok(Ok(ok)) => Ok(ok),
        Ok(Err(build_err_str)) => Err(PyValueError::new_err(build_err_str)),
        Err(panic_err) => {
            let pyerr = if let Some(pyerr) = panic_err.downcast_ref::<PyErr>() {
                pyerr.clone_ref(py)
            } else {
                PyRuntimeError::new_err("Panic in Rust code")
            };
            Err(pyerr)
        }
    }
}

/// Execute a dataflow in the current process as part of a cluster.
///
/// You have to coordinate starting up all the processes in the
/// cluster and ensuring they each are assigned a unique ID and know
/// the addresses of other processes. You'd commonly use this for
/// starting processes as part of a Kubernetes cluster.
///
/// Blocks until execution is complete.
///
/// >>> from bytewax.dataflow import Dataflow
/// >>> from bytewax.inputs import TestingInputConfig
/// >>> from bytewax.outputs import StdOutputConfig
/// >>> flow = Dataflow()
/// >>> flow.input("inp", TestingInputConfig(range(3)))
/// >>> flow.capture(StdOutputConfig())
/// >>> addresses = []  # In a real example, you'd find the "host:port" of all other Bytewax workers.
/// >>> proc_id = 0  # In a real example, you'd assign each worker a distinct ID from 0..proc_count.
/// >>> cluster_main(flow, addresses, proc_id)
/// 0
/// 1
/// 2
///
/// See `bytewax.run_main()` for a way to test input and output
/// builders without the complexity of starting a cluster.
///
/// See `bytewax.spawn_cluster()` for starting a simple cluster
/// locally on one machine.
///
/// Args:
///
///   flow: Dataflow to run.
///
///   addresses: List of host/port addresses for all processes in
///       this cluster (including this one).
///
///   proc_id: Index of this process in cluster; starts from 0.
///
///   epoch_config: A custom epoch config. You probably don't need
///       this. See `EpochConfig` for more info.
///
///   recovery_config: State recovery config. See
///       `bytewax.recovery`. If `None`, state will not be
///       persisted.
///
///   worker_count_per_proc: Number of worker threads to start on
///       each process.
#[pyfunction(
    flow,
    addresses,
    proc_id,
    "*",
    epoch_config = "None",
    recovery_config = "None",
    worker_count_per_proc = "1"
)]
#[pyo3(
    text_signature = "(flow, addresses, proc_id, *, epoch_config, recovery_config, worker_count_per_proc)"
)]
pub(crate) fn cluster_main(
    py: Python,
    flow: Py<Dataflow>,
    addresses: Option<Vec<String>>,
    proc_id: usize,
    epoch_config: Option<Py<EpochConfig>>,
    recovery_config: Option<Py<RecoveryConfig>>,
    worker_count_per_proc: usize,
) -> PyResult<()> {
    py.allow_threads(move || {
        let addresses = addresses.unwrap_or_default();
        let (builders, other) = if addresses.is_empty() {
            timely::CommunicationConfig::Process(worker_count_per_proc)
        } else {
            timely::CommunicationConfig::Cluster {
                threads: worker_count_per_proc,
                process: proc_id,
                addresses,
                report: false,
                log_fn: Box::new(|_| None),
            }
        }
        .try_build()
        .map_err(PyRuntimeError::new_err)?;

        let should_shutdown = Arc::new(AtomicBool::new(false));
        let should_shutdown_w = should_shutdown.clone();
        let should_shutdown_p = should_shutdown.clone();

        // Panic hook is per-process, so this won't work if you call
        // `cluster_main()` twice concurrently.
        let default_hook = std::panic::take_hook();
        std::panic::set_hook(Box::new(move |info| {
            should_shutdown_p.store(true, Ordering::Relaxed);

            if let Some(pyerr) = info.payload().downcast_ref::<PyErr>() {
                Python::with_gil(|py| pyerr.print(py));
            } else {
                default_hook(info);
            }
        }));
        // Don't chain panic hooks if we run multiple
        // dataflows. Really this is all a hack because the panic
        // hook is global state. There's some talk of per-thread
        // panic hooks which would help
        // here. https://internals.rust-lang.org/t/pre-rfc-should-std-set-hook-have-a-per-thread-version/9518/3
        defer! {
            let _ = std::panic::take_hook();
        }

        let guards = timely::execute::execute_from::<_, StringResult<()>, _>(
            builders,
            other,
            timely::WorkerConfig::default(),
            move |worker| {
                worker_main(
                    worker,
                    &should_shutdown_w,
                    flow.clone(),
                    epoch_config.clone(),
                    recovery_config.clone(),
                )
            },
        )
        .map_err(PyRuntimeError::new_err)?;

        // Recreating what Python does in Thread.join() to "block"
        // but also check interrupt handlers.
        // https://github.com/python/cpython/blob/204946986feee7bc80b233350377d24d20fcb1b8/Modules/_threadmodule.c#L81
        while guards
            .guards()
            .iter()
            .any(|worker_thread| !worker_thread.is_finished())
        {
            thread::sleep(Duration::from_millis(1));
            Python::with_gil(|py| Python::check_signals(py)).map_err(|err| {
                should_shutdown.store(true, Ordering::Relaxed);
                err
            })?;
        }
        for maybe_worker_panic in guards.join() {
            // TODO: See if we can PR Timely to not cast panic info to
            // String. Then we could re-raise Python exception in main
            // thread and not need to print in panic::set_hook above,
            // although we still need it to tell the other workers to
            // do graceful shutdown.
            match maybe_worker_panic {
                Ok(Ok(ok)) => Ok(ok),
                Ok(Err(build_err_str)) => Err(PyValueError::new_err(build_err_str)),
                Err(_panic_err) => Err(PyRuntimeError::new_err(
                    "Worker thread died; look for errors above",
                )),
            }?;
        }

        Ok(())
    })
}

pub(crate) fn register(_py: Python, m: &PyModule) -> PyResult<()> {
    m.add_class::<EpochConfig>()?;
    m.add_class::<TestingEpochConfig>()?;
    m.add_class::<PeriodicEpochConfig>()?;
    m.add_function(wrap_pyfunction!(run_main, m)?)?;
    m.add_function(wrap_pyfunction!(cluster_main, m)?)?;
    Ok(())
}
