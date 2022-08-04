//! Internal code for dataflow execution.
//!
//! For a user-centric version of how to execute dataflows, read the
//! the `bytewax.execution` Python module docstring. Read that first.
//!
//! [`worker_main()`] for the root of all the internal action here.

use crate::dataflow::{Dataflow, Step};
use crate::inputs::build_input_reader;
use crate::inputs::InputReader;
use crate::operators::CollectGarbage;
use crate::operators::*;
use crate::outputs::build_output_writer;
use crate::outputs::capture;
use crate::pyo3_extensions::{extract_state_pair, wrap_state_pair, StateKey, TdPyAny};
use crate::recovery::RecoveryConfig;
use crate::recovery::StateReader;
use crate::recovery::StateWriter;
use crate::recovery::{build_recovery_readers, build_recovery_writers, FrontierUpdate};
use crate::recovery::{default_recovery_config, ProgressWriter};
use crate::recovery::{ProgressReader, StateCollector};
use crate::window::{build_clock, build_windower, reduce_window, StatefulWindowUnary};
use log::debug;
use pyo3::exceptions::{PyRuntimeError, PyValueError};
use pyo3::prelude::*;
use std::fmt::Debug;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::task::Poll;
use std::thread;
use std::time::{Duration, Instant};
use timely::communication::Allocate;
use timely::dataflow::operators::generic::source;
use timely::dataflow::operators::result::*;
use timely::dataflow::{operators::*, Stream};
use timely::dataflow::{ProbeHandle, Scope};
use timely::progress::Antichain;
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
/// Returns:
///
///     Config object. Pass this as the `epoch_config` parameter of
///     your execution entry point.
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
///     epoch_length (datetime.timedelta): System time length of each
///         epoch.
///
/// Returns:
///
///     Config object. Pass this as the `epoch_config` parameter of
///     your execution entry point.
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
        ("PeriodicEpochConfig", self.epoch_length.clone())
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
pub(crate) fn testing_input_source<S, D: Data + Debug>(
    scope: &S,
    mut reader: Box<dyn InputReader<D>>,
    start_at: S::Timestamp,
    probe: &ProbeHandle<S::Timestamp>,
) -> Stream<S, D>
where
    S: Scope<Timestamp = u64>,
{
    source(scope, "TestingInputSource", move |mut init_cap, info| {
        let probe = probe.clone();
        let activator = scope.activator_for(&info.address[..]);

        init_cap.downgrade(&start_at);
        let mut cap = Some(init_cap);

        move |output| {
            let mut eof = false;

            if let Some(cap) = cap.as_mut() {
                let epoch = cap.time();

                if !probe.less_than(epoch) {
                    match reader.next() {
                        Poll::Pending => {}
                        Poll::Ready(None) => {
                            eof = true;
                        }
                        Poll::Ready(Some(item)) => {
                            output.session(&cap).give(item);

                            let next_epoch = epoch + 1;
                            cap.downgrade(&next_epoch);
                        }
                    }
                }
            }

            if eof {
                cap = None;
            } else {
                activator.activate();
            }
        }
    })
}

/// Input source that increments the epoch periodically by system
/// time.
pub(crate) fn periodic_input_source<S, D: Data + Debug>(
    scope: &S,
    mut reader: Box<dyn InputReader<D>>,
    start_at: S::Timestamp,
    probe: &ProbeHandle<S::Timestamp>,
    epoch_length: Duration,
) -> Stream<S, D>
where
    S: Scope<Timestamp = u64>,
{
    source(scope, "PeriodicInputSource", move |mut init_cap, info| {
        let probe = probe.clone();
        let activator = scope.activator_for(&info.address[..]);

        init_cap.downgrade(&start_at);
        let mut cap = Some(init_cap);
        let mut epoch_started = Instant::now();

        move |output| {
            let mut eof = false;

            if let Some(cap) = cap.as_mut() {
                let epoch = cap.time();

                if !probe.less_than(epoch) {
                    if epoch_started.elapsed() > epoch_length {
                        let next_epoch = epoch + 1;
                        cap.downgrade(&next_epoch);
                        epoch_started = Instant::now();
                    }

                    match reader.next() {
                        Poll::Pending => {}
                        Poll::Ready(None) => {
                            eof = true;
                        }
                        Poll::Ready(Some(item)) => {
                            output.session(&cap).give(item);
                        }
                    }
                }
            }

            if eof {
                cap = None;
            } else {
                activator.activate();
            }
        }
    })
}

fn build_input_source<S>(
    py: Python,
    epoch_config: Py<EpochConfig>,
    scope: &S,
    reader: Box<dyn InputReader<TdPyAny>>,
    start_at: S::Timestamp,
    probe: &ProbeHandle<S::Timestamp>,
) -> Result<Stream<S, TdPyAny>, String>
where
    S: Scope<Timestamp = u64>,
{
    let epoch_config = epoch_config.as_ref(py);

    if let Ok(testing_config) = epoch_config.downcast::<PyCell<TestingEpochConfig>>() {
        let _testing_config = testing_config.borrow();

        Ok(testing_input_source(scope, reader, start_at, probe))
    } else if let Ok(periodic_config) = epoch_config.downcast::<PyCell<PeriodicEpochConfig>>() {
        let periodic_config = periodic_config.borrow();

        let epoch_length = periodic_config.epoch_length.0.to_std().expect("");

        Ok(periodic_input_source(
            scope,
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

/// Compile a dataflow which reads the progress data from the previous
/// execution and calculates the resume epoch.
fn build_resume_epoch_calc_dataflow<A: Allocate>(
    timely_worker: &mut Worker<A>,
    // TODO: Allow multiple (or none) FrontierReaders so you can recover a
    // different-sized cluster.
    progress_reader: Box<dyn ProgressReader<u64>>,
    resume_epoch_tx: std::sync::mpsc::Sender<u64>,
) -> Result<ProbeHandle<()>, String> {
    timely_worker.dataflow(|scope| {
        let mut probe = ProbeHandle::new();

        progress_source(scope, progress_reader, &probe)
            .accumulate(
                // A frontier of [0] is the "earliest", not the empty
                // frontier. (Empty is "complete" or "last", which is
                // used below).
                Antichain::from_elem(Default::default()),
                |worker_frontier, frontier_updates| {
                    for FrontierUpdate(antichain) in frontier_updates.iter() {
                        // For each worker in the failed cluster, find the
                        // latest frontier.
                        if timely::PartialOrder::less_than(worker_frontier, &antichain) {
                            *worker_frontier = antichain.clone();
                        }
                    }
                },
            )
            // Each worker in the recovery cluster reads only some of
            // the frontier data of workers in the failed cluster.
            .broadcast()
            .accumulate(Antichain::new(), |dataflow_frontier, worker_frontiers| {
                for worker_frontier in worker_frontiers.iter() {
                    // The slowest of the workers in the failed
                    // cluster is the resume epoch.
                    if timely::PartialOrder::less_than(worker_frontier, dataflow_frontier) {
                        *dataflow_frontier = worker_frontier.clone();
                    }
                }
            })
            .map(|dataflow_frontier| {
                // TODO: Is this the right way to transform a frontier
                // back into a recovery epoch?
                dataflow_frontier
                    .elements()
                    .iter()
                    .cloned()
                    .min()
                    .unwrap_or_default()
            })
            .inspect(move |resume_epoch| resume_epoch_tx.send(*resume_epoch).unwrap())
            .probe_with(&mut probe);

        Ok(probe)
    })
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
    flow: Py<Dataflow>,
    state_reader: Box<dyn StateReader<u64, Vec<u8>>>,
    progress_writer: Box<dyn ProgressWriter<u64>>,
    state_writer: Box<dyn StateWriter<u64, Vec<u8>>>,
    state_collector: Box<dyn StateCollector<u64>>,
) -> Result<ProbeHandle<u64>, String> {
    let worker_index = worker.index();
    let worker_count = worker.peers();

    worker.dataflow(|scope| {
        let flow = flow.as_ref(py).borrow();

        let mut probe = ProbeHandle::new();

        let state_loading_stream = state_source(scope, state_reader, resume_epoch, &probe);
        let input_reader =
            build_input_reader(py, flow.input_config.clone(), worker_index, worker_count)?;
        let mut stream =
            build_input_source(py, epoch_config, scope, input_reader, resume_epoch, &probe)?;

        let mut state_backup_streams = Vec::new();
        let mut capture_streams = Vec::new();

        for step in &flow.steps {
            // All these closure lifetimes are static, so tell
            // Python's GC that there's another pointer to the
            // mapping function that's going to hang around
            // for a while when it's moved into the closure.
            let step = step.clone();
            match step {
                Step::Map { mapper } => {
                    stream = stream.map(move |item| map(&mapper, item));
                }
                Step::FlatMap { mapper } => {
                    stream = stream.flat_map(move |item| flat_map(&mapper, item));
                }
                Step::Filter { predicate } => {
                    stream = stream.filter(move |item| filter(&predicate, item));
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
                    stream = stream
                        .map(extract_state_pair)
                        .stateful_unary(
                            step_id,
                            move |key, acc, value| reduce(&reducer, &is_complete, key, acc, value),
                            StateKey::route,
                            &state_loading_stream,
                            &mut state_backup_streams,
                        )
                        .map(wrap_state_pair);
                }
                Step::ReduceWindow {
                    step_id,
                    clock_config,
                    window_config,
                    reducer,
                } => {
                    let windower = build_windower(py, window_config)?;
                    let clock = build_clock(py, clock_config)?;

                    stream = stream
                        .map(extract_state_pair)
                        .stateful_window_unary(
                            step_id,
                            clock,
                            windower,
                            move |key, state, next_value| {
                                reduce_window(&reducer, key, state, next_value)
                            },
                            StateKey::route,
                            &state_loading_stream,
                            &mut state_backup_streams,
                        )
                        .map(|(key, result)| {
                            result
                                .map(|value| (key.clone(), value))
                                .map_err(|err| (key.clone(), err))
                        })
                        // For now, filter to just reductions and
                        // ignore late values.
                        .ok()
                        .map(wrap_state_pair);
                }
                Step::StatefulMap {
                    step_id,
                    builder,
                    mapper,
                } => {
                    stream = stream
                        .map(extract_state_pair)
                        .stateful_unary(
                            step_id,
                            move |key, state, value| {
                                stateful_map(&builder, &mapper, key, state, value)
                            },
                            StateKey::route,
                            &state_loading_stream,
                            &mut state_backup_streams,
                        )
                        .map(wrap_state_pair);
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

        if capture_streams.is_empty() {
            return Err("Dataflow needs to contain at least one capture".into());
        }

        let state_backup_stream = scope
            .concatenate(state_backup_streams)
            .write_state_with(state_writer);

        let capture_stream = scope.concatenate(capture_streams);

        let dataflow_frontier_stream = capture_stream
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
            .concat(&state_loading_stream)
            .collect_garbage(state_collector, dataflow_frontier_stream)
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
    recovery_config: Py<RecoveryConfig>,
) -> Result<u64, String> {
    let worker_index = worker.index();
    let worker_count = worker.peers();

    let (progress_reader, _state_reader) = Python::with_gil(|py| {
        build_recovery_readers(py, worker_index, worker_count, recovery_config)
    })?;

    let (resume_epoch_tx, resume_epoch_rx) = std::sync::mpsc::channel();
    let probe = build_resume_epoch_calc_dataflow(worker, progress_reader, resume_epoch_tx)?;

    run_until_done(worker, &interrupt_flag, probe);

    let resume_epoch = match resume_epoch_rx.recv() {
        Ok(resume_epoch) => {
            debug!("Loaded resume epoch {resume_epoch}");
            resume_epoch
        }
        Err(_) => {
            let default_epoch = Default::default();
            debug!("No resume epoch calculated; probably empty recovery store; starting at default epoch {default_epoch}");
            default_epoch
        }
    };

    Ok(resume_epoch)
}

fn build_and_run_production_dataflow<A: Allocate>(
    worker: &mut Worker<A>,
    interrupt_flag: &AtomicBool,
    flow: Py<Dataflow>,
    epoch_config: Py<EpochConfig>,
    recovery_config: Py<RecoveryConfig>,
    resume_epoch: u64,
) -> Result<(), String> {
    let worker_index = worker.index();
    let worker_count = worker.peers();

    let (_progress_reader, state_reader) = Python::with_gil(|py| {
        build_recovery_readers(py, worker_index, worker_count, recovery_config.clone())
    })?;
    let (progress_writer, state_writer, state_collector) = Python::with_gil(|py| {
        build_recovery_writers(py, worker_index, worker_count, recovery_config)
    })?;

    let probe = Python::with_gil(|py| {
        build_production_dataflow(
            py,
            worker,
            epoch_config,
            resume_epoch,
            flow,
            state_reader,
            progress_writer,
            state_writer,
            state_collector,
        )
    })?;

    run_until_done(worker, &interrupt_flag, probe);

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
) -> Result<(), String> {
    let epoch_config = epoch_config.unwrap_or_else(default_epoch_config);
    let recovery_config = recovery_config.unwrap_or_else(default_recovery_config);

    let resume_epoch =
        build_and_run_resume_epoch_calc_dataflow(worker, interrupt_flag, recovery_config.clone())?;

    build_and_run_production_dataflow(
        worker,
        interrupt_flag,
        flow,
        epoch_config,
        recovery_config,
        resume_epoch,
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
/// >>> flow = Dataflow()
/// >>> flow.capture()
/// >>> def input_builder(worker_index, worker_count, resume_epoch):
/// ...     for epoch, item in enumerate(range(resume_epoch, 3)):
/// ...         yield AdvanceTo(epoch)
/// ...         yield Emit(item)
/// >>> def output_builder(worker_index, worker_count):
/// ...     return print
/// >>> run_main(flow, ManualConfig(input_builder), output_builder)  # doctest: +ELLIPSIS
/// (...)
///
/// See `bytewax.spawn_cluster()` for starting a cluster on this
/// machine with full control over inputs and outputs.
///
/// Args:
///
///     flow: Dataflow to run.
///
///     epoch_config: A custom epoch config. You probably don't need
///         this. See `EpochConfig` for more info.
///
///     recovery_config: State recovery config. See
///         `bytewax.recovery`. If `None`, state will not be
///         persisted.
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
/// >>> flow = Dataflow()
/// >>> def input_builder(worker_index, worker_count, resume_epoch):
/// ...     for epoch, item in enumerate(range(resume_epoch, 3)):
/// ...         yield AdvanceTo(epoch)
/// ...         yield Emit(item)
/// >>> def output_builder(worker_index, worker_count):
/// ...     return print
/// >>> cluster_main(flow, ManualInput(input_builder), output_builder)  # doctest: +ELLIPSIS
/// (...)
///
/// See `bytewax.run_main()` for a way to test input and output
/// builders without the complexity of starting a cluster.
///
/// See `bytewax.spawn_cluster()` for starting a simple cluster
/// locally on one machine.
///
/// Args:
///
///     flow: Dataflow to run.
///
///     addresses: List of host/port addresses for all processes in
///         this cluster (including this one).
///
///     proc_id: Index of this process in cluster; starts from 0.
///
///     epoch_config: A custom epoch config. You probably don't need
///         this. See `EpochConfig` for more info.
///
///     recovery_config: State recovery config. See
///         `bytewax.recovery`. If `None`, state will not be
///         persisted.
///
///     worker_count_per_proc: Number of worker threads to start on
///         each process.
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
        let (builders, other) = if addresses.len() < 1 {
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

        let guards = timely::execute::execute_from::<_, Result<(), String>, _>(
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
