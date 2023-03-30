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

mod runner;

use runner::Runner;
use timely::dataflow::operators::{Concatenate, Filter, Inspect, Map, ResultStream, ToStream};

use crate::dataflow::{Dataflow, Step};
use crate::errors::{tracked_err, PythonException};
use crate::inputs::{DynamicInput, EpochInterval, PartitionedInput};
use crate::operators::collect_window::CollectWindowLogic;
use crate::operators::fold_window::FoldWindowLogic;
use crate::operators::reduce::ReduceLogic;
use crate::operators::reduce_window::ReduceWindowLogic;
use crate::operators::stateful_map::StatefulMapLogic;
use crate::operators::stateful_unary::StatefulUnary;
use crate::operators::{filter, flat_map, inspect, inspect_epoch, map};
use crate::outputs::{DynamicOutputOp, PartitionedOutputOp};
use crate::pyo3_extensions::{extract_state_pair, wrap_state_pair};
use crate::recovery::dataflows::attach_recovery_to_dataflow;
use crate::recovery::model::{
    Change, KChange, KWriter, ProgressMsg, ProgressWriter, ResumeFrom, StateWriter, WorkerKey,
};
use crate::recovery::python::{
    build_recovery_readers, build_recovery_writers, default_recovery_config,
};
use crate::recovery::{
    dataflows::{build_progress_loading_dataflow, build_state_loading_dataflow},
    model::{FlowStateBytes, ProgressReader, ResumeEpoch, StateReader},
    python::RecoveryConfig,
    store::in_mem::{InMemProgress, StoreSummary},
};
use crate::unwrap_any;
use crate::webserver::run_webserver;
use crate::window::clock::ClockBuilder;
use crate::window::{StatefulWindowUnary, WindowBuilder};
use pyo3::exceptions::{PyRuntimeError, PyTypeError, PyValueError};
use pyo3::prelude::*;
use pyo3::types::{PyString, PyTuple};
use serde::{Deserialize, Serialize};
use std::fmt::Debug;
use std::path::PathBuf;
use std::rc::Rc;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;
use std::time::{Duration, Instant};
use timely::communication::Allocate;
use timely::dataflow::ProbeHandle;
use timely::progress::Timestamp;
use timely::worker::Worker;
use tracing::span::EnteredSpan;

/// Integer representing the index of a worker in a cluster.
#[derive(Debug, Copy, Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub(crate) struct WorkerIndex(pub(crate) usize);

impl IntoPy<Py<PyAny>> for WorkerIndex {
    fn into_py(self, py: Python) -> Py<PyAny> {
        self.0.into_py(py)
    }
}

/// Integer representing the number of workers in a cluster.
#[derive(Debug, Copy, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct WorkerCount(pub(crate) usize);

impl WorkerCount {
    /// Iterate through all workers in this cluster.
    pub(crate) fn iter(&self) -> impl Iterator<Item = WorkerIndex> {
        (0..self.0).map(WorkerIndex)
    }
}

impl IntoPy<Py<PyAny>> for WorkerCount {
    fn into_py(self, py: Python) -> Py<PyAny> {
        self.0.into_py(py)
    }
}

#[test]
fn worker_count_iter_works() {
    let count = WorkerCount(3);
    let found: Vec<_> = count.iter().collect();
    let expected = vec![WorkerIndex(0), WorkerIndex(1), WorkerIndex(2)];
    assert_eq!(found, expected);
}

/// Turn the abstract blueprint for a dataflow into a Timely dataflow
/// so it can be executed.
///
/// This is more complicated than a 1:1 translation of Bytewax
/// concepts to Timely, as we are using Timely as a basis to implement
/// more-complicated Bytewax features like input builders and
/// recovery.
#[allow(clippy::too_many_arguments)]
fn build_production_dataflow<A, PW, SW>(
    py: Python,
    worker: &mut Worker<A>,
    flow: Py<Dataflow>,
    epoch_interval: EpochInterval,
    resume_from: ResumeFrom,
    mut resume_state: FlowStateBytes,
    mut resume_progress: InMemProgress,
    store_summary: StoreSummary,
    mut progress_writer: PW,
    state_writer: SW,
) -> PyResult<ProbeHandle<u64>>
where
    A: Allocate,
    PW: ProgressWriter + 'static,
    SW: StateWriter + 'static,
{
    let ResumeFrom(ex, resume_epoch) = resume_from;

    let worker_index = WorkerIndex(worker.index());
    let worker_count = WorkerCount(worker.peers());

    let worker_key = WorkerKey(ex, worker_index);

    let progress_init = KChange(
        worker_key,
        Change::Upsert(ProgressMsg::Init(worker_count, resume_epoch)),
    );
    resume_progress.write(progress_init.clone());
    progress_writer.write(progress_init);

    // Remember! Never build different numbers of Timely operators on
    // different workers! Timely does not like that and you'll see a
    // mysterious `failed to correctly cast channel` panic. You must
    // build asymmetry within each operator.
    worker.dataflow(|scope| {
        let flow = flow.as_ref(py).borrow();
        let mut probe = ProbeHandle::new();

        let mut inputs = Vec::new();
        let mut outputs = Vec::new();
        let mut step_changes = Vec::new();

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
                Step::CollectWindow {
                    step_id,
                    clock_config,
                    window_config,
                } => {
                    let step_resume_state = resume_state.remove(&step_id);

                    let clock_builder = clock_config
                        .build(py)
                        .reraise("error building CollectWindow clock")?;
                    let windower_builder = window_config
                        .build(py)
                        .reraise("error building CollectWindow windower")?;

                    let (output, changes) = stream.map(extract_state_pair).stateful_window_unary(
                        step_id,
                        clock_builder,
                        windower_builder,
                        CollectWindowLogic::builder(),
                        resume_epoch,
                        step_resume_state,
                    );

                    stream = output
                        .map(|(key, result)| {
                            result
                                .map(|value| (key.clone(), value))
                                .map_err(|err| (key.clone(), err))
                        })
                        // For now, filter to just reductions and
                        // ignore late values.
                        .ok()
                        .map(wrap_state_pair);
                    step_changes.push(changes);
                }
                Step::Input { step_id, input } => {
                    if let Ok(input) = input.extract::<PartitionedInput>(py) {
                        let step_resume_state = resume_state.remove(&step_id);

                        let (output, changes) = input
                            .partitioned_input(
                                py,
                                scope,
                                step_id.clone(),
                                epoch_interval.clone(),
                                worker_index,
                                worker_count,
                                &probe,
                                resume_epoch,
                                step_resume_state,
                            )
                            .reraise("error building PartitionedInput")?;

                        inputs.push(output.clone());
                        stream = output;
                        step_changes.push(changes);
                    } else if let Ok(input) = input.extract::<DynamicInput>(py) {
                        let output = input
                            .dynamic_input(
                                py,
                                scope,
                                step_id.clone(),
                                epoch_interval.clone(),
                                worker_index,
                                worker_count,
                                &probe,
                                resume_epoch,
                            )
                            .reraise("error building DynamicInput")?;

                        inputs.push(output.clone());
                        stream = output;
                    } else {
                        return Err(tracked_err::<PyTypeError>("unknown input type"));
                    }
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
                Step::FilterMap { mapper } => {
                    stream = stream
                        .map(move |item| map(&mapper, item))
                        .filter(move |item| Python::with_gil(|py| !item.is_none(py)));
                }
                Step::FoldWindow {
                    step_id,
                    clock_config,
                    window_config,
                    builder,
                    folder,
                } => {
                    let step_resume_state = resume_state.remove(&step_id);

                    let clock_builder = clock_config
                        .build(py)
                        .reraise("error building FoldWindow clock")?;
                    let windower_builder = window_config
                        .build(py)
                        .reraise("error building FoldWindow windower")?;

                    let (output, changes) = stream.map(extract_state_pair).stateful_window_unary(
                        step_id,
                        clock_builder,
                        windower_builder,
                        FoldWindowLogic::new(builder, folder),
                        resume_epoch,
                        step_resume_state,
                    );

                    stream = output
                        .map(|(key, result)| {
                            result
                                .map(|value| (key.clone(), value))
                                .map_err(|err| (key.clone(), err))
                        })
                        // For now, filter to just reductions and
                        // ignore late values.
                        .ok()
                        .map(wrap_state_pair);
                    step_changes.push(changes);
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
                    let step_resume_state = resume_state.remove(&step_id);

                    let (output, changes) = stream.map(extract_state_pair).stateful_unary(
                        step_id,
                        ReduceLogic::builder(reducer, is_complete),
                        resume_epoch,
                        step_resume_state,
                    );
                    stream = output.map(wrap_state_pair);
                    step_changes.push(changes);
                }
                Step::ReduceWindow {
                    step_id,
                    clock_config,
                    window_config,
                    reducer,
                } => {
                    let step_resume_state = resume_state.remove(&step_id);

                    let clock_builder = clock_config
                        .build(py)
                        .reraise("error building ReduceWindow clock")?;
                    let windower_builder = window_config
                        .build(py)
                        .reraise("error building ReduceWindow windower")?;

                    let (output, changes) = stream.map(extract_state_pair).stateful_window_unary(
                        step_id,
                        clock_builder,
                        windower_builder,
                        ReduceWindowLogic::builder(reducer),
                        resume_epoch,
                        step_resume_state,
                    );

                    stream = output
                        .map(|(key, result)| {
                            result
                                .map(|value| (key.clone(), value))
                                .map_err(|err| (key.clone(), err))
                        })
                        // For now, filter to just reductions and
                        // ignore late values.
                        .ok()
                        .map(wrap_state_pair);
                    step_changes.push(changes);
                }
                Step::StatefulMap {
                    step_id,
                    builder,
                    mapper,
                } => {
                    let step_resume_state = resume_state.remove(&step_id);

                    let (output, changes) = stream.map(extract_state_pair).stateful_unary(
                        step_id,
                        StatefulMapLogic::builder(builder, mapper),
                        resume_epoch,
                        step_resume_state,
                    );
                    stream = output.map(wrap_state_pair);
                    step_changes.push(changes);
                }
                Step::Output { step_id, output } => {
                    if let Ok(output) = output.extract(py) {
                        let step_resume_state = resume_state.remove(&step_id);

                        let (output, changes) = stream
                            .partitioned_output(
                                py,
                                step_id,
                                output,
                                worker_index,
                                worker_count,
                                step_resume_state,
                            )
                            .reraise("error building PartitionedOutput")?;
                        let clock = output.map(|_| ());

                        outputs.push(clock.clone());
                        step_changes.push(changes);
                        stream = output;
                    } else if let Ok(output) = output.extract(py) {
                        let output = stream
                            .dynamic_output(py, step_id, output, worker_index, worker_count)
                            .reraise("error building DynamicOutput")?;
                        let clock = output.map(|_| ());

                        outputs.push(clock.clone());
                        stream = output;
                    } else {
                        return Err(tracked_err::<PyTypeError>("unknown output type"));
                    }
                }
            }
        }

        if inputs.is_empty() {
            return Err(tracked_err::<PyValueError>(
                "Dataflow needs to contain at least one input",
            ));
        }
        if outputs.is_empty() {
            return Err(tracked_err::<PyValueError>(
                "Dataflow needs to contain at least one output",
            ));
        }
        if !resume_state.is_empty() {
            tracing::warn!(
                "Resume state exists for unknown steps {:?}; \
                    did you delete or rename a step and forget \
                    to remove or migrate state data?",
                resume_state.keys(),
            );
        }

        attach_recovery_to_dataflow(
            &mut probe,
            worker_key,
            resume_epoch,
            resume_progress,
            store_summary,
            progress_writer,
            state_writer,
            scope.concatenate(step_changes),
            scope.concatenate(outputs),
        );

        Ok(probe)
    })
}

// Struct used to handle a span that is closed and reopened periodically.
struct PeriodicSpan {
    span: Option<EnteredSpan>,
    length: Duration,
    // State
    last_open: Instant,
    counter: u64,
}

impl PeriodicSpan {
    pub fn new(length: Duration) -> Self {
        Self {
            span: Some(tracing::trace_span!("Periodic", counter = 0).entered()),
            length,
            last_open: Instant::now(),
            counter: 0,
        }
    }

    pub fn update(&mut self) {
        if self.last_open.elapsed() > self.length {
            if let Some(span) = self.span.take() {
                span.exit();
            }
            self.counter += 1;
            self.span = Some(tracing::trace_span!("Periodic", counter = self.counter).entered());
            self.last_open = Instant::now();
        }
    }
}

/// Run a dataflow which uses sources until complete.
fn run_until_done<A: Allocate, T: Timestamp>(
    worker: &mut Worker<A>,
    interrupt_flag: &AtomicBool,
    probe: ProbeHandle<T>,
) -> PyResult<()> {
    let mut span = PeriodicSpan::new(Duration::from_secs(10));
    while !interrupt_flag.load(Ordering::Relaxed) && !probe.done() {
        worker.step();
        span.update();
        let check = Python::with_gil(|py| py.check_signals());
        if check.is_err() {
            interrupt_flag.store(true, Ordering::Relaxed);
            // The ? here will always exit since we just checked
            // that `check` is Result::Err.
            check.reraise("interrupt signal received")?;
        }
    }
    Ok(())
}

fn build_and_run_progress_loading_dataflow<A, R>(
    worker: &mut Worker<A>,
    interrupt_flag: &AtomicBool,
    progress_reader: R,
) -> PyResult<InMemProgress>
where
    A: Allocate,
    R: ProgressReader + 'static,
{
    let (probe, progress_store) = build_progress_loading_dataflow(worker, progress_reader);

    run_until_done(worker, interrupt_flag, probe)?;

    let resume_progress = Rc::try_unwrap(progress_store)
        .expect("Resume epoch dataflow still has reference to progress_store")
        .into_inner();

    Ok(resume_progress)
}

fn build_and_run_state_loading_dataflow<A, R>(
    worker: &mut Worker<A>,
    interrupt_flag: &AtomicBool,
    resume_epoch: ResumeEpoch,
    state_reader: R,
) -> PyResult<(FlowStateBytes, StoreSummary)>
where
    A: Allocate,
    R: StateReader + 'static,
{
    let (probe, resume_state, summary) =
        build_state_loading_dataflow(worker, state_reader, resume_epoch);

    run_until_done(worker, interrupt_flag, probe)?;

    Ok((
        Rc::try_unwrap(resume_state)
            .expect("State loading dataflow still has reference to resume_state")
            .into_inner(),
        Rc::try_unwrap(summary)
            .expect("State loading dataflow still has reference to summary")
            .into_inner(),
    ))
}

#[allow(clippy::too_many_arguments)]
fn build_and_run_production_dataflow<A, PW, SW>(
    worker: &mut Worker<A>,
    interrupt_flag: &AtomicBool,
    flow: Py<Dataflow>,
    epoch_interval: EpochInterval,
    resume_from: ResumeFrom,
    resume_state: FlowStateBytes,
    resume_progress: InMemProgress,
    store_summary: StoreSummary,
    progress_writer: PW,
    state_writer: SW,
) -> PyResult<()>
where
    A: Allocate,
    PW: ProgressWriter + 'static,
    SW: StateWriter + 'static,
{
    let span = tracing::trace_span!("Building dataflow").entered();

    // Avoid initializing the tokio runtime for the webserver if we don't need it.
    let rt = if worker.index() == 0 && std::env::var("BYTEWAX_DATAFLOW_API_ENABLED").is_ok() {
        Some(unwrap_any!(tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .thread_name("webserver-threads")
            .enable_all()
            .build()
            .raise::<PyRuntimeError>(
                "error initializing tokio runtime for webserver"
            )))
    } else {
        None
    };

    let probe = Python::with_gil(|py| {
        if let Some(rt) = rt {
            let df = flow.extract(py).unwrap();
            rt.spawn(run_webserver(df));
        }

        build_production_dataflow(
            py,
            worker,
            flow,
            epoch_interval,
            resume_from,
            resume_state,
            resume_progress,
            store_summary,
            progress_writer,
            state_writer,
        )
        .reraise("error building Dataflow")
    })?;
    span.exit();

    run_until_done(worker, interrupt_flag, probe).reraise("error running Dataflow")?;

    Ok(())
}

fn worker_main<A: Allocate>(
    worker: &mut Worker<A>,
    interrupt_flag: &AtomicBool,
    flow: Py<Dataflow>,
    epoch_interval: Option<EpochInterval>,
    recovery_config: Option<Py<RecoveryConfig>>,
) -> PyResult<()> {
    let worker_index = worker.index();
    let worker_count = worker.peers();

    tracing::info!("Worker {worker_index:?} of {worker_count:?} starting up");

    let epoch_interval = epoch_interval.clone().unwrap_or_default();
    tracing::info!("Using epoch interval of {epoch_interval:?}");

    let recovery_config = recovery_config
        .clone()
        .unwrap_or_else(default_recovery_config);

    let (progress_reader, state_reader) = Python::with_gil(|py| {
        build_recovery_readers(py, worker_index, worker_count, recovery_config.clone())
            .reraise("error building recovery readers")
    })?;
    let (progress_writer, state_writer) = Python::with_gil(|py| {
        build_recovery_writers(py, worker_index, worker_count, recovery_config)
            .reraise("error building recovery writers")
    })?;

    let span = tracing::trace_span!("Resume epoch").entered();
    let resume_progress =
        build_and_run_progress_loading_dataflow(worker, interrupt_flag, progress_reader)
            .reraise("error while loading recovery progress")?;
    span.exit();
    let resume_from = resume_progress.resume_from();
    tracing::info!("Calculated {resume_from:?}");

    let span = tracing::trace_span!("State loading").entered();
    let ResumeFrom(_ex, resume_epoch) = resume_from;
    let (resume_state, store_summary) =
        build_and_run_state_loading_dataflow(worker, interrupt_flag, resume_epoch, state_reader)
            .reraise("error loading recovery state")?;
    span.exit();

    build_and_run_production_dataflow(
        worker,
        interrupt_flag,
        flow,
        epoch_interval,
        resume_from,
        resume_state,
        resume_progress,
        store_summary,
        progress_writer,
        state_writer,
    )?;

    shutdown_worker(worker);

    tracing::info!("Worker {worker_index:?} of {worker_count:?} shut down");

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

/// Get a Dataflow from a file_path.
/// dataflow_builder is the accessor we use to get the dataflow object.
/// It can represent either a variable or a callable.
/// If it's a callable we call it with dataflow_args as arguments.
/// The resulting object should be a Dataflow, or this will return a PyErr.
fn get_flow(
    py: Python,
    file_path: &PathBuf,
    dataflow_builder: &String,
    dataflow_args: Vec<String>,
) -> PyResult<Dataflow> {
    let util = py.import("importlib.util")?;
    let spec_from_file_location = util.getattr("spec_from_file_location")?;
    let module_from_spec = util.getattr("module_from_spec")?;

    let module_name = file_path
        .file_stem()
        // file_stem returns None if the path is a directory, we want a python file.
        .ok_or_else(|| tracked_err::<PyRuntimeError>("the path passed has to be a python file"))?
        .to_string_lossy();
    let spec = spec_from_file_location.call((module_name, file_path), None)?;
    let module = module_from_spec.call1((spec,))?;
    spec.getattr("loader")?
        .getattr("exec_module")
        .reraise("error importing dataflow file")?
        .call1((module,))?;
    let dataflow_builder = PyString::new(py, &dataflow_builder);
    let builder = module.getattr(dataflow_builder)?;
    if builder.is_callable() {
        let tuple: &PyTuple = PyTuple::new(py, dataflow_args);
        builder.call(tuple, None)?.extract::<Dataflow>()
    } else {
        Err(tracked_err::<PyRuntimeError>(
            "the dataflow getter function is not a function",
        ))
    }
}

/// Check the __spec__ variable (https://docs.python.org/3/reference/import.html#spec__)
/// If __spec__.name == "bytewax.run" it means the module was called from there.
fn is_in_bytewax_run(py: Python) -> PyResult<bool> {
    // This call should never fail, since it should return
    // None if __spec__ doesn't exists, so we can unwrap.
    let spec = py.eval("__spec__", None, None).unwrap();

    // if `__spec__` is None, this is not during an import.
    // if `__spec__.name` is "bytewax.run", this was called from there.
    Ok(!spec.is_none()
        && spec
            .getattr("name")
            // If we can't get __spec__.name, it means this function is being
            // imported in a custom way.
            .raise::<PyRuntimeError>("error getting `__spec__.name`")?
            .to_string()
            == "bytewax.run")
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
///   epoch_interval (datetime.timedelta): System time length of each
///       epoch. Defaults to 10 seconds.
///
///   recovery_config: State recovery config. See
///       `bytewax.recovery`. If `None`, state will not be
///       persisted.
///
#[pyfunction(flow, "*", epoch_interval = "None", recovery_config = "None")]
#[pyo3(text_signature = "(flow, *, epoch_interval, recovery_config)")]
pub(crate) fn run_main(
    py: Python,
    flow: Py<Dataflow>,
    epoch_interval: Option<EpochInterval>,
    recovery_config: Option<Py<RecoveryConfig>>,
) -> PyResult<()> {
    if is_in_bytewax_run(py)? {
        return Err(tracked_err::<PyRuntimeError>(
            "run_main call detected while importing dataflow in bytewax.run. \
            Remove or comment the line.",
        ));
    }
    Runner::new(flow, 1, 1, epoch_interval, recovery_config).simple(py)
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
///   epoch_interval (datetime.timedelta): System time length of each
///       epoch. Defaults to 10 seconds.
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
    epoch_interval = "None",
    recovery_config = "None",
    worker_count_per_proc = "1"
)]
#[pyo3(
    text_signature = "(flow, addresses, proc_id, *, epoch_interval, recovery_config, worker_count_per_proc)"
)]
pub(crate) fn cluster_main(
    py: Python,
    flow: Py<Dataflow>,
    addresses: Option<Vec<String>>,
    proc_id: usize,
    epoch_interval: Option<EpochInterval>,
    recovery_config: Option<Py<RecoveryConfig>>,
    worker_count_per_proc: usize,
) -> PyResult<()> {
    if is_in_bytewax_run(py)? {
        return Err(tracked_err::<PyRuntimeError>(
            "cluster_main call detected while importing dataflow in \
            bytewax.run. Remove or comment the line",
        ));
    }
    Runner::new(
        flow,
        1,
        worker_count_per_proc,
        epoch_interval,
        recovery_config,
    )
    .cluster_single(py, addresses, proc_id)
}

/// Spawns a cluster on a single machine.
/// This is only supposed to be used through `python -m bytewax.run`,
/// and not directly called inside python code.
///
/// See `python -m bytewax.run --help` for more info
#[pyfunction(
    file_path,
    "*",
    processes = 1,
    workers_per_process = 1,
    dataflow_builder = "String::from(\"get_flow\")",
    dataflow_args = "None",
    epoch_interval = "None",
    recovery_config = "None"
)]
pub(crate) fn spawn_cluster(
    py: Python,
    file_path: PathBuf,
    dataflow_builder: String,
    dataflow_args: Option<Vec<String>>,
    processes: Option<usize>,
    workers_per_process: Option<usize>,
    epoch_interval: Option<f64>,
    recovery_config: Option<Py<RecoveryConfig>>,
) -> PyResult<()> {
    if !is_in_bytewax_run(py)? {
        return Err(tracked_err::<PyRuntimeError>(&format!(
            "You shouldn't use spawn_cluster directly, \
            see `python -m bytewax.run --help` instead"
        )));
    }
    let epoch_interval = epoch_interval.map(|dur| EpochInterval::new(Duration::from_secs_f64(dur)));

    let flow = Py::new(
        py,
        get_flow(
            py,
            &file_path.into(),
            &dataflow_builder,
            dataflow_args.unwrap_or(vec![]),
        )?,
    )?;

    Runner::new(
        flow,
        processes.unwrap_or(1),
        workers_per_process.unwrap_or(1),
        epoch_interval,
        recovery_config,
    )
    .cluster_multiple(py)
}

pub(crate) fn register(_py: Python, m: &PyModule) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(run_main, m)?)?;
    m.add_function(wrap_pyfunction!(cluster_main, m)?)?;
    m.add_function(wrap_pyfunction!(spawn_cluster, m)?)?;
    Ok(())
}
