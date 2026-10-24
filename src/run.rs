//! Internal code for dataflow execution.
//!
//! For a user-centric version of how to execute dataflows, read the
//! the `bytewax.execution` Python module docstring. Read that first.

use std::fs::File;
use std::io::Write;
use std::path::PathBuf;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::sync::mpsc;
use std::sync::mpsc::Receiver;
use std::sync::mpsc::Sender;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::PoisonError;
use std::thread;
use std::time::Duration;

use pyo3::exceptions::PyKeyboardInterrupt;
use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;
use pyo3::types::PyType;
use tokio::runtime::Runtime;

use crate::dataflow::Dataflow;
use crate::errors::tracked_err;
use crate::errors::PythonException;
use crate::inputs::EpochInterval;
use crate::metrics::initialize_metrics;
use crate::recovery::ExecutionNumber;
use crate::recovery::RecoveryConfig;
use crate::unwrap_any;
use crate::webserver::run_webserver;
use crate::worker::worker_main;

/// Set by `request_graceful_drain`, and cleared when the last execution
/// in the process ends. A request made while no execution runs is kept
/// for the next one.
///
/// Inputs treat this as EOF, so the execution ends at a consistent
/// point that the next execution resumes from exactly. Stateful
/// operators see the same EOF but pause rather than finalize; see
/// the EOF handling in `operators.rs`.
pub(crate) static DRAIN: AtomicBool = AtomicBool::new(false);

/// Number of executions in progress in this process, of any kind.
static EXECUTING: AtomicUsize = AtomicUsize::new(0);

/// Number of executions in progress that cannot be drained: those
/// without a recovery config, and multi-process clusters.
///
/// A drain would discard the open windows and folds of an execution
/// without recovery. The processes of a multi-process cluster would not
/// agree on the end of input, so a drain in one process would finish
/// the others early. A request is refused while one of these runs, and
/// a drain pending in the process keeps one from starting.
static UNDRAINABLE: AtomicUsize = AtomicUsize::new(0);

/// Serializes the checks and updates of the drain state above, so a
/// request and an execution starting or ending cannot interleave
/// between a check and the update it guards.
static DRAIN_STATE: Mutex<()> = Mutex::new(());

/// Counts an execution as in progress until it is dropped.
struct ExecutionGuard {
    drainable: bool,
}

impl ExecutionGuard {
    /// Start an execution.
    ///
    /// `drainable` is whether it has a recovery config and runs in a
    /// single process.
    fn begin(drainable: bool) -> PyResult<Self> {
        let _state = DRAIN_STATE.lock().unwrap_or_else(PoisonError::into_inner);
        // A pending drain would stop an execution that cannot be drained
        // partway, so one cannot start while the drain is pending.
        if !drainable && DRAIN.load(Ordering::Acquire) {
            let msg = "a graceful drain is pending in this process, so an execution that cannot be drained cannot start: without a recovery config a drain would discard its open windows and folds, and a multi-process cluster cannot be drained; the pending drain applies to the next single-process execution with a recovery config";
            return Err(tracked_err::<PyRuntimeError>(msg));
        }
        EXECUTING.fetch_add(1, Ordering::AcqRel);
        if !drainable {
            UNDRAINABLE.fetch_add(1, Ordering::AcqRel);
        }
        Ok(Self { drainable })
    }
}

impl Drop for ExecutionGuard {
    fn drop(&mut self) {
        let _state = DRAIN_STATE.lock().unwrap_or_else(PoisonError::into_inner);
        if !self.drainable {
            UNDRAINABLE.fetch_sub(1, Ordering::AcqRel);
        }
        // The last execution to end clears the request, so a request made
        // during it does not reach the next execution. A request made
        // while none runs is kept for the next execution.
        if EXECUTING.fetch_sub(1, Ordering::AcqRel) == 1 {
            DRAIN.store(false, Ordering::Release);
        }
    }
}

/// Start the tokio runtime for the webserver.
/// Keep a reference to the runtime for as long as you need it running.
fn start_server_runtime(df: Dataflow) -> PyResult<Runtime> {
    let mut json_path =
        PathBuf::from(std::env::var("BYTEWAX_DATAFLOW_API_CACHE_PATH").unwrap_or(".".to_string()));
    json_path.push("dataflow.json");

    // Since the dataflow can't change at runtime, we encode it as a
    // string of JSON once, when the webserver starts.
    let dataflow_json: String = Python::with_gil(|py| -> PyResult<String> {
        let vis_mod = PyModule::import(py, "bytewax.visualize")?;
        let to_json = vis_mod.getattr("to_json")?;

        let dataflow_json = to_json
            .call1((df,))
            .reraise("error calling `bytewax.visualize.to_json`")?
            .to_string();

        // Since the dataflow can crash, write the dataflow JSON to a file
        let mut file = File::create(json_path)?;
        file.write_all(dataflow_json.as_bytes())?;

        Ok(dataflow_json)
    })?;

    // Start the metrics subsystem
    initialize_metrics()?;

    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .thread_name("webserver-threads")
        .enable_all()
        .build()
        .raise::<PyRuntimeError>("error initializing tokio runtime for webserver")?;
    rt.spawn(run_webserver(dataflow_json));
    Ok(rt)
}

/// Execute a dataflow in the current thread.
///
/// Blocks until execution is complete.
///
/// This is only used for unit testing. See `bytewax.run`.
///
/// ```{testcode}
/// from bytewax.dataflow import Dataflow
/// import bytewax.operators as op
/// from bytewax.testing import TestingSource, run_main
/// from bytewax.connectors.stdio import StdOutSink
/// flow = Dataflow("my_df")
/// s = op.input("inp", flow, TestingSource(range(3)))
/// op.output("out", s, StdOutSink())
///
/// run_main(flow)
/// ```
///
/// ```{testoutput}
/// 0
/// 1
/// 2
/// ```
///
/// :arg flow: Dataflow to run.
///
/// :type flow: bytewax.dataflow.Dataflow
///
/// :arg epoch_interval: System time length of each epoch. Defaults to
///     10 seconds.
///
/// :type epoch_interval: typing.Optional[datetime.timedelta]
///
/// :arg recovery_config: State recovery config. If `None`, state will
///     not be persisted.
///
/// :type recovery_config:
///     typing.Optional[bytewax.recovery.RecoveryConfig]
#[pyfunction]
#[pyo3(
    signature = (flow, *, epoch_interval = None, recovery_config = None)
)]
pub(crate) fn run_main(
    py: Python,
    flow: Dataflow,
    epoch_interval: Option<EpochInterval>,
    recovery_config: Option<Py<RecoveryConfig>>,
) -> PyResult<()> {
    tracing::warn!("Running single worker on single process");

    let epoch_interval = epoch_interval.unwrap_or_default();
    tracing::info!("Using epoch interval of {:?}", epoch_interval);

    let _execution = ExecutionGuard::begin(recovery_config.is_some())?;
    let recovering = recovery_config.is_some();
    let res = py.allow_threads(move || {
        std::panic::catch_unwind(|| {
            timely::execute::execute_directly::<Option<ExecutionNumber>, _>(move |worker| {
                unwrap_any!(worker_main(
                    worker,
                    // Since there are no other threads, directly
                    // detect signals in the dataflow run loop.
                    || {
                        unwrap_any!(Python::with_gil(
                            |py| Python::check_signals(py).reraise("signal received")
                        ));
                        // We'll panic directly and don't need to
                        // interrupt.
                        false
                    },
                    flow,
                    epoch_interval,
                    recovery_config
                ))
            })
        })
    });

    let closed = res.map_err(|panic_err| {
        // The worker panicked.
        // Print an empty line to separate rust panic message from the rest.
        eprintln!();
        if let Some(err) = panic_err.downcast_ref::<PyErr>() {
            // Special case for keyboard interrupt.
            if err.get_type(py).is(PyType::new::<PyKeyboardInterrupt>(py)) {
                tracked_err::<PyKeyboardInterrupt>(
                    "interrupt signal received, all processes have been shut down",
                )
            } else {
                // Panics with PyErr as payload should come from bytewax.
                err.clone_ref(py)
            }
        } else if let Some(msg) = panic_err.downcast_ref::<String>() {
            // Panics with String payload usually comes from timely here.
            tracked_err::<PyRuntimeError>(msg)
        } else if let Some(msg) = panic_err.downcast_ref::<&str>() {
            // Panic with &str payload, usually from a direct call to `panic!`
            // or `.expect`
            tracked_err::<PyRuntimeError>(msg)
        } else {
            // Give up trying to understand the error, and show the user
            // a really helpful message.
            // We could show the debug representation of `panic_err`, but
            // it would just be `Any { .. }`
            tracked_err::<PyRuntimeError>("unknown error")
        }
    })?;
    if let Some(ex) = closed.filter(|_| recovering) {
        tracing::info!("Execution {ex} closed cleanly");
    }
    Ok(())
}

/// Execute a dataflow in the current process as part of a cluster.
///
/// This is only used for unit testing. See `bytewax.run`.
///
/// Blocks until execution is complete.
///
/// ```{testcode}
/// from bytewax.dataflow import Dataflow
/// import bytewax.operators as op
/// from bytewax.testing import TestingSource, cluster_main
/// from bytewax.connectors.stdio import StdOutSink
///
/// flow = Dataflow("my_df")
/// s = op.input("inp", flow, TestingSource(range(3)))
/// op.output("out", s, StdOutSink())
///
/// # In a real example, use "host:port" of all other workers.
/// addresses = []
/// proc_id = 0
/// cluster_main(flow, addresses, proc_id)
/// ```
///
/// ```{testoutput}
/// 0
/// 1
/// 2
/// ```
///
/// :arg flow: Dataflow to run.
///
/// :type flow: bytewax.dataflow.Dataflow
///
/// :arg addresses: List of host/port addresses for all processes in
///     this cluster (including this one).
///
/// :type addresses: typing.List[str]
///
/// :arg proc_id: Index of this process in cluster; starts from 0.
///
/// :type proc_id: int
///
/// :arg epoch_interval: System time length of each epoch. Defaults to
///     10 seconds.
///
/// :type epoch_interval: typing.Optional[datetime.timedelta]
///
/// :arg recovery_config: State recovery config. If `None`, state will
///     not be persisted.
///
/// :type recovery_config:
///     typing.Optional[bytewax.recovery.RecoveryConfig]
///
/// :arg worker_count_per_proc: Number of worker threads to start on
///     each process. Defaults to `1`.
///
/// :type worker_count_per_proc: int
#[pyfunction]
#[pyo3(
    signature = (flow, addresses, proc_id, *, epoch_interval = None, recovery_config = None, worker_count_per_proc = 1)
)]
pub(crate) fn cluster_main(
    py: Python,
    flow: Dataflow,
    addresses: Option<Vec<String>>,
    proc_id: usize,
    epoch_interval: Option<EpochInterval>,
    recovery_config: Option<Py<RecoveryConfig>>,
    worker_count_per_proc: usize,
) -> PyResult<()> {
    tracing::warn!(
        "Running {} workers on process {}",
        worker_count_per_proc,
        proc_id
    );

    let epoch_interval = epoch_interval.unwrap_or_default();
    tracing::info!("Using epoch interval of {:?}", epoch_interval);

    let multi_process = addresses.as_ref().is_some_and(|addrs| !addrs.is_empty());
    let _execution = ExecutionGuard::begin(recovery_config.is_some() && !multi_process)?;
    let recovering = recovery_config.is_some();
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
        .raise::<PyRuntimeError>("error building timely communication pipeline")?;

        let should_shutdown = Arc::new(AtomicBool::new(false));
        let should_shutdown_p = should_shutdown.clone();
        let should_shutdown_w = should_shutdown.clone();
        let (tx, rx): (Sender<PyErr>, Receiver<PyErr>) = mpsc::channel();
        let panic_tx = tx.clone();
        // Custom hook to push the panic error into a channel
        // before panicking.
        std::panic::set_hook(Box::new(move |info| {
            should_shutdown_p.store(true, Ordering::Relaxed);

            let err = if let Some(err) = info.payload().downcast_ref::<PyErr>() {
                // Panics with PyErr as payload should come from bytewax.
                Python::with_gil(|py| err.clone_ref(py))
            } else {
                // Give up trying to understand the error,
                // and show the user what we have.
                tracked_err::<PyRuntimeError>(&format!("{info}"))
            };
            // TODO: This block handles an unfortunate interaction
            // with our test suite.
            //
            // When multiple entry point calls are made for a test
            // this panic hook is set during runs with `cluster_main`,
            // but can be invoked during subsequent invocations of
            // `run_main`, crashing the test suite.
            Python::with_gil(|py| {
                let channel_err = err.clone_ref(py);
                panic_tx.send(channel_err).unwrap_or_else(|_| {
                    err.print(py);
                });
            });
        }));

        let guards = timely::execute::execute_from::<_, Option<ExecutionNumber>, _>(
            builders,
            other,
            timely::WorkerConfig::default(),
            move |worker| {
                let flow = Python::with_gil(|py| flow.clone_ref(py));
                let recovery_config =
                    Python::with_gil(|py| recovery_config.as_ref().map(|rc| rc.clone_ref(py)));

                unwrap_any!(worker_main(
                    worker,
                    // Interrupt if the main thread detects a signal.
                    || should_shutdown_w.load(Ordering::Relaxed),
                    flow,
                    epoch_interval,
                    recovery_config
                ))
            },
        )
        .reraise("error during execution")?;

        let cooldown = Duration::from_millis(1);
        // Recreating what Python does in Thread.join() to "block"
        // but also check interrupt handlers.
        // https://github.com/python/cpython/blob/204946986feee7bc80b233350377d24d20fcb1b8/Modules/_threadmodule.c#L81
        while guards
            .guards()
            .iter()
            .any(|worker_thread| !worker_thread.is_finished())
        {
            thread::sleep(cooldown);
            // The compiler can't figure out the lifetimes work out.
            #[allow(clippy::redundant_closure)]
            Python::with_gil(|py| Python::check_signals(py)).inspect_err(|_err| {
                should_shutdown.store(true, Ordering::Relaxed);
            })?;
        }
        let mut closed = Vec::new();
        for maybe_worker_panic in guards.join() {
            closed.push(maybe_worker_panic.map_err(|_| {
                rx.try_recv()
                    .expect("unable to receive panic error from channel")
            })?);
        }
        // Log once per process, and only if every local worker completed.
        let closed: Option<Vec<ExecutionNumber>> = closed.into_iter().collect();
        if let Some(ex) = closed.and_then(|exs| exs.first().copied()) {
            if recovering {
                tracing::info!("Execution {ex} closed cleanly");
            }
        }

        Ok(())
    })
}

/// Ask the execution in progress to drain and exit cleanly.
///
/// A drain needs a single-process execution with a recovery config.
/// Without a recovery config it would discard open windows and folds,
/// so this raises `RuntimeError` while such an execution runs in this
/// process. A multi-process cluster is not drained at all, so this
/// raises while one runs too. A request made while no execution runs is
/// kept for the next execution in this process, which then refuses to
/// start unless it can be drained. The last execution to end clears it.
///
/// Inputs stop reading new data and treat the request as EOF, so each
/// partition snapshots its exact position and the execution closes
/// through the normal EOF path. A following execution on the same
/// recovery store then resumes with no replayed items.
///
/// Stateful operators pause rather than finish. Their logic is kept
/// and snapshotted, so an open window or fold is not emitted early,
/// and the next execution continues it. Only a true end of input runs
/// their final output.
///
/// To drain on SIGTERM, install a handler before the execution starts.
/// SIGTERM keeps its default action otherwise.
///
/// % Skip this doctest because it installs a signal handler.
///
/// ```python
/// import signal
/// from bytewax.run import request_graceful_drain
///
///
/// def _drain_on_sigterm(signum, frame):
///     # A second SIGTERM ends the process the usual way.
///     signal.signal(signal.SIGTERM, signal.SIG_DFL)
///     request_graceful_drain()
///
///
/// signal.signal(signal.SIGTERM, _drain_on_sigterm)
/// ```
///
/// A multi-process cluster is resumed by recovery after a crash or a
/// kill, as before. In a container, the process must receive the
/// signal: run Python in exec form, not under `sh -c`, or under an init
/// such as `tini`.
#[pyfunction]
pub(crate) fn request_graceful_drain() -> PyResult<()> {
    let _state = DRAIN_STATE.lock().unwrap_or_else(PoisonError::into_inner);
    if UNDRAINABLE.load(Ordering::Acquire) > 0 {
        let msg = "graceful drain needs a single-process execution with a recovery config: an execution in this process has no recovery config or runs on multiple processes, and cannot be drained; without recovery a drain would discard its open windows and folds, and processes of a multi-process cluster would not agree on the end of input";
        return Err(tracked_err::<PyRuntimeError>(msg));
    }
    DRAIN.store(true, Ordering::Release);
    Ok(())
}

#[pyfunction]
#[pyo3(
    signature=(flow, *, workers_per_process=1, process_id=None, addresses=None, epoch_interval=None, recovery_config=None)
)]
#[allow(clippy::too_many_arguments)]
pub(crate) fn cli_main(
    py: Python,
    flow: Dataflow,
    workers_per_process: Option<usize>,
    process_id: Option<usize>,
    addresses: Option<Vec<String>>,
    epoch_interval: Option<EpochInterval>,
    recovery_config: Option<Py<RecoveryConfig>>,
) -> PyResult<()> {
    let mut _server_rt = None;

    // Initialize the tokio runtime for the webserver if we needed.
    if std::env::var("BYTEWAX_DATAFLOW_API_ENABLED").is_ok() {
        _server_rt = Some(start_server_runtime(flow.clone_ref(py))?);
    };

    let workers_per_process = workers_per_process.unwrap_or(1);

    if workers_per_process == 1 && addresses.is_none() {
        run_main(py, flow, epoch_interval, recovery_config)?;
    } else {
        cluster_main(
            py,
            flow,
            addresses,
            process_id.unwrap_or(0),
            epoch_interval,
            recovery_config,
            workers_per_process,
        )?;
    }
    Ok(())
}

pub(crate) fn register(_py: Python, m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(run_main, m)?)?;
    m.add_function(wrap_pyfunction!(cluster_main, m)?)?;
    m.add_function(wrap_pyfunction!(cli_main, m)?)?;
    m.add_function(wrap_pyfunction!(request_graceful_drain, m)?)?;
    Ok(())
}
