//! Internal code for dataflow execution.
//!
//! For a user-centric version of how to execute dataflows, read the
//! the `bytewax.execution` Python module docstring. Read that first.

use std::fs::File;
use std::io::Write;
use std::path::PathBuf;
use std::process::Command;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::thread;
use std::time::Duration;

use pyo3::exceptions::PyKeyboardInterrupt;
use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;
use pyo3::types::PyType;
use tokio::runtime::Runtime;

use crate::dataflow::Dataflow;
use crate::errors::prepend_tname;
use crate::errors::tracked_err;
use crate::errors::PythonException;
use crate::inputs::EpochInterval;
use crate::recovery::RecoveryConfig;
use crate::unwrap_any;
use crate::webserver::run_webserver;
use crate::worker::worker_main;

/// Start the tokio runtime for the webserver.
/// Keep a reference to the runtime for as long as you need it running.
fn start_server_runtime(df: &Py<Dataflow>) -> PyResult<Runtime> {
    let mut json_path =
        PathBuf::from(std::env::var("BYTEWAX_DATAFLOW_API_CACHE_PATH").unwrap_or(".".to_string()));
    json_path.push("dataflow.json");

    // Since the dataflow can't change at runtime, we encode it as a
    // string of JSON once, when the webserver starts.
    let dataflow_json: String = Python::with_gil(|py| -> PyResult<String> {
        let encoder_module = PyModule::import(py, "bytewax._encoder")
            .raise::<PyRuntimeError>("Unable to load Bytewax encoder module")?;
        // For convenience, we are using a helper function supplied in the
        // bytewax.encoder module.
        let encode = encoder_module
            .getattr("encode_dataflow")
            .raise::<PyRuntimeError>("Unable to load encode_dataflow function")?;

        let dataflow_json = encode
            .call1((df,))
            .reraise("error encoding dataflow")?
            .to_string();
        // Since the dataflow can crash, write the dataflow JSON to a file
        let mut file = File::create(json_path)?;
        file.write_all(dataflow_json.as_bytes())?;

        Ok(dataflow_json)
    })?;

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
/// >>> from bytewax.dataflow import Dataflow
/// >>> from bytewax.testing import TestingInput, run_main
/// >>> from bytewax.connectors.stdio import StdOutput
/// >>> flow = Dataflow()
/// >>> flow.input("inp", TestingInput(range(3)))
/// >>> flow.capture(StdOutput())
/// >>> run_main(flow)
/// 0
/// 1
/// 2
///
/// Args:
///
///   flow: Dataflow to run.
///
///   epoch_interval (datetime.timedelta): System time length of each
///       epoch. Defaults to 10 seconds.
///
///   recovery_config (bytewax.recovery.RecoveryConfig): State
///       recovery config. If `None`, state will not be persisted.
///
#[pyfunction]
#[pyo3(
    signature = (flow, *, epoch_interval = None, recovery_config = None),
    text_signature = "(flow, *, epoch_interval = None, recovery_config = None)",
)]
pub(crate) fn run_main(
    py: Python,
    flow: Py<Dataflow>,
    epoch_interval: Option<EpochInterval>,
    recovery_config: Option<Py<RecoveryConfig>>,
) -> PyResult<()> {
    tracing::info!("Running single worker on single process");

    let epoch_interval = epoch_interval.unwrap_or_default();
    tracing::info!("Using epoch interval of {:?}", epoch_interval);

    let res = py.allow_threads(move || {
        std::panic::catch_unwind(|| {
            timely::execute::execute_directly::<(), _>(move |worker| {
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

    res.map_err(|panic_err| {
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
    })
}

/// Execute a dataflow in the current process as part of a cluster.
///
/// This is only used for unit testing. See `bytewax.run`.
///
/// Blocks until execution is complete.
///
/// >>> from bytewax.dataflow import Dataflow
/// >>> from bytewax.testing import TestingInput
/// >>> from bytewax.connectors.stdio import StdOutput
/// >>> flow = Dataflow()
/// >>> flow.input("inp", TestingInput(range(3)))
/// >>> flow.capture(StdOutput())
/// >>> addresses = []  # In a real example, you'd find the "host:port" of all other Bytewax workers.
/// >>> proc_id = 0  # In a real example, you'd assign each worker a distinct ID from 0..proc_count.
/// >>> cluster_main(flow, addresses, proc_id)
/// 0
/// 1
/// 2
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
///   recovery_config (bytewax.recovery.RecoveryConfig): State
///       recovery config. If `None`, state will not be persisted.
///
///   worker_count_per_proc: Number of worker threads to start on
///       each process.
#[pyfunction]
#[pyo3(
    signature = (flow, addresses, proc_id, *, epoch_interval = None, recovery_config = None, worker_count_per_proc = 1),
    text_signature = "(flow, addresses, proc_id, *, epoch_interval = None, recovery_config = None, worker_count_per_proc = 1)"
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
    tracing::info!(
        "Running {} workers on process {}",
        worker_count_per_proc,
        proc_id
    );

    let epoch_interval = epoch_interval.unwrap_or_default();
    tracing::info!("Using epoch interval of {:?}", epoch_interval);

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
        // Custom hook to print the proper stacktrace to stderr
        // before panicking if possible.
        std::panic::set_hook(Box::new(move |info| {
            should_shutdown_p.store(true, Ordering::Relaxed);

            let msg = if let Some(err) = info.payload().downcast_ref::<PyErr>() {
                // Panics with PyErr as payload should come from bytewax.
                Python::with_gil(|py| err.clone_ref(py))
            } else {
                // Give up trying to understand the error,
                // and show the user what we have.
                tracked_err::<PyRuntimeError>(&format!("{info}"))
            };
            // Prepend the name of the thread to each line
            let msg = prepend_tname(msg.to_string());
            // Acquire stdout lock and write the string as bytes,
            // so we avoid interleaving outputs from different threads (i think?).
            let mut stderr = std::io::stderr().lock();
            std::io::Write::write_all(&mut stderr, msg.as_bytes())
                .unwrap_or_else(|err| eprintln!("Error printing error (that's not good): {err}"));
        }));

        let guards = timely::execute::execute_from::<_, (), _>(
            builders,
            other,
            timely::WorkerConfig::default(),
            move |worker| {
                let flow = Python::with_gil(|py| flow.clone_ref(py));
                let recovery_config = recovery_config.clone();

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
        .raise::<PyRuntimeError>("error during execution")?;

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
            maybe_worker_panic.map_err(|_| {
                tracked_err::<PyRuntimeError>("Worker thread died; look for errors above")
            })?;
        }

        Ok(())
    })
}

/// This is only supposed to be used through `python -m
/// bytewax.run`. See the module docstring for use.
#[pyfunction]
#[pyo3(
    signature=(flow, *, processes=1, workers_per_process=1, process_id=None, addresses=None, epoch_interval=None, recovery_config=None),
    text_signature="(flow, *, processes=1, workers_per_process=1, process_id=None, addresses=None, epoch_interval=None, recovery_config=None)"
)]
#[allow(clippy::too_many_arguments)]
pub(crate) fn cli_main(
    py: Python,
    flow: Py<Dataflow>,
    processes: Option<usize>,
    workers_per_process: Option<usize>,
    process_id: Option<usize>,
    addresses: Option<Vec<String>>,
    epoch_interval: Option<EpochInterval>,
    recovery_config: Option<Py<RecoveryConfig>>,
) -> PyResult<()> {
    if processes.is_some() && (process_id.is_some() || addresses.is_some()) {
        return Err(tracked_err::<PyRuntimeError>(
            "Can't specify both 'processes' and 'process_id/addresses'",
        ));
    }

    let mut server_rt = None;
    // Initialize the tokio runtime for the webserver if we needed.
    if std::env::var("BYTEWAX_DATAFLOW_API_ENABLED").is_ok() {
        server_rt = Some(start_server_runtime(&flow)?);
        // Also remove the env var so other processes don't run the server.
        std::env::remove_var("BYTEWAX_DATAFLOW_API_ENABLED");
    };

    if let Some(proc_id) = process_id {
        cluster_main(
            py,
            flow,
            addresses,
            proc_id,
            epoch_interval,
            recovery_config,
            workers_per_process.unwrap_or(1),
        )
    } else {
        let proc_id = std::env::var("__BYTEWAX_PROC_ID").ok();

        let processes = processes.unwrap_or(1);
        let workers_per_process = workers_per_process.unwrap_or(1);

        if processes == 1 && workers_per_process == 1 {
            run_main(py, flow, epoch_interval, recovery_config)
        } else {
            let addresses = (0..processes)
                .map(|proc_id| format!("localhost:{}", proc_id as u64 + 2101))
                .collect();

            if let Some(proc_id) = proc_id {
                cluster_main(
                    py,
                    flow,
                    Some(addresses),
                    proc_id.parse().unwrap(),
                    epoch_interval,
                    recovery_config,
                    workers_per_process,
                )?;
            } else {
                let mut ps: Vec<_> = (0..processes)
                    .map(|proc_id| {
                        let mut args = std::env::args();
                        Command::new(args.next().unwrap())
                            .env("__BYTEWAX_PROC_ID", proc_id.to_string())
                            .args(args.collect::<Vec<String>>())
                            .spawn()
                            .unwrap()
                    })
                    .collect();
                // Allow threads here in order to not block any other
                // background threads from taking the GIL
                py.allow_threads(|| -> PyResult<()> {
                    let cooldown = Duration::from_millis(1);
                    loop {
                        if ps.iter_mut().all(|ps| !matches!(ps.try_wait(), Ok(None))) {
                            return Ok(());
                        }

                        let check = Python::with_gil(|py| py.check_signals());
                        if check.is_err() {
                            for process in ps.iter_mut() {
                                process.kill()?;
                            }
                            // Don't forget to shutdown the server runtime.
                            // If we just drop the runtime, it will wait indefinitely
                            // that the server stops, so we need to stop it manually.
                            if let Some(rt) = server_rt.take() {
                                rt.shutdown_timeout(Duration::from_secs(0));
                            }

                            // The ? here will always exit since we just checked
                            // that `check` is Result::Err.
                            check.reraise(
                                "interrupt signal received, all processes have been shut down",
                            )?;
                        }
                        thread::sleep(cooldown);
                    }
                })?;
            }
            Ok(())
        }
    }
}

pub(crate) fn register(_py: Python, m: &PyModule) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(run_main, m)?)?;
    m.add_function(wrap_pyfunction!(cluster_main, m)?)?;
    m.add_function(wrap_pyfunction!(cli_main, m)?)?;
    Ok(())
}
