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
//! compiled into a Timely dataflow in [`crate::worker::build_production_dataflow`].
//!
//! See [`crate::recovery`] for a description of the recovery
//! components added to the Timely dataflow.

use crate::dataflow::Dataflow;
use crate::errors::{prepend_tname, tracked_err, PythonException};
use crate::inputs::EpochInterval;
use crate::recovery::python::default_recovery_config;
use crate::recovery::python::RecoveryConfig;
use crate::unwrap_any;
use crate::webserver::run_webserver;
use crate::worker::Worker;
use pyo3::exceptions::{PyKeyboardInterrupt, PyRuntimeError};
use pyo3::prelude::*;
use pyo3::types::PyType;
use std::process::Command;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread;
use std::time::Duration;
use std::{fs::File, io::Write, sync::Arc};
use tokio::runtime::Runtime;

/// Start the tokio runtime for the webserver.
/// Keep a reference to the runtime for as long as you need it running.
fn start_server_runtime(df: Dataflow) -> PyResult<Runtime> {
    // Since the dataflow can't change at runtime, we encode it as a string of JSON
    // once, when the webserver starts.
    //
    // We also can't create the JSON from within `run_webserver`, as it will
    // prevent the webserver from starting when running with multiple workers or
    // processes.
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
        let mut file = File::create("dataflow.json")?;
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
    tracing::info!("Running single worker on single process");
    let res = py.allow_threads(move || {
        let mut _server_rt = None;
        if std::env::var("BYTEWAX_DATAFLOW_API_ENABLED").is_ok() {
            _server_rt = Some(start_server_runtime(
                Python::with_gil(|py| flow.extract::<Dataflow>(py))
                    .expect("Unable to start Dataflow API server"),
            ));
        }
        std::panic::catch_unwind(|| {
            timely::execute::execute_directly::<(), _>(move |worker| {
                let interrupt_flag = AtomicBool::new(false);

                let worker_runner = Worker::new(
                    worker,
                    &interrupt_flag,
                    flow,
                    epoch_interval.unwrap_or_default(),
                    recovery_config.unwrap_or(default_recovery_config()),
                );
                // The error will be reraised in the building phase.
                // If an error occur during the execution, it will
                // cause a panic since timely doesn't offer a way
                // to cleanly stop workers with a Result::Err.
                // The panic will be caught by catch_unwind, so we
                // unwrap with a PyErr payload.
                unwrap_any!(worker_runner.run().reraise("worker error"))
            })
        })
    });

    res.map_err(|panic_err| {
        // The worker panicked.
        // Print an empty line to separate rust panick message from the rest.
        eprintln!("");
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
    tracing::info!(
        "Running {} workers on process {}",
        worker_count_per_proc,
        proc_id
    );
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
        let should_shutdown_w = should_shutdown.clone();
        let should_shutdown_p = should_shutdown.clone();

        // Custom hook to print the proper stacktrace to stderr
        // before panicking if possible.
        std::panic::set_hook(Box::new(move |info| {
            should_shutdown_p.store(true, Ordering::Relaxed);
            let msg = if let Some(err) = info.payload().downcast_ref::<PyErr>() {
                // Panics with PyErr as payload should come from bytewax.
                Python::with_gil(|py| err.clone_ref(py))
            } else if let Some(msg) = info.payload().downcast_ref::<String>() {
                // Panics with String payload usually comes from timely here.
                tracked_err::<PyRuntimeError>(msg)
            } else if let Some(msg) = info.payload().downcast_ref::<&str>() {
                // Other kind of panics that can be downcasted to &str
                tracked_err::<PyRuntimeError>(msg)
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

        // Initialize the tokio runtime for the webserver if we needed.
        let mut _server_rt = None;
        if std::env::var("BYTEWAX_DATAFLOW_API_ENABLED").is_ok() {
            _server_rt = Some(start_server_runtime(Python::with_gil(|py| {
                flow.extract(py)
            })?)?);
        }
        let guards = timely::execute::execute_from::<_, (), _>(
            builders,
            other,
            timely::WorkerConfig::default(),
            move |worker| {
                let worker_runner = Worker::new(
                    worker,
                    &should_shutdown_w,
                    flow.clone(),
                    epoch_interval.clone().unwrap_or_default(),
                    recovery_config.clone().unwrap_or(default_recovery_config()),
                );
                unwrap_any!(worker_runner.run())
            },
        )
        .raise::<PyRuntimeError>("error during execution")?;

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
            maybe_worker_panic.map_err(|_| {
                tracked_err::<PyRuntimeError>("Worker thread died; look for errors above")
            })?;
        }

        Ok(())
    })
}

/// This is only supposed to be used through `python -m
/// bytewax.run`. See the module docstring for use.
#[pyfunction(
    flow,
    "*",
    processes = 1,
    workers_per_process = 1,
    process_id = "None",
    addresses = "None",
    epoch_interval = "None",
    recovery_config = "None"
)]
pub(crate) fn cli_main(
    py: Python,
    flow: Py<Dataflow>,
    processes: Option<usize>,
    workers_per_process: Option<usize>,
    process_id: Option<usize>,
    addresses: Option<Vec<String>>,
    epoch_interval: Option<f64>,
    recovery_config: Option<Py<RecoveryConfig>>,
) -> PyResult<()> {
    let epoch_interval = epoch_interval.map(|dur| EpochInterval::new(Duration::from_secs_f64(dur)));

    if processes.is_some() && (process_id.is_some() || addresses.is_some()) {
        return Err(tracked_err::<PyRuntimeError>(
            "Can't specify both 'processes' and 'process_id/addresses'",
        ));
    }

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
                let mut server_rt = None;
                // Initialize the tokio runtime for the webserver if we needed.
                if std::env::var("BYTEWAX_DATAFLOW_API_ENABLED").is_ok() {
                    server_rt = Some(start_server_runtime(flow.extract(py)?)?);
                    // Also remove the env var so other processes don't run the server.
                    std::env::remove_var("BYTEWAX_DATAFLOW_API_ENABLED");
                };
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
                loop {
                    if ps.iter_mut().all(|ps| !matches!(ps.try_wait(), Ok(None))) {
                        break;
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
                }
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
