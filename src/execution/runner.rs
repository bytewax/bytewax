use std::{
    io::Write,
    process::Command,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    thread,
    time::Duration,
};

use pyo3::{
    exceptions::{PyKeyboardInterrupt, PyRuntimeError},
    types::PyType,
    Py, PyErr, PyResult, Python,
};

use crate::{
    dataflow::Dataflow,
    errors::{prepend_tname, tracked_err, PythonException},
    inputs::EpochInterval,
    recovery::python::RecoveryConfig, unwrap_any, execution::worker_main,
};

/// Dataflow runner.
/// This struct is initialized with the parameters that are common
/// between the different execution strategies.
pub(crate) struct Runner {
    dataflow: Py<Dataflow>,
    processes: usize,
    workers_per_process: usize,
    epoch_interval: Option<EpochInterval>,
    recovery_config: Option<Py<RecoveryConfig>>,
}

impl Runner {
    pub(crate) fn new(
        dataflow: Py<Dataflow>,
        processes: usize,
        workers_per_process: usize,
        epoch_interval: Option<EpochInterval>,
        recovery_config: Option<Py<RecoveryConfig>>,
    ) -> Self {
        Self {
            dataflow,
            epoch_interval,
            recovery_config,
            processes,
            workers_per_process,
        }
    }

    /// Run a dataflow in a single worker on a single process
    /// without setting up communication between processes.
    pub(crate) fn simple(self, py: Python) -> PyResult<()> {
        tracing::info!("Running single worker on single process");
        let res = py.allow_threads(move || {
            std::panic::catch_unwind(|| {
                timely::execute::execute_directly::<(), _>(move |worker| {
                    let interrupt_flag = AtomicBool::new(false);
                    // The error will be reraised in the building phase.
                    // If an error occur during the execution, it will
                    // cause a panic since timely doesn't offer a way
                    // to cleanly stop workers with a Result::Err.
                    // The panic will be caught by catch_unwind, so we
                    // unwrap with a PyErr payload.
                    unwrap_any!(worker_main(
                        worker,
                        &interrupt_flag,
                        self.dataflow,
                        self.epoch_interval,
                        self.recovery_config
                    )
                    .reraise("worker error"))
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

    /// Run multiple processes with automated communication setup.
    /// Can only run processes on the same machine.
    pub(crate) fn cluster_multiple(self, py: Python) -> PyResult<()> {
        let proc_id = std::env::var("__BYTEWAX_PROC_ID").ok();

        if self.processes == 1 && self.workers_per_process == 1 {
            self.simple(py)
        } else {
            let addresses = (0..self.processes)
                .map(|proc_id| format!("localhost:{}", proc_id as u64 + 2101))
                .collect();

            if let Some(proc_id) = proc_id {
                self.cluster_single(py, Some(addresses), proc_id.parse().unwrap())?;
            } else {
                let mut ps: Vec<_> = (0..self.processes)
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
                    if ps.iter_mut().all(|ps| match ps.try_wait() {
                        Ok(None) => false,
                        _ => true,
                    }) {
                        break;
                    }

                    let check = Python::with_gil(|py| py.check_signals());
                    if check.is_err() {
                        for process in ps.iter_mut() {
                            process.kill()?;
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

    /// Run a single process with one or more workers.
    /// Requires manual configuration of process id and addresses
    /// of other processes.
    pub(crate) fn cluster_single(
        self,
        py: Python,
        addresses: Option<Vec<String>>,
        proc_id: usize,
    ) -> PyResult<()> {
        tracing::info!(
            "Running {} workers on process {}",
            self.workers_per_process,
            proc_id
        );
        py.allow_threads(move || {
            let addresses = addresses.unwrap_or_default();
            let (builders, other) = if addresses.is_empty() {
                timely::CommunicationConfig::Process(self.workers_per_process)
            } else {
                timely::CommunicationConfig::Cluster {
                    threads: self.workers_per_process,
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
                stderr.write_all(msg.as_bytes()).unwrap_or_else(|err| {
                    eprintln!("Error printing error (that's not good): {err}")
                });
            }));

            let guards = timely::execute::execute_from::<_, (), _>(
                builders,
                other,
                timely::WorkerConfig::default(),
                move |worker| {
                    unwrap_any!(worker_main(
                        worker,
                        &should_shutdown_w,
                        self.dataflow.clone(),
                        self.epoch_interval.clone(),
                        self.recovery_config.clone()
                    ))
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
}
