use std::{
    cell::RefCell,
    process::Command,
    rc::Rc,
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
use timely::{
    communication::Allocate,
    dataflow::{
        operators::{Filter, Probe},
        ProbeHandle,
    },
    progress::Timestamp,
    worker::Worker,
};
use tokio::runtime::Runtime;

use crate::{
    dataflow::Dataflow,
    errors::{prepend_tname, tracked_err, PythonException},
    inputs::EpochInterval,
    recovery::{
        model::{
            FlowStateBytes, KChange, ProgressReader, ResumeEpoch, ResumeFrom, StateReader, StoreKey,
        },
        operators::{read, BroadcastWrite, Recover, Summary, Write},
        python::{default_recovery_config, RecoveryBuilder, RecoveryConfig},
        store::in_mem::{InMemProgress, InMemStore, StoreSummary},
    },
    unwrap_any,
    webserver::run_webserver,
};

use super::{build_production_dataflow, PeriodicSpan, WorkerCount, WorkerIndex};

struct WorkerRunner<'a, A: Allocate> {
    worker: &'a mut Worker<A>,
    index: WorkerIndex,
    count: WorkerCount,
    interrupt_flag: &'a AtomicBool,
    flow: Py<Dataflow>,
    epoch_interval: EpochInterval,
    recovery_config: Py<RecoveryConfig>,
}

impl<'a, A: Allocate> WorkerRunner<'a, A> {
    fn new(
        worker: &'a mut Worker<A>,
        interrupt_flag: &'a AtomicBool,
        flow: Py<Dataflow>,
        epoch_interval: EpochInterval,
        recovery_config: Py<RecoveryConfig>,
    ) -> Self {
        let index = worker.index();
        let peers = worker.peers();
        Self {
            worker,
            index: WorkerIndex(index),
            count: WorkerCount(peers),
            interrupt_flag,
            flow,
            epoch_interval,
            recovery_config,
        }
    }

    fn run(mut self) -> PyResult<()> {
        tracing::info!("Worker {:?} of {:?} starting up", self.index, self.count);
        tracing::info!("Using epoch interval of {:?}", self.epoch_interval);

        let (progress_reader, state_reader) = Python::with_gil(|py| {
            self.recovery_config
                .build_readers(py, self.index.0, self.count.0)
                .reraise("error building recovery readers")
        })?;
        let (progress_writer, state_writer) = Python::with_gil(|py| {
            self.recovery_config
                .build_writers(py, self.index.0, self.count.0)
                .reraise("error building recovery writers")
        })?;

        let span = tracing::trace_span!("Resume epoch").entered();
        let resume_progress = self
            .load_progress(progress_reader)
            .reraise("error while loading recovery progress")?;
        span.exit();

        let resume_from = resume_progress.resume_from();
        tracing::info!("Calculated {resume_from:?}");

        let span = tracing::trace_span!("State loading").entered();
        let ResumeFrom(_ex, resume_epoch) = resume_from;
        let (resume_state, store_summary) = self
            .load_state(resume_epoch, state_reader)
            .reraise("error loading recovery state")?;
        span.exit();

        let span = tracing::trace_span!("Building dataflow").entered();
        let probe = Python::with_gil(|py| {
            build_production_dataflow(
                py,
                self.worker,
                self.flow.clone(),
                self.epoch_interval.clone(),
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

        // Finally run the dataflow here
        self.run_dataflow(probe).reraise("error running Dataflow")?;

        self.shutdown();

        tracing::info!("Worker {:?} of {:?} shut down", self.index, self.count);

        Ok(())
    }

    fn run_dataflow<T: Timestamp>(&mut self, probe: ProbeHandle<T>) -> PyResult<()> {
        let mut span = PeriodicSpan::new(Duration::from_secs(10));
        while !self.interrupt_flag.load(Ordering::Relaxed) && !probe.done() {
            self.worker.step();
            span.update();
            let check = Python::with_gil(|py| py.check_signals());
            if check.is_err() {
                self.interrupt_flag.store(true, Ordering::Relaxed);
                // The ? here will always exit since we just checked
                // that `check` is Result::Err.
                check.reraise("interrupt signal received")?;
            }
        }
        Ok(())
    }

    /// Compile a dataflow which loads the progress data from the previous
    /// cluster.
    ///
    /// Each resume cluster worker is assigned to read the entire progress
    /// data from some previous cluster worker.
    ///
    /// Read state out of the cell once the probe is done. Each resume
    /// cluster worker will have the progress info of all workers in the
    /// previous cluster.
    fn load_progress<R>(&mut self, progress_reader: R) -> PyResult<InMemProgress>
    where
        R: ProgressReader + 'static,
    {
        let (probe, progress_store): (ProbeHandle<()>, _) = self.worker.dataflow(|scope| {
            let mut probe = ProbeHandle::new();
            let resume_progress = Rc::new(RefCell::new(InMemProgress::new(self.count)));

            read(scope, progress_reader, &probe)
                .broadcast_write(resume_progress.clone())
                .probe_with(&mut probe);

            (probe, resume_progress)
        });

        self.run_dataflow(probe)?;

        let resume_progress = Rc::try_unwrap(progress_store)
            .expect("Resume epoch dataflow still has reference to progress_store")
            .into_inner();

        Ok(resume_progress)
    }

    /// Compile a dataflow which loads state data from the previous
    /// cluster.
    ///
    /// Loads up to, but not including, the resume epoch, since the resume
    /// epoch is where input will begin during this recovered cluster.
    ///
    /// Read state out of the cells once the probe is done.
    fn load_state<R>(
        &mut self,
        resume_epoch: ResumeEpoch,
        state_reader: R,
    ) -> PyResult<(FlowStateBytes, StoreSummary)>
    where
        A: Allocate,
        R: StateReader + 'static,
    {
        let (probe, resume_state, summary) = self.worker.dataflow(|scope| {
            let mut probe: ProbeHandle<u64> = ProbeHandle::new();
            let resume_state = Rc::new(RefCell::new(FlowStateBytes::new()));
            let summary = Rc::new(RefCell::new(InMemStore::new()));

            let store_change_stream = read(scope, state_reader, &probe);

            store_change_stream
                // The resume epoch is the epoch we are starting at,
                // so only load state from before < that point. Not
                // <=.
                .filter(move |KChange(StoreKey(epoch, _flow_key), _change)| {
                    epoch.0 < resume_epoch.0
                })
                .recover()
                .write(resume_state.clone())
                .probe_with(&mut probe);

            // Might need to GC writes from some workers even if we
            // shouldn't be loading any state.
            store_change_stream
                .summary()
                .write(summary.clone())
                .probe_with(&mut probe);

            (probe, resume_state, summary)
        });

        self.run_dataflow(probe)?;

        Ok((
            Rc::try_unwrap(resume_state)
                .expect("State loading dataflow still has reference to resume_state")
                .into_inner(),
            Rc::try_unwrap(summary)
                .expect("State loading dataflow still has reference to summary")
                .into_inner(),
        ))
    }

    /// Terminate all dataflows in this worker.
    ///
    /// We need this because otherwise all of Timely's entry points
    /// (e.g. [`timely::execute::execute_from`]) wait until all work is
    /// complete and we will hang.
    fn shutdown(&mut self) {
        for dataflow_id in self.worker.installed_dataflows() {
            self.worker.drop_dataflow(dataflow_id);
        }
    }
}

/// Dataflow runner.
/// This struct is initialized with the parameters that are common
/// between the different execution strategies.
pub(crate) struct DataflowRunner {
    dataflow: Py<Dataflow>,
    processes: usize,
    workers_per_process: usize,
    epoch_interval: Option<EpochInterval>,
    recovery_config: Option<Py<RecoveryConfig>>,
    server_rt: Option<Runtime>,
}

impl DataflowRunner {
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
            server_rt: None,
        }
    }

    /// Run a dataflow in a single worker on a single process
    /// without setting up communication between processes.
    pub(crate) fn simple(mut self, py: Python) -> PyResult<()> {
        tracing::info!("Running single worker on single process");
        let res = py.allow_threads(move || {
            // Initialize the tokio runtime for the webserver if we needed.
            if std::env::var("BYTEWAX_DATAFLOW_API_ENABLED").is_ok() {
                self.run_server().unwrap();
            };
            std::panic::catch_unwind(|| {
                timely::execute::execute_directly::<(), _>(move |worker| {
                    let interrupt_flag = AtomicBool::new(false);

                    let worker_runner = WorkerRunner::new(
                        worker,
                        &interrupt_flag,
                        self.dataflow,
                        self.epoch_interval
                            .unwrap_or(EpochInterval::new(Duration::from_secs(10))),
                        self.recovery_config.unwrap_or(default_recovery_config()),
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

    /// Run multiple processes with automated communication setup.
    /// Can only run processes on the same machine.
    pub(crate) fn cluster_multiple(mut self, py: Python) -> PyResult<()> {
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
                // Initialize the tokio runtime for the webserver if we needed.
                if std::env::var("BYTEWAX_DATAFLOW_API_ENABLED").is_ok() {
                    self.run_server()?;
                    // Also remove the env var so other processes don't run the server.
                    std::env::remove_var("BYTEWAX_DATAFLOW_API_ENABLED");
                };
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
                        if let Some(rt) = self.server_rt.take() {
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

    /// Run a single process with one or more workers.
    /// Requires manual configuration of process id and addresses
    /// of other processes.
    pub(crate) fn cluster_single(
        mut self,
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
                std::io::Write::write_all(&mut stderr, msg.as_bytes()).unwrap_or_else(|err| {
                    eprintln!("Error printing error (that's not good): {err}")
                });
            }));

            // Initialize the tokio runtime for the webserver if we needed.
            if std::env::var("BYTEWAX_DATAFLOW_API_ENABLED").is_ok() {
                self.run_server()?;
            };
            let guards = timely::execute::execute_from::<_, (), _>(
                builders,
                other,
                timely::WorkerConfig::default(),
                move |worker| {
                    let worker_runner = WorkerRunner::new(
                        worker,
                        &should_shutdown_w,
                        self.dataflow.clone(),
                        self.epoch_interval
                            .clone()
                            .unwrap_or(EpochInterval::new(Duration::from_secs(10))),
                        self.recovery_config
                            .clone()
                            .unwrap_or(default_recovery_config()),
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

    fn run_server(&mut self) -> PyResult<()> {
        let rt = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .thread_name("webserver-threads")
            .enable_all()
            .build()
            .raise::<PyRuntimeError>("error initializing tokio runtime for webserver")?;
        let df = Python::with_gil(|py| self.dataflow.extract(py))?;
        rt.spawn(run_webserver(df));
        self.server_rt = Some(rt);
        Ok(())
    }
}
