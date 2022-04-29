use crate::recovery::RecoveryConfig;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::sync::Arc;

use std::thread;
use std::time::Duration;

use pyo3::basic::CompareOp;
use pyo3::exceptions::{PyRuntimeError, PyValueError};
use pyo3::prelude::*;
use timely::dataflow::InputHandle;
use timely::dataflow::ProbeHandle;

use crate::dataflow::{build_dataflow, Dataflow};
use crate::pyo3_extensions::{TdPyAny, TdPyCallable, TdPyIterator};

#[pyclass(module = "bytewax")]
#[pyo3(text_signature = "(epoch)")]
pub(crate) struct AdvanceTo {
    #[pyo3(get)]
    epoch: u64,
}

#[pymethods]
impl AdvanceTo {
    #[new]
    fn new(epoch: u64) -> Self {
        Self { epoch }
    }

    fn __richcmp__(&self, other: &Self, op: CompareOp) -> PyResult<bool> {
        match op {
            CompareOp::Lt => Ok(self.epoch < other.epoch),
            CompareOp::Le => Ok(self.epoch <= other.epoch),
            CompareOp::Eq => Ok(self.epoch == other.epoch),
            CompareOp::Ne => Ok(self.epoch != other.epoch),
            CompareOp::Gt => Ok(self.epoch > other.epoch),
            CompareOp::Ge => Ok(self.epoch >= other.epoch),
        }
    }
}

#[pyclass(module = "bytewax")]
#[pyo3(text_signature = "(item)")]
pub(crate) struct Emit {
    #[pyo3(get)]
    item: TdPyAny,
}

#[pymethods]
impl Emit {
    #[new]
    fn new(item: Py<PyAny>) -> Self {
        Self { item: item.into() }
    }
}

/// Encapsulates the process of pulling data out of the input Python
/// iterator and feeding it into Timely.
///
/// This will be called in the worker's "main" loop to feed data in.
pub(crate) struct Pump {
    pull_from_pyiter: TdPyIterator,
    pyiter_is_empty: bool,
    push_to_timely: InputHandle<u64, TdPyAny>,
}

impl Pump {
    pub(crate) fn new(
        pull_from_pyiter: TdPyIterator,
        push_to_timely: InputHandle<u64, TdPyAny>,
    ) -> Self {
        Self {
            pull_from_pyiter,
            pyiter_is_empty: false,
            push_to_timely,
        }
    }

    /// Take a single data element and timestamp and feed it into the
    /// dataflow.
    fn pump(&mut self) {
        Python::with_gil(|py| {
            let mut pull_from_pyiter = self.pull_from_pyiter.0.as_ref(py);
            if let Some(input_or_action) = pull_from_pyiter.next() {
                match input_or_action {
                    Ok(item) => {
                        if let Ok(send) = item.downcast::<PyCell<Emit>>() {
                            self.push_to_timely.send(send.borrow().item.clone());
                        } else if let Ok(advance_to) = item.downcast::<PyCell<AdvanceTo>>() {
                            self.push_to_timely.advance_to(advance_to.borrow().epoch);
                        } else {
                            panic!("Unknown input action")
                        }
                    }
                    Err(err) => {
                        std::panic::panic_any(err);
                    }
                }
            } else {
                self.pyiter_is_empty = true;
            }
        });
    }

    fn input_remains(&self) -> bool {
        !self.pyiter_is_empty
    }
}

/// "Main loop" of a Timely worker thread.
///
/// 1. Pump [`Pump`]s (get input data from Python into Timely).
///
/// 2. Dispose of empty [`Pump`]s and Probes which indicate there's no
/// data left to process in that dataflow.
///
/// 3. Call [timely::worker::Worker::step] to tell Timely to do
/// whatever work it can.
pub(crate) fn worker_main<A>(
    mut pumps_with_input_remaining: Vec<Pump>,
    probe: ProbeHandle<u64>,
    interrupt_flag: &AtomicBool,
    worker: &mut timely::worker::Worker<A>,
) where
    A: timely::communication::Allocate,
{
    while !interrupt_flag.load(Ordering::Relaxed) && !probe.done() {
        if !pumps_with_input_remaining.is_empty() {
            // We have input remaining, pump the pumps, and step the workers while
            // there is work less than the current epoch to do.
            let mut updated_pumps = Vec::new();
            for mut pump in pumps_with_input_remaining.into_iter() {
                pump.pump();
                worker.step_while(|| probe.less_than(&pump.push_to_timely.time()));
                if pump.input_remains() {
                    updated_pumps.push(pump);
                }
            }
            pumps_with_input_remaining = updated_pumps;
        } else {
            // Inputs are empty, step the workers until probe is done.
            worker.step();
        }
    }
}

pub(crate) fn shutdown_worker<A>(worker: &mut timely::worker::Worker<A>)
where
    A: timely::communication::Allocate,
{
    for dataflow_id in worker.installed_dataflows() {
        worker.drop_dataflow(dataflow_id);
    }
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
/// >>> def input_builder(worker_index, worker_count):
/// ...     return enumerate(range(3))
/// >>> def output_builder(worker_index, worker_count):
/// ...     return print
/// >>> run_main(flow, input_builder, output_builder)  # doctest: +ELLIPSIS
/// (...)
///
/// See `bytewax.run()` for a convenience method to not need to worry
/// about input or output builders.
///
/// See `bytewax.spawn_cluster()` for starting a cluster on this
/// machine with full control over inputs and outputs.
///
/// Args:
///
///     flow: Dataflow to run.
///
///     input_builder: Returns input that each worker thread should
///         process. If you are recovering a stateful dataflow, you
///         must ensure your input resumes from the last finalized
///         epoch.
///
///     output_builder: Returns a callback function for each worker
///         thread, called with `(epoch, item)` whenever and item
///         passes by a capture operator on this process.
///
///     recovery_config: State recovery config. See
///         `bytewax.recovery`. If `None`, state will not be persisted.
///
#[pyfunction(flow, input_builder, output_builder, "*", recovery_config = "None")]
#[pyo3(text_signature = "(flow, input_builder, output_builder, *, recovery_config)")]
pub(crate) fn run_main(
    py: Python,
    flow: Py<Dataflow>,
    input_builder: TdPyCallable,
    output_builder: TdPyCallable,
    recovery_config: Option<Py<RecoveryConfig>>,
) -> PyResult<()> {
    let result = py.allow_threads(move || {
        std::panic::catch_unwind(|| {
            // TODO: See if we can PR Timely to not cast result error
            // to a String. Then we could "raise" Python errors from
            // the builder directly. Probably also as part of the
            // panic recast issue below.
            timely::execute::execute_directly::<Result<(), String>, _>(move |worker| {
                let (pump, probe) = Python::with_gil(|py| {
                    let flow = &flow.as_ref(py).borrow();

                    build_dataflow(
                        worker,
                        py,
                        flow,
                        input_builder,
                        output_builder,
                        recovery_config,
                    )
                })?;

                worker_main(vec![pump], probe, &AtomicBool::new(false), worker);

                shutdown_worker(worker);

                Ok(())
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
/// >>> def input_builder(worker_index, worker_count):
/// ...     return enumerate(range(3))
/// >>> def output_builder(worker_index, worker_count):
/// ...     return print
/// >>> cluster_main(flow, input_builder, output_builder)  # doctest: +ELLIPSIS
/// (...)
///
/// See `bytewax.run_main()` for a way to test input and output
/// builders without the complexity of starting a cluster.
///
/// See `bytewax.run_cluster()` for a convenience method to pass data
/// through a dataflow for notebook development.
///
/// See `bytewax.spawn_cluster()` for starting a simple cluster
/// locally on one machine.
///
/// Args:
///
///     flow: Dataflow to run.
///
///     input_builder: Returns input that each worker thread should
///         process. Should yield either `AdvanceTo` or `Emit` to
///         advance the epoch, or input new data into the dataflow.
///         If you are recovering a stateful dataflow, you must ensure
///         your input resumes from the last finalized epoch.
///
///     output_builder: Returns a callback function for each worker
///         thread, called with `(epoch, item)` whenever and item
///         passes by a capture operator on this process.
///         
///     addresses: List of host/port addresses for all processes in
///         this cluster (including this one).
///
///     proc_id: Index of this process in cluster; starts from 0.
///
///     recovery_config: State recovery config. See
///         `bytewax.recovery`. If `None`, state will not be persisted.
///
///     worker_count_per_proc: Number of worker threads to start on
///         each process.
#[pyfunction(
    flow,
    input_builder,
    output_builder,
    addresses,
    proc_id,
    "*",
    recovery_config = "None",
    worker_count_per_proc = "1"
)]
#[pyo3(
    text_signature = "(flow, input_builder, output_builder, addresses, proc_id, *, recovery_config, worker_count_per_proc)"
)]
pub(crate) fn cluster_main(
    py: Python,
    flow: Py<Dataflow>,
    input_builder: TdPyCallable,
    output_builder: TdPyCallable,
    addresses: Option<Vec<String>>,
    proc_id: usize,
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
        // We can drop these if we want to use nightly
        // Thread::is_running
        // https://github.com/rust-lang/rust/issues/90470
        let shutdown_worker_count = Arc::new(AtomicUsize::new(0));
        let shutdown_worker_count_w = shutdown_worker_count.clone();

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
                defer! {
                    shutdown_worker_count_w.fetch_add(1, Ordering::Relaxed);
                }

                let (pump, probe) = Python::with_gil(|py| {
                    let flow = &flow.as_ref(py).borrow();
                    let input_builder = input_builder.clone_ref(py);
                    let output_builder = output_builder.clone_ref(py);
                    let recovery_config = recovery_config.clone();

                    build_dataflow(
                        worker,
                        py,
                        flow,
                        input_builder,
                        output_builder,
                        recovery_config,
                    )
                })?;

                worker_main(vec![pump], probe, &should_shutdown_w, worker);

                shutdown_worker(worker);

                Ok(())
            },
        )
        .map_err(PyRuntimeError::new_err)?;

        // Recreating what Python does in Thread.join() to "block"
        // but also check interrupt handlers.
        // https://github.com/python/cpython/blob/204946986feee7bc80b233350377d24d20fcb1b8/Modules/_threadmodule.c#L81
        let workers_in_proc_count = guards.guards().len();
        while shutdown_worker_count.load(Ordering::Relaxed) < workers_in_proc_count {
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
    m.add_function(wrap_pyfunction!(run_main, m)?)?;
    m.add_function(wrap_pyfunction!(cluster_main, m)?)?;
    m.add_class::<Emit>()?;
    m.add_class::<AdvanceTo>()?;
    Ok(())
}
