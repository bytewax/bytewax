//! Internal code for dataflow execution.
//!
//! For a user-centric version of how to execute dataflows, read the
//! the `bytewax.execution` Python module docstring. Read that first.
//!
//! See [`build_dataflow()`] and [`worker_main()`] for the main parts
//! of this.

use crate::operators::window::aggregate_window;
use crate::operators::window::TumblingWindow;
use crate::operators::DataflowFrontier;
use crate::operators::Recovery;
use crate::recovery::build_state_caches;
use crate::recovery::RecoveryConfig;
use send_wrapper::SendWrapper;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use timely::scheduling::Scheduler;

use std::thread;
use std::time::Duration;

use log::debug;
use pyo3::basic::CompareOp;
use pyo3::exceptions::{PyRuntimeError, PyValueError};
use pyo3::prelude::*;
use timely::dataflow::operators::aggregation::*;
use timely::dataflow::operators::generic::operator::source;
use timely::dataflow::operators::*;
use timely::dataflow::InputHandle;
use timely::dataflow::ProbeHandle;

use crate::dataflow::{Dataflow, Step};
use crate::operators::build;
use crate::operators::check_complete;
use crate::operators::stateful_map;
use crate::operators::Reduce;
use crate::operators::{
    capture, filter, flat_map, inspect, inspect_epoch, map, reduce, reduce_epoch,
    reduce_epoch_local, StatefulMap,
};
use crate::pyo3_extensions::{extract_state_pair, wrap_state_pair, StateKey};
use crate::pyo3_extensions::{TdPyAny, TdPyCallable, TdPyIterator};
use crate::recovery::build_recovery_store;
use std::collections::HashMap;

/// Turn the abstract blueprint for a dataflow into a Timely dataflow
/// so it can be executed.
///
/// This is more complicated than a 1:1 translation of Bytewax
/// concepts to Timely, as we are using Timely as a basis to implement
/// more-complicated Bytewax features like input builders and
/// recovery.
///
/// The main steps here are:
///
/// 1. Load up relevant state from the recovery store and transform
/// that into what the stateful operators and garbage collector need.
///
/// 2. Call the input and output builders to get our callbacks for the
/// [`Pump`] and capture operator.
///
/// 3. "Compile" the Bytewax [`crate::dataflow::Dataflow`] into a
/// Timely dataflow. This involves re-using some Timely operators
/// (e.g. [`timely::dataflow::operators::map::Map`]), abusing some
/// Timely operators with different names but the correct
/// functionality
/// (e.g. [`timely::dataflow::operators::inspect::Inspect`]), using
/// our own custom operators (e.g. [`crate::operators::Reduce`]).
///
/// 4. Put in utility recovery
/// machinery. (C.f. [`crate::operators::Recovery`] and
/// [`crate::recovery`]).
///
/// 5. Return the [`Pump`] and execution frontier probe for execution.
pub(crate) fn build_dataflow<A>(
    timely_worker: &mut timely::worker::Worker<A>,
    py: Python,
    flow: &Dataflow,
    input_builder: TdPyCallable,
    output_builder: TdPyCallable,
    recovery_config: Option<Py<RecoveryConfig>>,
) -> Result<(Pump, ProbeHandle<u64>), String>
where
    A: timely::communication::Allocate,
{
    let worker_index = timely_worker.index();
    let worker_count = timely_worker.peers();

    timely_worker.dataflow(|scope| {
        let mut timely_input = InputHandle::new();
        let mut execution_frontier = ProbeHandle::new();
        let mut stream = timely_input.to_stream(scope);
        let mut state_updates = Vec::new();
        let mut captures = Vec::new();

        let recovery_store = build_recovery_store(py, recovery_config)?;

        // SAFETY: Avoid PyO3 Send overloading.
        let wrapped_recovery_store = SendWrapper::new(recovery_store);
        // All RecoveryStore methods require not holding the GIL. They
        // might spawn background threads.
        let (recovery_data, resume_epoch) = py.allow_threads(|| wrapped_recovery_store.load());

        // Lock in that we're at the recovery epoch. Timely will error
        // if the input handler does not start here.
        timely_input.advance_to(resume_epoch);
        // Yes this is a clone of a giant Vec, but I think this'll be
        // fine (at least from a memory perspective) because we're
        // dropping all the actual state data and just noting if it's
        // an upsert or delete.
        let recovery_store_log = recovery_data
            .clone()
            .into_iter()
            .map(|(step_id, key, epoch, state)| (step_id, key, epoch, state.into()))
            .collect();
        let mut state_caches = build_state_caches(recovery_data);

        let worker_input: TdPyIterator = input_builder
            .call1(py, (worker_index, worker_count, resume_epoch))
            .unwrap()
            .extract(py)
            .unwrap();
        let worker_output: TdPyCallable = output_builder
            .call1(py, (worker_index, worker_count))
            .unwrap()
            .extract(py)
            .unwrap();

        for step in &flow.steps {
            match step {
                Step::Map { mapper } => {
                    // All these closure lifetimes are static, so tell
                    // Python's GC that there's another pointer to the
                    // mapping function that's going to hang around
                    // for a while when it's moved into the closure.
                    let mapper = mapper.clone_ref(py);
                    stream = stream.map(move |item| map(&mapper, item));
                }
                Step::FlatMap { mapper } => {
                    let mapper = mapper.clone_ref(py);
                    stream = stream.flat_map(move |item| flat_map(&mapper, item));
                }
                Step::Filter { predicate } => {
                    let predicate = predicate.clone_ref(py);
                    stream = stream.filter(move |item| filter(&predicate, item));
                }
                Step::Inspect { inspector } => {
                    let inspector = inspector.clone_ref(py);
                    stream = stream.inspect(move |item| inspect(&inspector, item));
                }
                Step::InspectEpoch { inspector } => {
                    let inspector = inspector.clone_ref(py);
                    stream = stream
                        .inspect_time(move |epoch, item| inspect_epoch(&inspector, epoch, item));
                }
                Step::Reduce {
                    step_id,
                    reducer,
                    is_complete,
                } => {
                    let step_id = step_id.clone();
                    let reducer = reducer.clone_ref(py);
                    let is_complete = is_complete.clone_ref(py);

                    let state_cache = state_caches.remove(&step_id).unwrap_or_default();

                    let (downstream, state_update_stream) = stream.map(extract_state_pair).reduce(
                        state_cache,
                        move |key, aggregator, value| reduce(&reducer, key, aggregator, value),
                        move |key, aggregator| check_complete(&is_complete, key, aggregator),
                        StateKey::route,
                    );

                    let state_update_stream =
                        state_update_stream.map(move |(key, state)| (step_id.clone(), key, state));
                    state_updates.push(state_update_stream);

                    stream = downstream.map(wrap_state_pair);
                }
                Step::ReduceEpoch { reducer } => {
                    let reducer = reducer.clone_ref(py);
                    stream = stream.map(extract_state_pair).aggregate(
                        move |key, value, aggregator: &mut Option<TdPyAny>| {
                            reduce_epoch(&reducer, aggregator, key, value);
                        },
                        move |key, aggregator: Option<TdPyAny>| {
                            // Aggregator will only exist for keys
                            // that exist, so it will have been filled
                            // into Some(value) above.
                            wrap_state_pair((key, aggregator.unwrap()))
                        },
                        StateKey::route,
                    );
                }
                Step::ReduceEpochLocal { reducer } => {
                    let reducer = reducer.clone_ref(py);
                    stream = stream
                        .map(extract_state_pair)
                        .accumulate(
                            HashMap::new(),
                            move |aggregators, all_key_value_in_epoch| {
                                reduce_epoch_local(&reducer, aggregators, &all_key_value_in_epoch);
                            },
                        )
                        .flat_map(|aggregators| aggregators.into_iter().map(wrap_state_pair));
                }
                Step::StatefulMap {
                    step_id,
                    builder,
                    mapper,
                } => {
                    let step_id = step_id.clone();
                    let builder = builder.clone_ref(py);
                    let mapper = mapper.clone_ref(py);

                    let state_cache = state_caches.remove(&step_id).unwrap_or_default();

                    let (downstream, state_update_stream) =
                        stream.map(extract_state_pair).stateful_map(
                            state_cache,
                            move |key| build(&builder, key),
                            move |key, state, value| stateful_map(&mapper, key, state, value),
                            StateKey::route,
                        );

                    let state_update_stream =
                        state_update_stream.map(move |(key, state)| (step_id.clone(), key, state));
                    state_updates.push(state_update_stream);

                    stream = downstream.map(wrap_state_pair);
                }
                Step::TumblingWindow {
                    step_id,
                    datetime_getter_fn,
                } => {
                    let time_getter_fn = datetime_getter_fn.clone_ref(py);
                    let state_cache = state_caches.remove(step_id).unwrap_or_default();
                    let window_source_stream = source(scope, "Source", |capability, info| {
                        let activator = scope.activator_for(&info.address[..]);

                        let mut cap = Some(capability);
                        move |output| {
                            if let Some(cap) = cap.as_mut() {
                                output.session(&cap).give(());
                                cap.downgrade(&(cap.time() + 1));
                                debug!("Setting activation");
                                activator.activate_after(Duration::new(1, 0));
                            }
                        }
                    });

                    // TODO: Handle state update stream
                    let (downstream, state_update_stream) =
                        stream.map(extract_state_pair).tumbling_window(
                            window_source_stream,
                            state_cache,
                            move |key, current_window, value| {
                                aggregate_window(key, current_window, value)
                            },
                            StateKey::route,
                        );

                    // let state_update_stream =
                    //     state_update_stream.map(move |(key, state)| (step_id.clone(), key, state));
                    // state_updates.push(state_update_stream);

                    stream = downstream.map(wrap_state_pair);
                }
                Step::Capture {} => {
                    let worker_output = worker_output.clone_ref(py);
                    let capture = stream
                        .inspect_time(move |epoch, item| capture(&worker_output, epoch, item))
                        .probe_with(&mut execution_frontier);

                    captures.push(capture.clone());

                    stream = capture;
                }
            }
        }

        if captures.len() < 1 {
            return Err("Dataflow needs to contain at least one capture".into());
        }

        let (dataflow_frontier_handle, dataflow_frontier_stream) =
            scope.feedback(Default::default());
        let (backup, gc) = scope.recovery(
            state_updates,
            dataflow_frontier_stream,
            wrapped_recovery_store.take(),
            recovery_store_log,
        );
        scope
            .dataflow_frontier(backup, captures)
            // No worker can GC until all workers have backed up and
            // output an epoch.
            .broadcast()
            .connect_loop(dataflow_frontier_handle);
        // Execution should not complete until GC is complete since we
        // record the dataflow frontier there.
        gc.probe_with(&mut execution_frontier);

        let pump = Pump::new(worker_input, timely_input);
        Ok((pump, execution_frontier))
    })
}

/// Advance to the supplied epoch.
///
/// When providing input to a Dataflow, work cannot complete until
/// there is no more data for a given epoch.
///
/// AdvanceTo is the signal to a Dataflow that the frontier has moved
/// beyond the current epoch, and that items with an epoch less than
/// the epoch in AdvanceTo can be worked to completion.
///
/// Using AdvanceTo and Emit is only necessary when using `spawn_cluster`
/// and `cluster_main()` as `run()` and `run_cluster()` will yield AdvanceTo
/// and Emit for you.
///
/// See also: `inputs.yield_epochs()`
///
/// >>> def input_builder(worker_index, worker_count):
/// ...     for i in range(10):
/// ...         yield AdvanceTo(i) # Advances the epoch to i
/// ...         yield Emit(i) # Adds the input i at epoch i
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

/// Emit the supplied item into the dataflow at the current epoch
///
/// Emit is how we introduce input into a dataflow:
///
/// >>> def input_builder(worker_index, worker_count):
/// ...     for i in range(10):
/// ...         yield AdvanceTo(i) # Advances the epoch to i
/// ...         yield Emit(i) # Adds the input i at epoch i
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
                            panic!("{}", format!("Input must be an instance of either `AdvanceTo` or `Emit`. Got: {item:?}. See https://docs.bytewax.io/apidocs#bytewax.AdvanceTo for more information."))
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
/// >>> def input_builder(worker_index, worker_count, resume_epoch):
/// ...     for epoch, item in enumerate(range(resume_epoch, 3)):
/// ...         yield AdvanceTo(epoch)
/// ...         yield Emit(item)
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
///     input_builder: Yields `AdvanceTo()` or `Emit()` with this
///         worker's input. Must resume from the epoch specified.
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
/// >>> def input_builder(worker_index, worker_count, resume_epoch):
/// ...     for epoch, item in enumerate(range(resume_epoch, 3)):
/// ...         yield AdvanceTo(epoch)
/// ...         yield Emit(item)
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
///     input_builder: Yields `AdvanceTo()` or `Emit()` with this
///         worker's input. Must resume from the epoch specified.
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
    m.add_function(wrap_pyfunction!(run_main, m)?)?;
    m.add_function(wrap_pyfunction!(cluster_main, m)?)?;
    m.add_class::<Emit>()?;
    m.add_class::<AdvanceTo>()?;
    Ok(())
}
