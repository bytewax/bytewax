//! Internal code for dataflow execution.
//!
//! For a user-centric version of how to execute dataflows, read the
//! the `bytewax.execution` Python module docstring. Read that first.
//!
//! [`worker_main()`] for the root of all the internal action here.

use crate::dataflow::{Dataflow, Step};
use crate::inputs::pump_from_config;
use crate::inputs::InputConfig;
use crate::inputs::Pump;
use crate::operators::CollectGarbage;
use crate::operators::*;
use crate::pyo3_extensions::{extract_state_pair, wrap_state_pair, StateKey};
use crate::pyo3_extensions::{TdPyAny, TdPyCallable};
use crate::recovery::RecoveryConfig;
use crate::recovery::RecoveryKey;
use crate::recovery::StateBackup;
use crate::recovery::StateReader;
use crate::recovery::StateWriter;
use crate::recovery::{build_recovery_readers, build_recovery_writers, FrontierUpdate};
use crate::recovery::{default_recovery_config, ProgressWriter};
use crate::recovery::{ProgressReader, StateCollector};
use log::debug;
use pyo3::exceptions::{PyRuntimeError, PyValueError};
use pyo3::prelude::*;
use std::collections::HashMap;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::thread;
use std::time::Duration;
use timely::communication::Allocate;
use timely::dataflow::operators::aggregation::*;
use timely::dataflow::operators::*;
use timely::dataflow::InputHandle;
use timely::dataflow::ProbeHandle;
use timely::progress::Antichain;
use timely::progress::Timestamp;
use timely::worker::Worker;

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
    resume_epoch: u64,
    flow: Py<Dataflow>,
    input_config: Py<InputConfig>,
    output_builder: TdPyCallable,
    state_reader: Box<dyn StateReader<u64, TdPyAny>>,
    progress_writer: Box<dyn ProgressWriter<u64>>,
    state_writer: Box<dyn StateWriter<u64, TdPyAny>>,
    state_collector: Box<dyn StateCollector<u64>>,
) -> Result<(Box<dyn Pump>, ProbeHandle<u64>), String> {
    let worker_index = worker.index();
    let worker_count = worker.peers();

    worker.dataflow(|scope| {
        let mut root_input = InputHandle::new();
        let mut probe = ProbeHandle::new();

        let state_loading_stream = state_source(scope, state_reader, resume_epoch, &probe);
        let mut stream = scope.input_from(&mut root_input);
        root_input.advance_to(resume_epoch);

        let mut state_backup_streams = Vec::new();
        let mut capture_streams = Vec::new();

        let flow = flow.as_ref(py).borrow();
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
                    let filter_step_id = step_id.clone();
                    let state_loading_stream = state_loading_stream.filter(
                        move |StateBackup(
                            RecoveryKey(loading_step_id, _key, _epoch),
                            _state_update,
                        )| { &filter_step_id == loading_step_id },
                    );

                    let (downstream, state_backup_stream) =
                        // Reduce is implemented using stateful map so
                        // we only have to implement state interacting
                        // with recovery once.
                        stream.map(extract_state_pair).stateful_map(
                            step_id,
                            state_loading_stream,
                            move |_key| build_none(),
                            move |key, acc, value| {
                                reduce(&reducer, &is_complete, key, acc, value)
                            },
                            StateKey::route,
                        );

                    state_backup_streams.push(state_backup_stream);
                    stream = downstream.map(wrap_state_pair);
                }
                Step::ReduceEpoch { reducer } => {
                    stream = stream.map(extract_state_pair).aggregate(
                        move |key, value, acc: &mut Option<TdPyAny>| {
                            reduce_epoch(&reducer, acc, key, value);
                        },
                        move |key, acc: Option<TdPyAny>| {
                            // Accumulator will only exist for keys
                            // that exist, so it will have been filled
                            // into Some(value) above.
                            wrap_state_pair((key, acc.unwrap()))
                        },
                        StateKey::route,
                    );
                }
                Step::ReduceEpochLocal { reducer } => {
                    stream = stream
                        .map(extract_state_pair)
                        .accumulate(HashMap::new(), move |accs, all_key_value_in_epoch| {
                            reduce_epoch_local(&reducer, accs, &all_key_value_in_epoch);
                        })
                        .flat_map(|accs| accs.into_iter().map(wrap_state_pair));
                }
                Step::StatefulMap {
                    step_id,
                    builder,
                    mapper,
                } => {
                    let filter_step_id = step_id.clone();
                    let state_loading_stream = state_loading_stream.filter(
                        move |StateBackup(
                            RecoveryKey(loading_step_id, _key, _epoch),
                            _state_update,
                        )| { &filter_step_id == loading_step_id },
                    );

                    let (downstream, state_backup_stream) =
                        stream.map(extract_state_pair).stateful_map(
                            step_id,
                            state_loading_stream,
                            move |key| build(&builder, key),
                            move |key, state, value| stateful_map(&mapper, key, state, value),
                            StateKey::route,
                        );

                    state_backup_streams.push(state_backup_stream);
                    stream = downstream.map(wrap_state_pair);
                }
                Step::Capture {} => {
                    let capture = stream;

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

        let worker_output: TdPyCallable = output_builder
            .call1(py, (worker_index, worker_count))
            .unwrap()
            .extract(py)
            .unwrap();

        let capture_stream = scope
            .concatenate(capture_streams)
            .inspect_time(move |epoch, item| capture(&worker_output, epoch, item));

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

        let pump = pump_from_config(
            py,
            input_config,
            root_input,
            worker_index,
            worker_count,
            resume_epoch,
        );
        Ok((pump, probe))
    })
}

/// Run a dataflow which uses a [`Pump`] until complete.
fn pump_until_done<A: Allocate>(
    worker: &mut Worker<A>,
    interrupt_flag: &AtomicBool,
    mut pumps_with_input_remaining: Vec<Box<dyn Pump>>,
    probe: ProbeHandle<u64>,
) {
    while !interrupt_flag.load(Ordering::Relaxed) && !probe.done() {
        if !pumps_with_input_remaining.is_empty() {
            // We have input remaining, pump the pumps, and step the workers while
            // there is work less than the current epoch to do.
            let mut updated_pumps = Vec::new();
            for mut pump in pumps_with_input_remaining.into_iter() {
                pump.pump();
                worker.step_while(|| probe.less_than(&pump.input_time()));
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
    input_config: Py<InputConfig>,
    output_builder: TdPyCallable,
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

    let (pump, probe) = Python::with_gil(|py| {
        build_production_dataflow(
            py,
            worker,
            resume_epoch,
            flow,
            input_config,
            output_builder,
            state_reader,
            progress_writer,
            state_writer,
            state_collector,
        )
    })?;

    pump_until_done(worker, &interrupt_flag, vec![pump], probe);

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
    input_config: Py<InputConfig>,
    output_builder: TdPyCallable,
    recovery_config: Option<Py<RecoveryConfig>>,
) -> Result<(), String> {
    let recovery_config = recovery_config.unwrap_or_else(default_recovery_config);

    let resume_epoch =
        build_and_run_resume_epoch_calc_dataflow(worker, interrupt_flag, recovery_config.clone())?;

    build_and_run_production_dataflow(
        worker,
        interrupt_flag,
        flow,
        input_config,
        output_builder,
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
///     input_config: Input config of type Manual or Kafka. See `bytewax.inputs`.
///
///     output_builder: Returns a callback function for each worker
///         thread, called with `(epoch, item)` whenever and item
///         passes by a capture operator on this process.
///
///     recovery_config: State recovery config. See
///         `bytewax.recovery`. If `None`, state will not be
///         persisted.
///
#[pyfunction(flow, input_config, output_builder, "*", recovery_config = "None")]
#[pyo3(text_signature = "(flow, input_config, output_builder, *, recovery_config)")]
pub(crate) fn run_main(
    py: Python,
    flow: Py<Dataflow>,
    input_config: Py<InputConfig>,
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
                let interrupt_flag = AtomicBool::new(false);

                worker_main(
                    worker,
                    &interrupt_flag,
                    flow,
                    input_config,
                    output_builder,
                    recovery_config,
                )
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
///     input_config: Input config of type Manual or Kafka. See `bytewax.inputs`.
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
///         `bytewax.recovery`. If `None`, state will not be
///         persisted.
///
///     worker_count_per_proc: Number of worker threads to start on
///         each process.
#[pyfunction(
    flow,
    input_config,
    output_builder,
    addresses,
    proc_id,
    "*",
    recovery_config = "None",
    worker_count_per_proc = "1"
)]
#[pyo3(
    text_signature = "(flow, input_config, output_builder, addresses, proc_id, *, recovery_config, worker_count_per_proc)"
)]
pub(crate) fn cluster_main(
    py: Python,
    flow: Py<Dataflow>,
    input_config: Py<InputConfig>,
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
                worker_main(
                    worker,
                    &should_shutdown_w,
                    flow.clone(),
                    input_config.clone(),
                    output_builder.clone(),
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
    m.add_function(wrap_pyfunction!(run_main, m)?)?;
    m.add_function(wrap_pyfunction!(cluster_main, m)?)?;
    Ok(())
}
