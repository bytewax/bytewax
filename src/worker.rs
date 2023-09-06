//! Definition of a Bytewax worker.

use std::cell::RefCell;
use std::rc::Rc;
use std::time::Duration;

use opentelemetry::global;
use opentelemetry::KeyValue;
use pyo3::exceptions::PyTypeError;
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use timely::communication::Allocate;
use timely::dataflow::operators::generic::operator::empty;
use timely::dataflow::operators::Broadcast;
use timely::dataflow::operators::Concatenate;
use timely::dataflow::operators::Exchange;
use timely::dataflow::operators::Filter;
use timely::dataflow::operators::Inspect;
use timely::dataflow::operators::Map;
use timely::dataflow::operators::Probe;
use timely::dataflow::operators::ResultStream;
use timely::dataflow::ProbeHandle;
use timely::progress::Timestamp;
use timely::worker::Worker as TimelyWorker;
use tracing::instrument;

use crate::dataflow::Dataflow;
use crate::dataflow::Step;
use crate::errors::tracked_err;
use crate::errors::PythonException;
use crate::inputs::*;
use crate::operators::collect_window::CollectWindowLogic;
use crate::operators::fold_window::FoldWindowLogic;
use crate::operators::reduce::ReduceLogic;
use crate::operators::reduce_window::ReduceWindowLogic;
use crate::operators::stateful_map::StatefulMapLogic;
use crate::operators::stateful_unary::StatefulUnary;
use crate::operators::*;
use crate::outputs::*;
use crate::pyo3_extensions::extract_state_pair;
use crate::pyo3_extensions::wrap_state_pair;
use crate::recovery::*;
use crate::timely::AsWorkerExt;
use crate::window::clock::ClockBuilder;
use crate::window::StatefulWindowUnary;
use crate::window::WindowBuilder;
use crate::with_timer;

/// Bytewax worker.
///
/// Wraps a [`TimelyWorker`].
struct Worker<'a, A, F>
where
    A: Allocate,
    F: Fn() -> bool,
{
    worker: &'a mut TimelyWorker<A>,
    /// This is a function that should return `true` only when the
    /// dataflow should perform an abrupt shutdown.
    interrupt_callback: F,
}

impl<'a, A, F> Worker<'a, A, F>
where
    A: Allocate,
    F: Fn() -> bool,
{
    fn new(worker: &'a mut TimelyWorker<A>, interrupt_callback: F) -> Self {
        Self {
            worker,
            interrupt_callback,
        }
    }

    /// Run a specific dataflow until it is complete.
    ///
    /// [`ProbeHandle`]s are how we ID a dataflow.
    fn run<T>(&mut self, probe: ProbeHandle<T>)
    where
        T: Timestamp,
    {
        tracing::info!("Timely dataflow start");
        let cooldown = Duration::from_millis(1);
        while !(self.interrupt_callback)() && !probe.done() {
            tracing::debug_span!("step").in_scope(|| {
                self.worker.step_or_park(Some(cooldown));
            });
        }
        tracing::info!("Timely dataflow stop");
    }

    /// Terminate all dataflows in this worker.
    ///
    /// We need this because otherwise all of Timely's entry points
    /// (e.g. [`timely::execute::execute_from`]) wait until all work
    /// is complete and we will hang if we are shutting down due to
    /// error.
    fn shutdown(&mut self) {
        for dataflow_id in self.worker.installed_dataflows() {
            self.worker.drop_dataflow(dataflow_id);
        }
    }
}

/// Public, main entry point for a worker thread.
#[instrument(name = "worker_main", skip_all, fields(worker = worker.index()))]
pub(crate) fn worker_main<A>(
    worker: &mut TimelyWorker<A>,
    interrupt_callback: impl Fn() -> bool,
    flow: Py<Dataflow>,
    epoch_interval: EpochInterval,
    recovery_config: Option<Py<RecoveryConfig>>,
) -> PyResult<()>
where
    A: Allocate,
{
    let mut worker = Worker::new(worker, interrupt_callback);
    tracing::info!("Worker start");

    let recovery = recovery_config
        .map(|config| Python::with_gil(|py| config.borrow(py).build(py)))
        .transpose()?;

    let resume_from = recovery
        .as_ref()
        .map(|(bundle, _backup_interval)| -> PyResult<ResumeFrom> {
            let resume_calc = Python::with_gil(|py| Rc::new(RefCell::new(ResumeCalc::new(py))));
            let resume_calc_d = resume_calc.clone();
            let probe = Python::with_gil(|py| {
                build_resume_calc_dataflow(py, worker.worker, bundle.clone_ref(py), resume_calc_d)
                    .reraise("error building progress load dataflow")
            })?;

            tracing::info_span!("resume_calc_dataflow").in_scope(|| {
                worker.run(probe);
            });

            let resume_from = resume_calc.borrow().resume_from()?;
            tracing::info!("Calculated {resume_from:?}");

            Ok(resume_from)
        })
        .transpose()?
        .unwrap_or_default();

    let probe = Python::with_gil(|py| {
        build_production_dataflow(
            py,
            worker.worker,
            flow,
            epoch_interval,
            resume_from,
            recovery,
        )
        .reraise("error building production dataflow")
    })?;

    tracing::info_span!("production_dataflow").in_scope(|| {
        worker.run(probe);
    });

    worker.shutdown();
    tracing::info!("Worker stop");
    Ok(())
}

/// Compile a dataflow which loads the progress data from the previous
/// execution.
///
/// Read state out of the cell once the probe is done. Calculation of
/// [`ResumeFrom`] is deterministic and so all workers will have
/// converged to the same value.
fn build_resume_calc_dataflow<A>(
    _py: Python,
    worker: &mut TimelyWorker<A>,
    bundle: RecoveryBundle,
    resume_calc: Rc<RefCell<ResumeCalc>>,
) -> PyResult<ProbeHandle<u64>>
where
    A: Allocate,
{
    worker.dataflow(|scope| {
        let mut probe = ProbeHandle::new();

        let (parts, exs, fronts, commits) = bundle.read_progress(scope);
        scope
            .resume_from(
                &parts.broadcast(),
                &exs.broadcast(),
                &fronts.broadcast(),
                &commits.broadcast(),
                resume_calc,
            )
            .probe_with(&mut probe);

        Ok(probe)
    })
}

/// Turn a Bytewax dataflow into a Timely dataflow.
fn build_production_dataflow<A>(
    py: Python,
    worker: &mut TimelyWorker<A>,
    flow: Py<Dataflow>,
    epoch_interval: EpochInterval,
    resume_from: ResumeFrom,
    recovery: Option<(RecoveryBundle, BackupInterval)>,
) -> PyResult<ProbeHandle<u64>>
where
    A: Allocate,
{
    let this_worker = worker.w_index();
    let this_worker_label = KeyValue::new("worker_id", this_worker.0.to_string());

    // Remember! Never build different numbers of Timely operators on
    // different workers! Timely does not like that and you'll see a
    // mysterious `failed to correctly cast channel` panic. You must
    // build asymmetry within each operator.
    worker.dataflow(|scope| {
        let flow = flow.as_ref(py).borrow();

        let mut probe = ProbeHandle::new();

        let mut inputs = Vec::new();
        let mut outputs = Vec::new();
        let mut snaps = Vec::new();

        let ResumeFrom(_ex, resume_epoch) = resume_from;

        let loads = if let Some((bundle, _backup_interval)) = &recovery {
            scope.load_snaps(resume_epoch, bundle.clone_ref(py))
        } else {
            // Load nothing from a previous execution.
            empty(scope)
        };

        // Start with an empty stream. We might overwrite it with
        // input later.
        let mut stream = empty(scope);

        // Top level metrics meter
        let meter = global::meter("dataflow");

        for step in &flow.steps {
            // All these closure lifetimes are static, so tell
            // Python's GC that there's another pointer to the
            // mapping function that's going to hang around
            // for a while when it's moved into the closure.
            let step = step.clone();
            match step {
                // The exchange operator wraps the number to a modulo of workers_count,
                // so we can pass any valid u64 without specifying the range.
                Step::Redistribute => stream = stream.exchange(move |_| fastrand::u64(..)),
                Step::CollectWindow {
                    step_id,
                    clock_config,
                    window_config,
                } => {
                    let clock_builder = clock_config
                        .build(py)
                        .reraise("error building CollectWindow clock")?;
                    let windower_builder = window_config
                        .build(py)
                        .reraise("error building CollectWindow windower")?;

                    let (output, snap) = stream.map(extract_state_pair).stateful_window_unary(
                        step_id,
                        clock_builder,
                        windower_builder,
                        CollectWindowLogic::builder(),
                        resume_epoch,
                        &loads,
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
                    snaps.push(snap);
                }
                Step::Input { step_id, input } => {
                    if let Ok(input) = input.extract::<PartitionedInput>(py) {
                        let (output, snap) = input
                            .partitioned_input(
                                py,
                                scope,
                                step_id,
                                epoch_interval,
                                &probe,
                                resume_epoch,
                                &loads,
                            )
                            .reraise("error building PartitionedInput")?;

                        inputs.push(output.clone());
                        stream = output;
                        snaps.push(snap);
                    } else if let Ok(input) = input.extract::<DynamicInput>(py) {
                        let output = input
                            .dynamic_input(py, scope, step_id, epoch_interval, &probe, resume_epoch)
                            .reraise("error building DynamicInput")?;

                        inputs.push(output.clone());
                        stream = output;
                    } else {
                        return Err(tracked_err::<PyTypeError>("unknown input type"));
                    }
                }
                Step::Map { step_id, mapper } => {
                    let histogram = meter
                        .f64_histogram("map.mapper.duration")
                        .with_description("map step duration in seconds")
                        .init();
                    let labels = vec![
                        KeyValue::new("step_id", step_id.0),
                        this_worker_label.clone(),
                    ];
                    stream =
                        stream.map(move |item| with_timer!(histogram, labels, map(&mapper, item)));
                }
                Step::FlatMap { step_id, mapper } => {
                    let histogram = meter
                        .f64_histogram("flat_map.mapper.duration")
                        .with_description("flat_map duration in seconds")
                        .init();
                    let labels = vec![
                        KeyValue::new("step_id", step_id.0),
                        this_worker_label.clone(),
                    ];
                    stream = stream.flat_map(move |item| {
                        with_timer!(histogram, labels, flat_map(&mapper, item))
                    });
                }
                Step::Filter { step_id, predicate } => {
                    let histogram = meter
                        .f64_histogram("filter.predicate.duration")
                        .with_description("filter predicate duration in seconds")
                        .init();
                    let labels = vec![
                        KeyValue::new("step_id", step_id.0),
                        this_worker_label.clone(),
                    ];
                    stream = stream.filter(move |item| {
                        with_timer!(histogram, labels, filter(&predicate, item))
                    });
                }
                Step::FilterMap { step_id, mapper } => {
                    let histogram = meter
                        .f64_histogram("filter_map.mapper.duration")
                        .with_description("filter_map mapper duration in seconds")
                        .init();
                    let labels = vec![
                        KeyValue::new("step_id", step_id.0),
                        this_worker_label.clone(),
                    ];
                    stream = stream
                        .map(move |item| with_timer!(histogram, labels, map(&mapper, item)))
                        .filter(move |item| Python::with_gil(|py| !item.is_none(py)));
                }
                Step::FoldWindow {
                    step_id,
                    clock_config,
                    window_config,
                    builder,
                    folder,
                } => {
                    let clock_builder = clock_config
                        .build(py)
                        .reraise("error building FoldWindow clock")?;
                    let windower_builder = window_config
                        .build(py)
                        .reraise("error building FoldWindow windower")?;

                    let (output, snap) = stream.map(extract_state_pair).stateful_window_unary(
                        step_id,
                        clock_builder,
                        windower_builder,
                        FoldWindowLogic::new(builder, folder),
                        resume_epoch,
                        &loads,
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
                    snaps.push(snap);
                }
                Step::Inspect { inspector } => {
                    stream = stream.inspect(move |item| inspect(&inspector, item));
                }
                Step::InspectWorker { inspector } => {
                    stream =
                        stream.inspect(move |item| inspect_worker(&inspector, &this_worker, item));
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
                    let (output, snap) = stream.map(extract_state_pair).stateful_unary(
                        step_id,
                        ReduceLogic::builder(reducer, is_complete),
                        resume_epoch,
                        &loads,
                    );
                    stream = output.map(wrap_state_pair);
                    snaps.push(snap);
                }
                Step::ReduceWindow {
                    step_id,
                    clock_config,
                    window_config,
                    reducer,
                } => {
                    let clock_builder = clock_config
                        .build(py)
                        .reraise("error building ReduceWindow clock")?;
                    let windower_builder = window_config
                        .build(py)
                        .reraise("error building ReduceWindow windower")?;

                    let (output, snap) = stream.map(extract_state_pair).stateful_window_unary(
                        step_id,
                        clock_builder,
                        windower_builder,
                        ReduceWindowLogic::builder(reducer),
                        resume_epoch,
                        &loads,
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
                    snaps.push(snap);
                }
                Step::StatefulMap {
                    step_id,
                    builder,
                    mapper,
                } => {
                    let (output, snap) = stream.map(extract_state_pair).stateful_unary(
                        step_id,
                        StatefulMapLogic::builder(builder, mapper),
                        resume_epoch,
                        &loads,
                    );
                    stream = output.map(wrap_state_pair);
                    snaps.push(snap);
                }
                Step::Output { step_id, output } => {
                    if let Ok(output) = output.extract(py) {
                        let (output, snap) = stream
                            .partitioned_output(py, step_id, output, &loads)
                            .reraise("error building PartitionedOutput")?;
                        let clock = output.map(|_| ());

                        outputs.push(clock.clone());
                        snaps.push(snap);
                        stream = output;
                    } else if let Ok(output) = output.extract(py) {
                        let output = stream
                            .dynamic_output(py, step_id, output)
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

        // Attach the probe to the relevant final output.
        if let Some((bundle, backup_interval)) = recovery {
            scope
                .concatenate(snaps)
                .write_recovery(resume_from, bundle, epoch_interval, backup_interval)
                .probe_with(&mut probe);
        } else {
            scope.concatenate(outputs).probe_with(&mut probe);
        }

        Ok(probe)
    })
}
