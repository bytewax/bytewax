use std::cell::RefCell;
use std::rc::Rc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use pyo3::exceptions::{PyTypeError, PyValueError};
use pyo3::{IntoPy, Py, PyAny, PyResult, Python};
use serde::{Deserialize, Serialize};
use timely::communication::Allocate;
use timely::dataflow::{
    operators::{Concatenate, Filter, Inspect, Map, Probe, ResultStream, ToStream},
    ProbeHandle,
};
use timely::progress::Timestamp;
use timely::worker::Worker as TimelyWorker;
use tracing::span::EnteredSpan;

use crate::dataflow::{Dataflow, Step};
use crate::errors::{tracked_err, PythonException};
use crate::inputs::{DynamicInput, EpochInterval, PartitionedInput};
use crate::operators::{
    collect_window::CollectWindowLogic, filter, flat_map, fold_window::FoldWindowLogic, inspect,
    inspect_epoch, map, reduce::ReduceLogic, reduce_window::ReduceWindowLogic,
    stateful_map::StatefulMapLogic, stateful_unary::StatefulUnary,
};
use crate::outputs::{DynamicOutputOp, PartitionedOutputOp};
use crate::pyo3_extensions::{extract_state_pair, wrap_state_pair};
use crate::recovery::dataflows::attach_recovery_to_dataflow;
use crate::recovery::model::{
    Change, FlowStateBytes, KChange, KWriter, ProgressMsg, ProgressReader, ProgressWriter,
    ResumeEpoch, ResumeFrom, StateReader, StateWriter, StoreKey, WorkerKey,
};
use crate::recovery::operators::{read, BroadcastWrite, Recover, Summary, Write};
use crate::recovery::python::{RecoveryBuilder, RecoveryConfig};
use crate::recovery::store::in_mem::{InMemProgress, InMemStore, StoreSummary};
use crate::window::{clock::ClockBuilder, StatefulWindowUnary, WindowBuilder};

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

pub(crate) struct Worker<'a, A: Allocate> {
    worker: &'a mut TimelyWorker<A>,
    index: WorkerIndex,
    count: WorkerCount,
    interrupt_flag: &'a AtomicBool,
    flow: Py<Dataflow>,
    epoch_interval: EpochInterval,
    recovery_config: Py<RecoveryConfig>,
}

impl<'a, A: Allocate> Worker<'a, A> {
    pub(crate) fn new(
        worker: &'a mut TimelyWorker<A>,
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

    pub(crate) fn run(mut self) -> PyResult<()> {
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
        let cooldown = Some(Duration::from_millis(1));
        let mut span = PeriodicSpan::new(Duration::from_secs(10));
        while !self.interrupt_flag.load(Ordering::Relaxed) && !probe.done() {
            self.worker.step_or_park(cooldown);
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

/// Turn the abstract blueprint for a dataflow into a Timely dataflow
/// so it can be executed.
///
/// This is more complicated than a 1:1 translation of Bytewax
/// concepts to Timely, as we are using Timely as a basis to implement
/// more-complicated Bytewax features like input builders and
/// recovery.
#[allow(clippy::too_many_arguments)]
pub(crate) fn build_production_dataflow<A, PW, SW>(
    py: Python,
    worker: &mut TimelyWorker<A>,
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
