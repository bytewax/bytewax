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

pub(crate) struct WorkerRunner<'a, A: Allocate> {
    worker: &'a mut Worker<A>,
    index: WorkerIndex,
    count: WorkerCount,
    interrupt_flag: &'a AtomicBool,
    flow: Py<Dataflow>,
    epoch_interval: EpochInterval,
    recovery_config: Py<RecoveryConfig>,
}

impl<'a, A: Allocate> WorkerRunner<'a, A> {
    pub(crate) fn new(
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
