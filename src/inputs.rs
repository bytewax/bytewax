//! Internal code for input.
//!
//! For a user-centric version of input, read the `bytewax.inputs`
//! Python module docstring. Read that first.

use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::sync::atomic;
use std::sync::atomic::AtomicBool;
use std::sync::Arc;

use chrono::DateTime;
use chrono::TimeDelta;
use chrono::Utc;
use opentelemetry::KeyValue;
use pyo3::create_exception;
use pyo3::exceptions::PyRuntimeError;
use pyo3::exceptions::PyStopIteration;
use pyo3::exceptions::PyTypeError;
use pyo3::intern;
use pyo3::prelude::*;
use timely::dataflow::channels::pact::Pipeline;
use timely::dataflow::operators::generic::builder_rc::OperatorBuilder;
use timely::dataflow::operators::Capability;
use timely::dataflow::ProbeHandle;
use timely::dataflow::Scope;
use timely::dataflow::Stream;
use timely::progress::Timestamp;

use crate::errors::tracked_err;
use crate::errors::PythonException;
use crate::pyo3_extensions::TdPyAny;
use crate::recovery::*;
use crate::timely::*;
use crate::unwrap_any;
use crate::with_timer;

const DEFAULT_COOLDOWN: TimeDelta = TimeDelta::microseconds(1000);

/// Length of epoch.
///
/// Epoch boundaries represent synchronization points between all
/// workers and are when state snapshots are taken and written for
/// recovery.
///
/// The epoch is also used as backpressure within the dataflow; input
/// sources do not start reading new data until all data in the
/// previous epoch has been output and recovery data written.
#[derive(Debug, Copy, Clone)]
pub(crate) struct EpochInterval(TimeDelta);

impl<'py> FromPyObject<'py> for EpochInterval {
    fn extract_bound(ob: &Bound<'py, PyAny>) -> PyResult<Self> {
        if let Ok(duration) = ob.extract::<TimeDelta>() {
            Ok(Self(duration))
        } else {
            Err(PyTypeError::new_err(
                "epoch interval must be a `datetime.timedelta`",
            ))
        }
    }
}

impl Default for EpochInterval {
    fn default() -> Self {
        Self(TimeDelta::try_seconds(10).unwrap())
    }
}

create_exception!(
    bytewax.inputs,
    AbortExecution,
    PyRuntimeError,
    "Raise this from `next_batch` to abort for testing purposes."
);

/// Represents a `bytewax.inputs.Source` from Python.
#[derive(Clone)]
pub(crate) struct Source(Py<PyAny>);

/// Do some eager type checking.
impl<'py> FromPyObject<'py> for Source {
    fn extract_bound(ob: &Bound<'py, PyAny>) -> PyResult<Self> {
        let py = ob.py();
        let abc = py.import_bound("bytewax.inputs")?.getattr("Source")?;
        if !ob.is_instance(&abc)? {
            Err(PyTypeError::new_err(
                "source must subclass `bytewax.inputs.Source`",
            ))
        } else {
            Ok(Self(ob.to_object(py)))
        }
    }
}

impl IntoPy<Py<PyAny>> for Source {
    fn into_py(self, _py: Python<'_>) -> Py<PyAny> {
        self.0
    }
}

impl ToPyObject for Source {
    fn to_object(&self, py: Python<'_>) -> PyObject {
        self.0.to_object(py)
    }
}

impl Source {
    pub(crate) fn extract<'p, D>(&'p self, py: Python<'p>) -> PyResult<D>
    where
        D: FromPyObject<'p>,
    {
        self.0.extract(py)
    }
}

/// Represents a `bytewax.inputs.FixedPartitionedSource` from Python.
#[derive(Clone)]
pub(crate) struct FixedPartitionedSource(Py<PyAny>);

/// Do some eager type checking.
impl<'py> FromPyObject<'py> for FixedPartitionedSource {
    fn extract_bound(ob: &Bound<'py, PyAny>) -> PyResult<Self> {
        let py = ob.py();
        let abc = py
            .import_bound("bytewax.inputs")?
            .getattr("FixedPartitionedSource")?;
        if !ob.is_instance(&abc)? {
            Err(PyTypeError::new_err(
                "fixed partitioned source must subclass `bytewax.inputs.FixedPartitionedSource`",
            ))
        } else {
            Ok(Self(ob.to_object(py)))
        }
    }
}

fn default_next_awake(
    res: Option<DateTime<Utc>>,
    batch_len: usize,
    now: DateTime<Utc>,
) -> Option<DateTime<Utc>> {
    if let Some(next_awake) = res {
        // If the source returned an explicit next awake time, always
        // oblige.
        Some(next_awake)
    } else {
        // If `next_awake` returned `None`, then do the default
        // behavior:
        if batch_len > 0 {
            // Re-awaken immediately if there were items in the batch.
            None
        } else {
            // Wait a cooldown before re-awakening if there were no
            // items in the batch.
            Some(now + DEFAULT_COOLDOWN)
        }
    }
}

pub(crate) struct PartitionedPartState {
    downstream_cap: Capability<u64>,
    snap_cap: Capability<u64>,
    epoch_started: DateTime<Utc>,
    next_awake: Option<DateTime<Utc>>,
}

impl PartitionedPartState {
    pub(crate) fn awake_due(&self, now: DateTime<Utc>) -> bool {
        match self.next_awake {
            None => true,
            Some(next_awake) => now >= next_awake,
        }
    }
}

impl FixedPartitionedSource {
    pub(crate) fn list_parts(&self, py: Python) -> PyResult<Vec<StateKey>> {
        self.0.call_method0(py, "list_parts")?.extract(py)
    }

    pub(crate) fn build_part(
        &self,
        py: Python,
        step_id: &StepId,
        for_part: &StateKey,
        resume_state: Option<PyObject>,
    ) -> PyResult<StatefulPartition> {
        self.0
            .call_method1(
                py,
                "build_part",
                (step_id.clone(), for_part.clone(), resume_state),
            )?
            .extract(py)
    }

    /// Read items from a partitioned input.
    ///
    /// Will manage assigning primary workers for all partitions and
    /// building them.
    ///
    /// This can't be unified into the recovery system input operators
    /// because they are stateless and have no epoch semantics.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn partitioned_input<S>(
        self,
        py: Python,
        scope: &mut S,
        step_id: StepId,
        epoch_interval: EpochInterval,
        probe: &ProbeHandle<u64>,
        abort: &Arc<AtomicBool>,
        mut state: InputState,
    ) -> PyResult<(Stream<S, TdPyAny>, Stream<S, SerializedSnapshot>)>
    where
        S: Scope<Timestamp = u64>,
    {
        let recovery_on = state.recovery_on();
        let immediate_snapshot = state.immediate_snapshot();
        let start_at = state.start_at();
        let this_worker = scope.w_index();

        let local_parts = self.list_parts(py).reraise_with(|| {
            format!("error calling `FixedPartitionSource.list_parts` in step {step_id}")
        })?;
        let all_parts = local_parts.into_broadcast(scope, S::Timestamp::minimum());
        let updates = all_parts.assign_primaries(format!("{step_id}.assign_primaries"));

        let op_name = format!("{step_id}.partitioned_input");
        let mut op_builder = OperatorBuilder::new(op_name.clone(), scope.clone());

        let mut input = op_builder.new_input(&updates, Pipeline);

        let (mut downstream_output, downstream) = op_builder.new_output();
        let (mut snaps_output, snaps) = op_builder.new_output();

        let info = op_builder.operator_info();
        let activator = scope.activator_for(&info.address[..]);
        let probe = probe.clone();
        let abort = abort.clone();

        let meter = opentelemetry::global::meter("bytewax");
        let item_out_count = meter
            .u64_counter("item_out_count")
            .with_description("number of items this step has emitted")
            .init();
        let next_batch_histogram = meter
            .f64_histogram("inp_part_next_batch_duration_seconds")
            .with_description("`next_batch` duration in seconds")
            .init();
        let batch_size_histogram = meter
            .u64_histogram("inp_part_next_batch_size")
            .with_description("`next_batch` batch size")
            .init();
        let snapshot_histogram = meter
            .f64_histogram("snapshot_duration_seconds")
            .with_description("`snapshot` duration in seconds")
            .init();
        let labels = vec![
            KeyValue::new("step_id", step_id.0.to_string()),
            KeyValue::new("worker_index", this_worker.0.to_string()),
        ];

        op_builder.build(move |mut caps| {
            caps.downgrade_all(&start_at.0);
            let snap_cap = caps.pop().unwrap();
            let downstream_cap = caps.pop().unwrap();
            let mut caps = Some((downstream_cap, snap_cap));

            let mut inbuf = InBuffer::new();
            let mut parts: BTreeMap<StateKey, PartitionedPartState> = BTreeMap::new();

            move |input_frontiers| {
                let _guard = tracing::debug_span!("operator", operator = op_name).entered();
                let now = Utc::now();

                input.for_each(|cap, incoming| {
                    let epoch = cap.time();
                    inbuf.extend(*epoch, incoming);
                });

                // Apply this worker's primary assignments in epoch order. We don't need a
                // notificator here because we don't need any capability management.
                let frontier = &input_frontiers[0];
                let closed_epochs: Vec<_> =
                    inbuf.epochs().filter(|e| frontier.is_closed(e)).collect();

                // Wait for primaries to be assigned (our only input, so check if frontier is eof)
                // Once that's done, populate local state as needed, and only then move forward
                // with the dataflow.
                if frontier.is_eof() {
                    if let Some((downstream_cap, snap_cap)) = caps.take() {
                        let epoch = downstream_cap.time();
                        let mut primary_parts: BTreeSet<StateKey> =
                            state.keys().drain(..).collect();

                        for epoch in closed_epochs {
                            if let Some(parts) = inbuf.remove(&epoch) {
                                for (part_key, worker) in parts {
                                    if worker == this_worker {
                                        primary_parts.insert(part_key);
                                    }
                                }
                            }
                        }

                        Python::with_gil(|py| {
                            for part_key in primary_parts {
                                if !state.contains_key(&part_key) {
                                    tracing::info!("Init-ing {part_key:?} at epoch {epoch:?}");
                                    let part = self
                                        .build_part(py, &step_id, &part_key, None)
                                        .reraise_with(|| {
                                            format!(
                                                "error calling `FixedPartitionSource.build_part` \
                                                in step {step_id} for partition {part_key}"
                                            )
                                        })
                                        .unwrap();
                                    state.insert(part_key.clone(), part);
                                }
                                let next_awake = state.next_awake(py, &part_key).unwrap();
                                let part_state = PartitionedPartState {
                                    downstream_cap: downstream_cap.clone(),
                                    snap_cap: snap_cap.clone(),
                                    epoch_started: now,
                                    next_awake,
                                };
                                parts.insert(part_key.clone(), part_state);
                            }
                        });
                    }
                } else {
                    // Wait for primaries to be assigned before moving forward.
                    return;
                }

                let mut eofd_parts_buffer = Vec::new();
                let mut downstream_handle = downstream_output.activate();
                let mut snaps_handle = snaps_output.activate();
                let mut snaps = vec![];

                for part_key in state.keys() {
                    let _guard = tracing::trace_span!("partition", part_key = ?part_key).entered();
                    let part = parts.get_mut(&part_key).unwrap();
                    assert!(*part.downstream_cap.time() == *part.snap_cap.time());
                    let epoch = *part.downstream_cap.time();
                    // When we increment the epoch for this partition, wait until all
                    // ouputs have finished the previous epoch before emitting more
                    // data to have backpressure.
                    if !probe.less_than(&epoch) {
                        let mut eof = false;
                        // Separately check whether we should
                        // call `next_batch` because we need
                        // to keep advancing the epoch for
                        // this input, even if it hasn't been
                        // awoken to prevent dataflow stall.
                        if part.awake_due(now) {
                            unwrap_any!(Python::with_gil(|py| -> PyResult<()> {
                                let batch_res = with_timer!(
                                    next_batch_histogram,
                                    labels,
                                    state.next_batch(py, &part_key)?
                                );

                                match batch_res {
                                    BatchResult::Batch(batch) => {
                                        let batch_len = batch.len();
                                        batch_size_histogram.record(batch_len as u64, &labels);

                                        let mut downstream_session =
                                            downstream_handle.session(&part.downstream_cap);
                                        item_out_count.add(batch_len as u64, &labels);
                                        let mut batch =
                                            batch.into_iter().map(TdPyAny::from).collect();
                                        downstream_session.give_vec(&mut batch);

                                        let next_awake_res = state.next_awake(py, &part_key)?;
                                        part.next_awake =
                                            default_next_awake(next_awake_res, batch_len, now);
                                    }
                                    BatchResult::Eof => {
                                        eof = true;
                                        eofd_parts_buffer.push(part_key.clone());
                                        tracing::debug!("EOFd");
                                    }
                                    BatchResult::Abort => {
                                        abort.store(true, atomic::Ordering::Relaxed);
                                        tracing::debug!("EOFd");
                                    }
                                }

                                Ok(())
                            }));
                        }

                        // Do immediate snapshot here if requested.
                        if immediate_snapshot {
                            snaps.push((part_key.clone(), part.snap_cap.clone(), epoch));
                        }

                        // Increment the epoch for this partition when the interval
                        // elapses or the input is EOF. We increment and snapshot on EOF
                        // so that we capture the offsets for this partition to resume
                        // from; we produce the same behavior as if this partition would
                        // have lived to the next epoch interval. Only increment once
                        // we've caught up (in this if-block) otherwise you can get
                        // cascading advancement and never poll input.
                        if now - part.epoch_started >= epoch_interval.0 || eof {
                            // Do snapshots in batch at the end of the epoch otherwise
                            if !immediate_snapshot {
                                snaps.push((part_key.clone(), part.snap_cap.clone(), epoch));
                            }
                            let next_epoch = epoch + 1;
                            part.downstream_cap.downgrade(&next_epoch);
                            part.snap_cap.downgrade(&next_epoch);
                            part.epoch_started = now;
                            tracing::debug!("Advanced to epoch {next_epoch}");
                        }
                    }
                }

                // Now snapshot and send to snap_stream. The `snaps` vec is populated
                // according to immediate_mode, so we can check here at every activation.
                if recovery_on {
                    for (part_key, snap_cap, epoch) in snaps.drain(..) {
                        unwrap_any!(Python::with_gil(|py| -> PyResult<()> {
                            let mut snaps_session = snaps_handle.session(&snap_cap);
                            let snap = with_timer!(
                                snapshot_histogram,
                                labels,
                                state
                                    .snap(py, part_key, epoch)
                                    .reraise("Error snapshotting PartitionedInput")?
                            );
                            // TODO: Batch snapshots writes possibly
                            state.write_snapshots(vec![snap.clone()]);
                            snaps_session.give(snap);
                            Ok(())
                        }));
                    }
                }

                for part in eofd_parts_buffer {
                    state.remove(&part);
                    parts.remove(&part);
                }

                if !parts.is_empty() {
                    if let Some(min_next_awake) = parts
                        .values()
                        .map(|state| state.next_awake.unwrap_or(now))
                        .min()
                    {
                        let awake_after = min_next_awake - now;
                        // If we are already late for the next activation, awake immediately.
                        let awake_after = awake_after.to_std().unwrap_or(std::time::Duration::ZERO);
                        activator.activate_after(awake_after);
                    }
                }
            }
        });

        Ok((downstream, snaps))
    }
}

/// Represents a `bytewax.inputs.StatefulSourcePartition` in Python.
pub(crate) struct StatefulPartition(Py<PyAny>);

/// Do some eager type checking.
impl<'py> FromPyObject<'py> for StatefulPartition {
    fn extract_bound(ob: &Bound<'py, PyAny>) -> PyResult<Self> {
        let py = ob.py();
        let abc = py
            .import_bound("bytewax.inputs")?
            .getattr("StatefulSourcePartition")?;
        if !ob.is_instance(&abc)? {
            Err(tracked_err::<PyTypeError>(
                "stateful source partition must subclass `bytewax.inputs.StatefulSourcePartition`",
            ))
        } else {
            Ok(Self(ob.to_object(py)))
        }
    }
}

pub(crate) enum BatchResult {
    Eof,
    Abort,
    Batch(Vec<PyObject>),
}

impl StatefulPartition {
    pub(crate) fn next_batch(&self, py: Python) -> PyResult<BatchResult> {
        match self.0.bind(py).call_method0(intern!(py, "next_batch")) {
            Err(err) if err.is_instance_of::<PyStopIteration>(py) => Ok(BatchResult::Eof),
            Err(err) if err.is_instance_of::<AbortExecution>(py) => Ok(BatchResult::Abort),
            Err(err) => Err(err),
            Ok(obj) => {
                let iter = obj.iter().reraise_with(|| {
                    format!(
                        "`next_batch` must return an iterable; got a `{}` instead",
                        unwrap_any!(obj.get_type().name()),
                    )
                })?;
                let batch = iter
                    .map(|res| res.map(PyObject::from))
                    .collect::<PyResult<Vec<_>>>()
                    .reraise("error while iterating through batch")?;
                Ok(BatchResult::Batch(batch))
            }
        }
    }

    pub(crate) fn next_awake(&self, py: Python) -> PyResult<Option<DateTime<Utc>>> {
        self.0
            .call_method0(py, intern!(py, "next_awake"))?
            .extract(py)
    }

    pub(crate) fn snapshot(&self, py: Python) -> PyResult<TdPyAny> {
        Ok(self.0.call_method0(py, intern!(py, "snapshot"))?.into())
    }

    pub(crate) fn close(&self, py: Python) -> PyResult<()> {
        let _ = self.0.call_method0(py, "close");
        Ok(())
    }
}

impl Drop for StatefulPartition {
    fn drop(&mut self) {
        unwrap_any!(Python::with_gil(|py| self
            .close(py)
            .reraise("error closing StatefulSourcePartition")));
    }
}

/// Represents a `bytewax.inputs.DynamicInput` from Python.
#[derive(Clone)]
pub(crate) struct DynamicSource(Py<PyAny>);

/// Do some eager type checking.
impl<'py> FromPyObject<'py> for DynamicSource {
    fn extract_bound(ob: &Bound<'py, PyAny>) -> PyResult<Self> {
        let py = ob.py();
        let abc = py
            .import_bound("bytewax.inputs")?
            .getattr("DynamicSource")?;
        if !ob.is_instance(&abc)? {
            Err(tracked_err::<PyTypeError>(
                "dynamic source must subclass `bytewax.inputs.DynamicSource`",
            ))
        } else {
            Ok(Self(ob.to_object(py)))
        }
    }
}

struct DynamicPartState {
    part: StatelessPartition,
    output_cap: Capability<u64>,
    epoch_started: DateTime<Utc>,
    next_awake: Option<DateTime<Utc>>,
}

impl DynamicPartState {
    fn awake_due(&self, now: DateTime<Utc>) -> bool {
        match self.next_awake {
            None => true,
            Some(next_awake) => now >= next_awake,
        }
    }
}

impl DynamicSource {
    fn build(
        &self,
        py: Python,
        step_id: &StepId,
        index: WorkerIndex,
        count: WorkerCount,
    ) -> PyResult<StatelessPartition> {
        self.0
            .call_method1(py, "build", (step_id.0.clone(), index.0, count.0))?
            .extract(py)
    }

    /// Read items from a dynamic input.
    ///
    /// Will manage automatically building sources. All you have to do
    /// is pass in the definition.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn dynamic_input<S>(
        self,
        py: Python,
        scope: &S,
        step_id: StepId,
        epoch_interval: EpochInterval,
        probe: &ProbeHandle<u64>,
        abort: &Arc<AtomicBool>,
        start_at: ResumeEpoch,
    ) -> PyResult<Stream<S, TdPyAny>>
    where
        S: Scope<Timestamp = u64>,
    {
        let worker_index = scope.w_index();
        let worker_count = scope.w_count();
        let part = self
            .build(py, &step_id, worker_index, worker_count)
            .reraise_with(|| format!("error calling `DynamicSource.build` in step {step_id}"))?;

        let op_name = format!("{step_id}.dynamic_input");
        let mut op_builder = OperatorBuilder::new(op_name.clone(), scope.clone());

        let (mut downstream_wrapper, downstream) = op_builder.new_output();

        let info = op_builder.operator_info();
        let activator = scope.activator_for(&info.address[..]);
        let probe = probe.clone();
        let abort = abort.clone();

        let meter = opentelemetry::global::meter("bytewax");
        let item_out_count = meter
            .u64_counter("item_out_count")
            .with_description("number of items this step has emitted")
            .init();
        let next_batch_histogram = meter
            .f64_histogram("inp_part_next_batch_duration_seconds")
            .with_description("`next_batch` duration in seconds")
            .init();
        let labels = vec![
            KeyValue::new("step_id", step_id.0.to_string()),
            KeyValue::new("worker_index", worker_index.0.to_string()),
        ];

        op_builder.build(move |mut init_caps| {
            let now = Utc::now();

            // Inputs must init to the resume epoch.
            init_caps.downgrade_all(&start_at.0);
            let output_cap = init_caps.pop().unwrap();

            let next_awake = unwrap_any!(Python::with_gil(|py| part
                .next_awake(py)
                .reraise("error getting next awake time")));
            let mut part_state = Some(DynamicPartState {
                part,
                output_cap,
                epoch_started: now,
                next_awake,
            });

            move |_input_frontiers| {
                tracing::debug_span!("operator", operator = op_name).in_scope(|| {
                    let now = Utc::now();

                    let mut eof = false;

                    let mut downstream_handle = downstream_wrapper.activate();
                    if let Some(part_state) = &mut part_state {
                        let epoch = part_state.output_cap.time();

                        // When we increment the epoch for this
                        // partition, wait until all ouputs have
                        // finished the previous epoch before emitting
                        // more data to have backpressure.
                        if !probe.less_than(epoch) {
                            // Separately check wheither we should
                            // call `next_batch` because we need to
                            // keep advancing the epoch for this
                            // input, even if it hasn't been awoken to
                            // prevent dataflow stall.
                            if part_state.awake_due(now) {
                                unwrap_any!(Python::with_gil(|py| -> PyResult<()> {
                                    let res = with_timer!(
                                        next_batch_histogram,
                                        labels,
                                        part_state.part.next_batch(py).reraise_with(|| format!(
                                            "error calling `StatelessSourcePartition.next_batch` \
                                                in step {step_id}"
                                        ))?
                                    );

                                    match res {
                                        BatchResult::Batch(batch) => {
                                            let batch_len = batch.len();

                                            let mut downstream_session =
                                                downstream_handle.session(&part_state.output_cap);
                                            item_out_count.add(batch_len as u64, &labels);
                                            let mut batch =
                                                batch.into_iter().map(TdPyAny::from).collect();
                                            downstream_session.give_vec(&mut batch);

                                            let next_awake_res = part_state
                                                .part
                                                .next_awake(py)
                                                .reraise_with(|| {
                                                    format!(
                                                        "error calling \
                                                        `StatelessSourcePartition.next_awake` \
                                                        in step {step_id}"
                                                    )
                                                })?;

                                            part_state.next_awake =
                                                default_next_awake(next_awake_res, batch_len, now);
                                        }
                                        BatchResult::Eof => {
                                            eof = true;
                                            tracing::trace!("EOFd");
                                        }
                                        BatchResult::Abort => {
                                            abort.store(true, atomic::Ordering::Relaxed);
                                            tracing::error!("Aborted");
                                        }
                                    }

                                    Ok(())
                                }));
                            }

                            // Only increment once we've caught up (in
                            // this if-block) otherwise you can get
                            // cascading advancement and never poll
                            // input.
                            if now - part_state.epoch_started >= epoch_interval.0 {
                                let next_epoch = epoch + 1;
                                part_state.epoch_started = now;
                                part_state.output_cap.downgrade(&next_epoch);
                                tracing::trace!("Advanced to epoch {next_epoch}");
                            }
                        }
                    }

                    if eof {
                        part_state = None;
                    }

                    if let Some(part_state) = &part_state {
                        if let Some(next_awake) = part_state.next_awake {
                            let awake_after = next_awake - now;
                            // If we are already late for the next
                            // activation, awake immediately.
                            let awake_after =
                                awake_after.to_std().unwrap_or(std::time::Duration::ZERO);
                            activator.activate_after(awake_after);
                        } else {
                            activator.activate();
                        }
                    }
                });
            }
        });

        Ok(downstream)
    }
}

/// Represents a `bytewax.inputs.StatelessSource` in Python.
struct StatelessPartition(Py<PyAny>);

/// Do some eager type checking.
impl<'py> FromPyObject<'py> for StatelessPartition {
    fn extract_bound(ob: &Bound<'py, PyAny>) -> PyResult<Self> {
        let py = ob.py();
        let abc = py
            .import_bound("bytewax.inputs")?
            .getattr("StatelessSourcePartition")?;
        if !ob.is_instance(&abc)? {
            Err(tracked_err::<PyTypeError>(
                "stateless source partition must subclass \
                `bytewax.inputs.StatelessSourcePartition`",
            ))
        } else {
            Ok(Self(ob.to_object(py)))
        }
    }
}

impl StatelessPartition {
    fn next_batch(&self, py: Python) -> PyResult<BatchResult> {
        match self.0.bind(py).call_method0(intern!(py, "next_batch")) {
            Err(err) if err.is_instance_of::<PyStopIteration>(py) => Ok(BatchResult::Eof),
            Err(err) if err.is_instance_of::<AbortExecution>(py) => Ok(BatchResult::Abort),
            Err(err) => Err(err),
            Ok(obj) => {
                let iter = obj.iter().reraise_with(|| {
                    format!(
                        "`next_batch` must return an iterable; got a `{}` instead",
                        unwrap_any!(obj.get_type().name()),
                    )
                })?;
                let batch = iter
                    .map(|res| res.map(PyObject::from))
                    .collect::<PyResult<Vec<_>>>()
                    .reraise("error while iterating through batch")?;
                Ok(BatchResult::Batch(batch))
            }
        }
    }

    fn next_awake(&self, py: Python) -> PyResult<Option<DateTime<Utc>>> {
        self.0
            .call_method0(py, intern!(py, "next_awake"))?
            .extract(py)
    }

    fn close(&self, py: Python) -> PyResult<()> {
        let _ = self.0.call_method0(py, "close")?;
        Ok(())
    }
}

impl Drop for StatelessPartition {
    fn drop(&mut self) {
        unwrap_any!(Python::with_gil(|py| self
            .close(py)
            .reraise("error closing StatelessSourcePartition")));
    }
}

pub(crate) fn register(py: Python, m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add("AbortExecution", py.get_type_bound::<AbortExecution>())?;
    Ok(())
}
