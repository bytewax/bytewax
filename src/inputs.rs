//! Internal code for input.
//!
//! For a user-centric version of input, read the `bytewax.inputs`
//! Python module docstring. Read that first.

use std::cell::RefCell;
use std::collections::BTreeMap;
use std::rc::Rc;
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
use pyo3::types::PyBytes;
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

pub(crate) struct InputState {
    step_id: StepId,

    // References to LocalStateStore and StateStoreCache
    local_state_store: Option<Rc<RefCell<LocalStateStore>>>,
    state_store_cache: Rc<RefCell<StateStoreCache>>,

    // Internal state
    parts: BTreeMap<StateKey, PartitionedPartState>,
    current_time: DateTime<Utc>,
    eofd_parts_buffer: Vec<StateKey>,

    // Builder function used each time we need to insert a partition
    // into the state_store_cache.
    builder: Box<dyn Fn(Python, StateKey, Option<PyObject>) -> PyResult<StatefulSourcePartition>>,

    // Snapshots
    snaps: Vec<StateKey>,
    snapshot_mode: SnapshotMode,
}

impl InputState {
    pub fn init(
        py: Python,
        step_id: StepId,
        local_state_store: Option<Rc<RefCell<LocalStateStore>>>,
        state_store_cache: Rc<RefCell<StateStoreCache>>,
        source: FixedPartitionedSource,
        snapshot_mode: SnapshotMode,
    ) -> PyResult<Self> {
        state_store_cache.borrow_mut().add_step(step_id.clone());
        let s_id = step_id.clone();
        let builder =
            move |py: Python<'_>, state_key, state| -> PyResult<StatefulSourcePartition> {
                let parts_list = source.list_parts(py)?;
                assert!(
                    parts_list.contains(&state_key),
                    "State found for unknown key {} in the recovery store for {}. \
                    Known partitions: {}. \
                    Fixed partitions cannot change between executions, aborting.",
                    state_key,
                    s_id,
                    parts_list
                        .iter()
                        .map(|sk| format!("\"{}\"", sk.0))
                        .collect::<Vec<_>>()
                        .join(", ")
                );

                source.build_part(py, &s_id, &state_key, state)
            };

        // Initialize and hydrate the state if a local_state_store is present.
        let mut this = Self {
            step_id,
            local_state_store,
            state_store_cache,
            snapshot_mode,
            current_time: Utc::now(),
            builder: Box::new(builder),
            parts: BTreeMap::new(),
            snaps: vec![],
            eofd_parts_buffer: vec![],
        };

        if let Some(lss) = this.local_state_store.as_ref() {
            let snaps = lss.borrow_mut().get_snaps(py, &this.step_id)?;
            for (state_key, state) in snaps {
                this.insert(py, state_key, state)
            }
        }

        Ok(this)
    }

    pub fn init_step(&mut self) {
        self.current_time = Utc::now();
    }

    pub fn start_at(&self) -> ResumeEpoch {
        if let Some(lss) = self.local_state_store.as_ref() {
            lss.borrow().resume_from_epoch()
        } else {
            ResumeFrom::default().epoch()
        }
    }

    pub fn snap(&self, py: Python, key: StateKey, epoch: u64) -> PyResult<SerializedSnapshot> {
        let ssc = self.state_store_cache.borrow();
        let ser_change = ssc
            .get(&self.step_id, &key)
            // It's ok if there's no logic, because it might have been discarded
            // due to one of the `on_*` methods returning `IsComplete::Discard`.
            .map(|logic| -> PyResult<Vec<u8>> {
                let snap = logic
                    .call_method0(py, intern!(py, "snapshot"))
                    .reraise_with(|| {
                        format!(
                            "error calling `snapshot` in {} for key {}",
                            self.step_id, key
                        )
                    })?;

                let pickle = py.import_bound(intern!(py, "pickle"))?;
                let ser_snap = pickle
                    .call_method1(intern!(py, "dumps"), (snap.bind(py),))
                    .reraise("Error serializing snapshot")?;
                let ser_snap = ser_snap.downcast::<PyBytes>()?;
                let ser_snap = ser_snap.as_bytes().to_vec();
                Ok(ser_snap)
            })
            .transpose()?;

        Ok(SerializedSnapshot::new(
            self.step_id.clone(),
            key,
            epoch,
            ser_change,
        ))
    }

    pub fn snapshots(&mut self, py: Python) -> Vec<(Capability<u64>, SerializedSnapshot)> {
        let mut res = vec![];
        let snaps: Vec<_> = self.snaps.drain(..).collect();
        for key in snaps {
            let epoch = self.epoch_for(&key);
            let snap = self
                .snap(py, key.clone(), epoch)
                .reraise("Error snapshotting PartitionedInput")
                .unwrap();
            if let Some(lss) = self.local_state_store.as_ref() {
                lss.borrow_mut().write_snapshots(vec![snap.clone()]);
            }
            let cap = self.parts.get(&key).unwrap().snap_cap.clone();
            res.push((cap, snap));
        }
        res
    }

    pub fn advance_epoch(&mut self, key: &StateKey, next_epoch: u64) {
        let part = self.parts.get_mut(key).unwrap();
        part.downstream_cap.downgrade(&next_epoch);
        part.snap_cap.downgrade(&next_epoch);
        part.epoch_started = self.current_time;
        // We snapshot on EOF so that we capture the offsets for this partition to
        // resume from; we produce the same behavior as if this partition would have
        // lived to the next epoch interval.
        self.snaps.push(key.clone())
    }

    pub fn awake_after(&self) -> Option<std::time::Duration> {
        self.parts
            .values()
            .map(|part| part.next_awake.unwrap_or(self.current_time))
            .min()
            .map(|min_next_awake| {
                (min_next_awake - self.current_time)
                    .to_std()
                    // If we are already late for the next activation, awake immediately.
                    .unwrap_or(std::time::Duration::ZERO)
            })
    }

    pub fn epoch_for(&mut self, key: &StateKey) -> u64 {
        *self.parts.get(key).unwrap().downstream_cap.time()
    }

    pub fn remove(&mut self, py: Python, key: &StateKey) {
        let logic = self
            .state_store_cache
            .borrow_mut()
            .remove(&self.step_id, key)
            .unwrap();
        logic.call_method0(py, "close").unwrap();
        self.parts.remove(key);
    }

    pub fn awake_due(&mut self, key: &StateKey) -> bool {
        let res = self.parts.get(key).unwrap().awake_due(self.current_time);
        // TODO: Explain why we do this here
        if self.snapshot_mode.immediate() {
            self.snaps.push(key.clone())
        }
        res
    }

    pub fn populate_parts(
        &mut self,
        py: Python,
        downstream_cap: Capability<u64>,
        snap_cap: Capability<u64>,
    ) {
        for key in self.keys() {
            let next_awake = self.next_awake(py, &key).unwrap();
            let part_state = PartitionedPartState {
                downstream_cap: downstream_cap.clone(),
                snap_cap: snap_cap.clone(),
                epoch_started: self.current_time,
                next_awake,
            };
            self.parts.insert(key, part_state);
        }
    }

    pub fn keys(&self) -> Vec<StateKey> {
        self.state_store_cache.borrow().keys(&self.step_id)
    }

    pub fn contains_key(&self, key: &StateKey) -> bool {
        self.state_store_cache
            .borrow()
            .contains_key(&self.step_id, key)
    }

    pub fn insert(&mut self, py: Python, state_key: StateKey, state: Option<PyObject>) {
        let logic = (self.builder)(py, state_key.clone(), state).unwrap();
        self.snaps.push(state_key.clone());
        self.state_store_cache
            .borrow_mut()
            .insert(&self.step_id, state_key, logic.0);
    }

    pub fn next_batch(&mut self, py: Python, key: &StateKey) -> PyResult<BatchResult> {
        let batch = self
            .state_store_cache
            .borrow()
            .get(&self.step_id, key)
            .unwrap()
            .bind(py)
            .call_method0(intern!(py, "next_batch"))
            .reraise_with(|| {
                format!(
                    "error calling `StatefulSourcePartition.next_batch` in \
                    step {} for partition {}",
                    self.step_id, key
                )
            });

        let res = match batch {
            Err(err) if err.is_instance_of::<PyStopIteration>(py) => Ok(BatchResult::Eof),
            Err(err) if err.is_instance_of::<AbortExecution>(py) => Ok(BatchResult::Abort),
            Err(err) => Err(err),
            Ok(obj) => {
                let batch = obj
                    .iter()
                    .reraise_with(|| {
                        format!(
                            "`next_batch` must return an iterable; got a `{}` instead",
                            unwrap_any!(obj.get_type().name()),
                        )
                    })?
                    .map(|res| res.map(PyObject::from))
                    .collect::<PyResult<Vec<_>>>()
                    .reraise("error while iterating through batch")?;
                Ok(BatchResult::Batch(batch))
            }
        };

        match res {
            Ok(BatchResult::Batch(ref batch)) => {
                let batch_len = batch.len();
                let next_awake_res = self.next_awake(py, key)?;
                self.update_part_next_awake(key, next_awake_res, batch_len, self.current_time)
            }
            Ok(BatchResult::Eof) => {
                self.eofd_parts_buffer.push(key.clone());
            }
            _ => {}
        }

        res
    }

    pub fn clear_eofd_parts(&mut self, py: Python) {
        let parts: Vec<_> = self.eofd_parts_buffer.drain(..).collect();
        for part in parts {
            self.remove(py, &part)
        }
    }

    pub fn downstream_cap(&self, key: &StateKey) -> &Capability<u64> {
        &self.parts.get(key).unwrap().downstream_cap
    }

    pub fn epoch_started(&self, key: &StateKey) -> DateTime<Utc> {
        self.parts.get(key).unwrap().epoch_started
    }

    pub fn epoch_ended(&self, key: &StateKey, epoch_interval: EpochInterval) -> bool {
        self.current_time - self.epoch_started(key) >= epoch_interval.0
    }

    pub fn update_part_next_awake(
        &mut self,
        key: &StateKey,
        next_awake: Option<DateTime<Utc>>,
        batch_len: usize,
        now: DateTime<Utc>,
    ) {
        self.parts.get_mut(key).unwrap().next_awake =
            default_next_awake(next_awake, batch_len, now);
    }

    pub fn next_awake(&self, py: Python, key: &StateKey) -> PyResult<Option<DateTime<Utc>>> {
        self.state_store_cache
            .borrow()
            .get(&self.step_id, key)
            .unwrap()
            .bind(py)
            .call_method0(intern!(py, "next_awake"))
            .reraise_with(|| {
                format!(
                    "error calling `StatefulSourcePartition.next_awake` in \
                    step {} for partition {}",
                    self.step_id, key
                )
            })?
            .extract()
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
    ) -> PyResult<StatefulSourcePartition> {
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
        let this_worker = scope.w_index();

        let local_parts = self.list_parts(py).reraise_with(|| {
            format!("error calling `FixedPartitionSource.list_parts` in step {step_id}")
        })?;
        let all_parts = local_parts.into_broadcast(scope, S::Timestamp::minimum());
        let primary_updates = all_parts.assign_primaries(format!("{step_id}.assign_primaries"));

        let op_name = format!("{step_id}.partitioned_input");
        let mut op_builder = OperatorBuilder::new(op_name.clone(), scope.clone());

        let mut primaries_input = op_builder.new_input(&primary_updates, Pipeline);

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

        op_builder.build(move |mut init_caps| {
            init_caps.downgrade_all(&state.start_at().0);
            let init_snap_cap = init_caps.pop().unwrap();
            let init_downstream_cap = init_caps.pop().unwrap();
            let mut init_caps = Some((init_downstream_cap, init_snap_cap));

            let mut inbuf = InBuffer::new();

            move |input_frontiers| {
                let _guard = tracing::debug_span!("operator", operator = op_name).entered();
                state.init_step();

                primaries_input.for_each(|cap, incoming| {
                    let epoch = cap.time();
                    inbuf.extend(*epoch, incoming);
                });

                // Wait for primaries to be assigned (our only input, so check if frontier is eof)
                let frontier = &input_frontiers[0];
                if !frontier.is_eof() {
                    return;
                }

                // Once that's done, populate state and move forward with the dataflow.
                if let Some((downstream_cap, snap_cap)) = init_caps.take() {
                    let closed_epochs = inbuf
                        .epochs()
                        .filter(|e| frontier.is_closed(e))
                        .collect::<Vec<_>>();

                    Python::with_gil(|py| {
                        for epoch in closed_epochs {
                            if let Some(parts) = inbuf.remove(&epoch) {
                                for (part_key, worker) in parts {
                                    if worker == this_worker && !state.contains_key(&part_key) {
                                        state.insert(py, part_key, None);
                                    }
                                }
                            }
                        }

                        state.populate_parts(py, downstream_cap, snap_cap);
                    });
                }

                // Now run the logic for all the keys in the state.
                let mut downstream_handle = downstream_output.activate();
                let mut snaps_handle = snaps_output.activate();

                for key in state.keys() {
                    let _guard = tracing::trace_span!("partition", part_key = ?key).entered();
                    let epoch = state.epoch_for(&key);

                    // When we increment the epoch for this partition, wait until all
                    // ouputs have finished the previous epoch before emitting more
                    // data to have backpressure.
                    if !probe.less_than(&epoch) {
                        let mut eof = false;
                        // Separately check whether we should call `next_batch` because
                        // we need to keep advancing the epoch for this input, even if it
                        // hasn't been awoken to prevent dataflow stall.
                        if state.awake_due(&key) {
                            unwrap_any!(Python::with_gil(|py| -> PyResult<()> {
                                let batch_res = with_timer!(
                                    next_batch_histogram,
                                    labels,
                                    state.next_batch(py, &key)?
                                );

                                match batch_res {
                                    BatchResult::Batch(batch) => {
                                        let batch_len = batch.len();
                                        batch_size_histogram.record(batch_len as u64, &labels);

                                        let downstream_cap = state.downstream_cap(&key);
                                        let mut downstream_session =
                                            downstream_handle.session(downstream_cap);
                                        item_out_count.add(batch_len as u64, &labels);
                                        let mut batch =
                                            batch.into_iter().map(TdPyAny::from).collect();
                                        downstream_session.give_vec(&mut batch);
                                    }
                                    BatchResult::Eof => {
                                        eof = true;
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

                        // Increment the epoch for this partition when the interval
                        // elapses or the input is EOF.
                        // Only increment once we've caught up (in this if-block) otherwise
                        // you can get cascading advancement and never poll input.
                        if state.epoch_ended(&key, epoch_interval) || eof {
                            let next_epoch = epoch + 1;
                            state.advance_epoch(&key, next_epoch);
                            tracing::debug!("Advanced to epoch {next_epoch}");
                        }
                    }
                }

                // Now send the generated snapshots to the snapshots stream.
                Python::with_gil(|py| {
                    with_timer!(snapshot_histogram, labels, {
                        for (snap_cap, snap) in state.snapshots(py) {
                            let mut snaps_session = snaps_handle.session(&snap_cap);
                            snaps_session.give(snap);
                        }
                    });

                    // Clear state from parts that have EOFd
                    state.clear_eofd_parts(py);
                });

                if let Some(awake_after) = state.awake_after() {
                    activator.activate_after(awake_after);
                }
            }
        });

        Ok((downstream, snaps))
    }
}

/// Represents a `bytewax.inputs.StatefulSourcePartition` in Python.
pub(crate) struct StatefulSourcePartition(Py<PyAny>);

/// Do some eager type checking.
impl<'py> FromPyObject<'py> for StatefulSourcePartition {
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
