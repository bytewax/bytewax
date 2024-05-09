//! Internal code for implementing recovery.
//!
//! For a user-centric version of recovery, read the
//! `bytewax.recovery` Python module docstring. Read that first.

use std::cell::Ref;
use std::cell::RefCell;
use std::cell::RefMut;
use std::collections::BTreeMap;
use std::collections::HashMap;
use std::fmt;
use std::fmt::Debug;
use std::hash::Hash;
use std::path::PathBuf;
use std::rc::Rc;

use chrono::DateTime;
use chrono::Utc;
use pyo3::exceptions::PyFileNotFoundError;
use pyo3::exceptions::PyRuntimeError;
use pyo3::exceptions::PyTypeError;
use pyo3::intern;
use pyo3::prelude::*;
use pyo3::sync::GILOnceCell;
use pyo3::types::PyBytes;
use rusqlite::Connection;
use rusqlite::OpenFlags;
use rusqlite_migration::Migrations;
use rusqlite_migration::M;
use serde::Deserialize;
use serde::Serialize;
use timely::dataflow::channels::pact::Pipeline;
use timely::dataflow::operators::generic::builder_rc::OperatorBuilder;
use timely::dataflow::operators::Concatenate;
use timely::dataflow::Scope;
use timely::dataflow::Stream;

use crate::errors::tracked_err;
use crate::errors::PythonException;
use crate::inputs;
use crate::inputs::BatchResult;
use crate::inputs::StatefulSourcePartition;
use crate::operators::StatefulBatchLogic;
use crate::outputs;
use crate::outputs::StatefulSinkPartition;
use crate::pyo3_extensions::TdPyAny;
use crate::pyo3_extensions::TdPyCallable;
use crate::timely::*;
use crate::unwrap_any;

#[derive(Clone, Debug)]
pub(crate) struct Backup(PyObject);

impl<'py> FromPyObject<'py> for Backup {
    fn extract_bound(obj: &Bound<'py, PyAny>) -> PyResult<Self> {
        let py = obj.py();
        let abc = py.import_bound("bytewax.backup")?.getattr("Backup")?;
        if !obj.is_instance(&abc)? {
            Err(tracked_err::<PyTypeError>(
                "backup must subclass `bytewax.backup.Backup`",
            ))
        } else {
            Ok(Self(obj.to_object(py)))
        }
    }
}

impl IntoPy<Py<PyAny>> for Backup {
    fn into_py(self, _py: Python<'_>) -> Py<PyAny> {
        self.0
    }
}

impl Backup {
    pub(crate) fn list_keys(&self, py: Python) -> PyResult<Vec<String>> {
        self.0
            .call_method0(py, intern!(py, "list_keys"))?
            .extract(py)
    }

    pub(crate) fn upload(&self, py: Python, from_local: PathBuf, to_key: String) -> PyResult<()> {
        self.0
            .call_method_bound(py, intern!(py, "upload"), (from_local, to_key), None)?;
        Ok(())
    }

    pub(crate) fn download(&self, py: Python, from_key: String, to_local: PathBuf) -> PyResult<()> {
        self.0
            .call_method_bound(py, intern!(py, "download"), (from_key, to_local), None)?;
        Ok(())
    }

    pub(crate) fn delete(&self, py: Python, key: String) -> PyResult<()> {
        self.0.call_method1(py, intern!(py, "delete"), (key,))?;
        Ok(())
    }
}

pub(crate) enum StatefulLogicKind {
    Stateful(StatefulBatchLogic),
    Source(StatefulSourcePartition),
    Sink(StatefulSinkPartition),
}

impl From<StatefulBatchLogic> for StatefulLogicKind {
    fn from(val: StatefulBatchLogic) -> Self {
        StatefulLogicKind::Stateful(val)
    }
}

impl From<StatefulSourcePartition> for StatefulLogicKind {
    fn from(val: StatefulSourcePartition) -> Self {
        StatefulLogicKind::Source(val)
    }
}

impl From<StatefulSinkPartition> for StatefulLogicKind {
    fn from(val: StatefulSinkPartition) -> Self {
        StatefulLogicKind::Sink(val)
    }
}

impl StatefulLogicKind {
    pub(crate) fn snapshot(&self, py: Python) -> PyResult<TdPyAny> {
        match self {
            Self::Stateful(logic) => logic.snapshot(py),
            Self::Source(logic) => logic.snapshot(py),
            Self::Sink(logic) => logic.snapshot(py),
        }
    }
    pub(crate) fn as_source(&self) -> &StatefulSourcePartition {
        assert!(
            matches!(self, Self::Source(_)),
            "Trying to treat input state as a state for a different operator. \
            This is a bug in Bytewax, aborting!"
        );
        match self {
            Self::Source(logic) => logic,
            _ => unreachable!(),
        }
    }
    pub(crate) fn as_sink(&self) -> &StatefulSinkPartition {
        assert!(
            matches!(self, Self::Sink(_)),
            "Trying to treat output state as a state for a different operator. \
            This is a bug in Bytewax, aborting!"
        );
        match self {
            Self::Sink(logic) => logic,
            _ => unreachable!(),
        }
    }
    pub(crate) fn as_stateful(&self) -> &StatefulBatchLogic {
        assert!(
            matches!(self, Self::Stateful(_)),
            "Trying to treat stateful_batch state as a state for a different operator. \
            This is a bug in Bytewax, aborting!"
        );
        match self {
            Self::Stateful(logic) => logic,
            _ => unreachable!(),
        }
    }
}

pub(crate) struct StateStore {
    flow_id: String,
    worker_index: usize,
    worker_count: usize,
    recovery_config: Option<RecoveryConfig>,

    cache: HashMap<StepId, BTreeMap<StateKey, StatefulLogicKind>>,
    conn: Option<Connection>,

    resume_from: ResumeFrom,
    seg_num: u64,
    prev_epoch: u64,
}

impl StateStore {
    pub fn new(
        recovery_config: Option<RecoveryConfig>,
        flow_id: String,
        worker_index: usize,
        worker_count: usize,
    ) -> PyResult<Self> {
        // Init or load the recovery db is recovery was configured.
        let mut conn = recovery_config
            .as_ref()
            .map(|rc| rc.db_connection(&flow_id, worker_index))
            .transpose()?;

        // Calculate resume_from.
        // If recovery is configured, read the latest execution number from the db,
        // otherwise start from the defaults.
        let resume_from = conn
            .as_mut()
            .map(|conn| {
                let res = conn.transaction().unwrap().query_row(
                    "SELECT ex_num, cluster_frontier, worker_count, worker_index
                    FROM meta
                    WHERE (ex_num, flow_id) IN (SELECT MAX(ex_num), flow_id FROM meta)",
                    (),
                    |row| {
                        let ex_num = row.get::<_, Option<u64>>(0)?.map(ExecutionNumber);
                        let resume_epoch = row.get::<_, Option<u64>>(1)?.map(ResumeEpoch);
                        Ok(ex_num
                            .zip(resume_epoch)
                            // Advance the execution number here.
                            .map(|(en, re)| ResumeFrom(en.next(), re)))
                    },
                );
                match res {
                    // If no rows in the db, it was empty, so use the default.
                    Err(rusqlite::Error::QueryReturnedNoRows) => ResumeFrom::default(),
                    // Any other error was an error reading from the db, we should stop here.
                    Err(err) => std::panic::panic_any(err),
                    Ok(row) => row.unwrap_or_default(),
                }
            })
            .unwrap_or_default();

        Ok(Self {
            cache: HashMap::new(),
            recovery_config,
            conn,
            flow_id,
            worker_index,
            worker_count,
            seg_num: 0,
            resume_from,
            prev_epoch: 0,
        })
    }

    pub fn backup(&self) -> Option<Backup> {
        self.recovery_config.as_ref().map(|rc| rc.backup())
    }

    pub fn resume_from(&self) -> ResumeFrom {
        self.resume_from
    }

    pub fn recovery_on(&self) -> bool {
        self.recovery_config.is_some()
    }

    pub fn immediate_snapshot(&self) -> bool {
        self.recovery_config
            .as_ref()
            .is_some_and(|rc| !rc.batch_backup)
    }

    pub fn update_resume_epoch(&mut self, epoch: u64) {
        self.resume_from.1 .0 = epoch;
    }

    // Api for managing the store.

    // Always assume that the step_id is present, as we expect it to be added
    // when the operator is instanced. If that's not the case, it's a bug
    // on our side and we should abort.
    pub fn get(&self, step_id: &StepId, key: &StateKey) -> Option<&StatefulLogicKind> {
        self.cache.get(step_id).unwrap().get(key)
    }

    pub fn get_logics(&self, step_id: &StepId) -> &BTreeMap<StateKey, StatefulLogicKind> {
        self.cache.get(step_id).unwrap()
    }

    pub fn contains_key(&self, step_id: &StepId, key: &StateKey) -> bool {
        self.cache.get(step_id).unwrap().contains_key(key)
    }

    pub fn insert(&mut self, step_id: &StepId, key: StateKey, logic: StatefulLogicKind) {
        self.cache.get_mut(step_id).unwrap().insert(key, logic);
    }

    pub fn remove(&mut self, step_id: &StepId, key: &StateKey) -> Option<StatefulLogicKind> {
        self.cache.get_mut(step_id).unwrap().remove(key)
    }

    /// Hydrate the local cache with all the snapshots for a step_id.
    /// Pass a builder function that turns a `(state_key, state)` tuple
    /// into a `StatefulLogicKind`, and it will be called with data coming
    /// from each deserialized snapshot.
    pub fn hydrate(
        &mut self,
        step_id: &StepId,
        builder: impl Fn(&StateKey, Option<PyObject>) -> StatefulLogicKind,
    ) -> PyResult<()> {
        // We never want to add a step twice, so panic if that's tried.
        assert!(
            !self.cache.contains_key(step_id),
            "Trying to hydrate state twice, this is probably a bug"
        );
        self.cache.insert(step_id.clone(), Default::default());

        // If no db connection is present, we don't need to do anything else.
        if self.conn.is_none() {
            return Ok(());
        }

        // Get all the snapshots in the store for this specific step_id,
        // deserialize them, then call the builder function to make them
        // the right StatefulLogicKind variant.
        let deserialized_snaps = Python::with_gil(|py| -> PyResult<Vec<_>> {
            let pickle = py.import_bound("pickle")?;
            Ok(self
                .conn
                .as_ref()
                // Unwrap here since we already checked conn is some.
                .unwrap()
                // Retrieve all the snapshots for the latest epoch saved
                // in the recovery store that's <= than resume_from..
                .prepare(
                    "SELECT step_id, state_key, MAX(epoch) as epoch, ser_change
                    FROM snaps
                    WHERE epoch <= ?1 AND step_id = ?2
                    GROUP BY step_id, state_key",
                )
                .reraise("Error preparing query for recovery db.")?
                .query_map((&self.resume_from.1 .0, &step_id.0), |row| {
                    Ok(SerializedSnapshot(
                        StepId(row.get(0)?),
                        StateKey(row.get(1)?),
                        SnapshotEpoch(row.get(2)?),
                        row.get(3)?,
                    ))
                })
                .reraise("Error binding query parameters in recovery store")?
                .map(|res| res.expect("Error unpacking SerializedSnapshot"))
                .map(|SerializedSnapshot(_, key, _, ser_state)| {
                    let state = ser_state.map(|ser_state| {
                        pickle
                            .call_method1(
                                intern!(py, "loads"),
                                (PyBytes::new_bound(py, &ser_state),),
                            )
                            .unwrap()
                            .unbind()
                    });
                    (key, state)
                })
                .collect())
        })?;

        for (state_key, state) in deserialized_snaps {
            self.insert(step_id, state_key.clone(), builder(&state_key, state));
        }
        Ok(())
    }

    // Snapshots related api from here.

    /// Write a vec of snapshots to the local db.
    pub(crate) fn write_snapshots(&mut self, snaps: Vec<SerializedSnapshot>) {
        assert!(
            self.conn.is_some(),
            "Trying to snapshot state without an open db connection"
        );
        let conn = self.conn.as_mut().unwrap();
        let txn = conn.transaction().unwrap();
        for snap in snaps {
            tracing::trace!("Writing {snap:?}");
            let SerializedSnapshot(step_id, state_key, snap_epoch, ser_change) = snap;
            txn.execute(
                "INSERT INTO snaps (step_id, state_key, epoch, ser_change)
                 VALUES (?1, ?2, ?3, ?4)
                 ON CONFLICT (step_id, state_key, epoch) DO UPDATE
                 SET ser_change = EXCLUDED.ser_change",
                (step_id.0, state_key.0, snap_epoch.0, ser_change),
            )
            .unwrap();
        }
        txn.commit().unwrap();
    }

    /// Generates and returns a fully serialized snapshot for a given key at the given epoch.
    pub fn snap(
        &self,
        py: Python,
        step_id: StepId,
        key: StateKey,
        epoch: u64,
    ) -> PyResult<SerializedSnapshot> {
        let ser_change = self
            .get(&step_id, &key)
            // It's ok if there's no logic, because it might have been discarded
            // due to one of the `on_*` methods returning `IsComplete::Discard`.
            .map(|logic| -> PyResult<Vec<u8>> {
                let snap = PyObject::from(logic.snapshot(py).reraise_with(|| {
                    format!("error calling `snapshot` in {step_id} for key {key}")
                })?);
                let pickle = py.import_bound(intern!(py, "pickle"))?;
                let ser_snap = pickle
                    .call_method1(intern!(py, "dumps"), (snap.bind(py),))
                    .reraise("Error serializing snapshot")?
                    .downcast::<PyBytes>()
                    .unwrap()
                    .as_bytes()
                    .to_vec();
                Ok(ser_snap)
            })
            .transpose()?;

        Ok(SerializedSnapshot(
            step_id,
            key,
            SnapshotEpoch(epoch),
            ser_change,
        ))
    }

    /// Open a new db segment where to save partial data for an epoch.
    /// The segment number resets each time the epoch changes.
    pub(crate) fn open_segment(&mut self, epoch: u64) -> PyResult<Segment> {
        assert!(
            self.recovery_config.is_some(),
            "Trying to save durable state, but recovery is not configured"
        );
        if epoch != self.prev_epoch {
            self.seg_num = 1;
            self.prev_epoch = epoch;
        } else {
            self.seg_num += 1;
        }

        let file_name = format!(
            "ex-{}:epoch-{}:segment-{}:_:worker-{}.sqlite3",
            self.resume_from.0, epoch, self.seg_num, self.worker_index
        );
        let mut path = self.recovery_config.as_ref().unwrap().db_dir.clone();
        path.push(file_name);

        Segment::new(
            path,
            self.flow_id.clone(),
            self.resume_from.1 .0,
            self.worker_index,
            self.worker_count,
        )
    }

    pub fn write_frontier(&mut self, epoch: u64) -> PyResult<()> {
        assert!(
            self.conn.is_some(),
            "Trying to snapshot state without an open db connection"
        );
        let conn = self.conn.as_mut().unwrap();
        let txn = conn.transaction().unwrap();
        tracing::trace!("Writing epoch {epoch:?}");
        txn.execute(
            "INSERT INTO meta (flow_id, ex_num, worker_index, worker_count, cluster_frontier)
            VALUES (?1, ?2, ?3, ?4, ?5)
            ON CONFLICT (flow_id, ex_num, worker_index) DO UPDATE
            SET cluster_frontier = EXCLUDED.cluster_frontier",
            (
                &self.flow_id,
                self.resume_from.0 .0,
                self.worker_index,
                self.worker_count,
                epoch,
            ),
        )
        .reraise("Error initing transaction")?;
        txn.commit().reraise("Error committing cluster_frontier")
    }
}

fn setup_conn(path: PathBuf) -> PyResult<Connection> {
    let mut conn = Connection::open_with_flags(
        path,
        OpenFlags::SQLITE_OPEN_READ_WRITE
            | OpenFlags::SQLITE_OPEN_CREATE
            | OpenFlags::SQLITE_OPEN_NO_MUTEX,
    )
    .reraise("can't open recovery DB")?;
    rusqlite::vtab::series::load_module(&conn).reraise("Error initializing db")?;
    conn.pragma_update(None, "foreign_keys", "ON")
        .reraise("Error initializing db")?;
    conn.pragma_update(None, "journal_mode", "WAL")
        .reraise("Error initializing db")?;
    conn.pragma_update(None, "busy_timeout", "5000")
        .reraise("Error initializing db")?;
    Python::with_gil(|py| {
        get_migrations(py)
            .to_latest(&mut conn)
            .reraise("Error initializing db")
    })?;
    Ok(conn)
}

pub(crate) struct Segment {
    conn: Connection,
    path: PathBuf,
    flow_id: String,
    ex_num: u64,
    worker_index: usize,
    worker_count: usize,
}

impl Segment {
    fn new(
        path: PathBuf,
        flow_id: String,
        ex_num: u64,
        worker_index: usize,
        worker_count: usize,
    ) -> PyResult<Self> {
        let conn = setup_conn(path.clone())?;
        Ok(Self {
            conn,
            path,
            flow_id,
            ex_num,
            worker_index,
            worker_count,
        })
    }

    pub fn file_name(&self) -> PathBuf {
        self.path.clone()
    }

    /// Write the frontier, and close this segment by consuming self.
    pub fn write_frontier(mut self, epoch: u64) -> PyResult<PathBuf> {
        let txn = self.conn.transaction().unwrap();
        txn.execute(
            "INSERT INTO meta (flow_id, ex_num, worker_index, worker_count, cluster_frontier)
            VALUES (?1, ?2, ?3, ?4, ?5)
            ON CONFLICT (flow_id, ex_num, worker_index) DO UPDATE
            SET cluster_frontier = EXCLUDED.cluster_frontier",
            (
                &self.flow_id,
                self.ex_num,
                self.worker_index,
                self.worker_count,
                epoch,
            ),
        )
        .reraise("Error initing transaction")?;
        txn.commit().reraise("Error committing cluster_frontier")?;
        Ok(self.path)
    }

    /// Write snapshots, and close this segment by consuming self.
    pub fn write_snapshots(mut self, snaps: Vec<SerializedSnapshot>) {
        let txn = self.conn.transaction().unwrap();
        for SerializedSnapshot(step_id, state_key, snap_epoch, ser_change) in snaps {
            txn.execute(
                "INSERT INTO snaps (step_id, state_key, epoch, ser_change)
                VALUES (?1, ?2, ?3, ?4)
                ON CONFLICT (step_id, state_key, epoch) DO UPDATE
                SET ser_change = EXCLUDED.ser_change",
                (step_id.0, state_key.0, snap_epoch.0, ser_change),
            )
            .unwrap();
        }
        txn.commit().unwrap();
    }
}

/// Trait to group common operations that are used in all stateful operators.
/// It just needs to know how to borrow the StateStore and how to retrieve
/// the step_id to manipulate state and local store.
pub(crate) trait StateManager {
    fn borrow_state(&self) -> Ref<StateStore>;
    fn borrow_state_mut(&self) -> RefMut<StateStore>;
    fn step_id(&self) -> &StepId;

    fn snap(&self, py: Python, key: StateKey, epoch: u64) -> PyResult<SerializedSnapshot> {
        self.borrow_state()
            .snap(py, self.step_id().clone(), key, epoch)
    }
    fn write_snapshots(&self, snaps: Vec<SerializedSnapshot>) {
        self.borrow_state_mut().write_snapshots(snaps);
    }
    fn recovery_on(&self) -> bool {
        self.borrow_state().recovery_config.is_some()
    }
    fn start_at(&self) -> ResumeEpoch {
        self.borrow_state().resume_from.1
    }
    fn immediate_snapshot(&self) -> bool {
        self.borrow_state()
            .recovery_config
            .as_ref()
            .is_some_and(|rc| !rc.batch_backup)
    }
    fn contains_key(&self, key: &StateKey) -> bool {
        self.borrow_state().contains_key(self.step_id(), key)
    }
    fn insert(&self, key: StateKey, logic: impl Into<StatefulLogicKind>) {
        self.borrow_state_mut()
            .insert(self.step_id(), key, logic.into());
    }
    fn remove(&mut self, key: &StateKey) {
        self.borrow_state_mut().remove(self.step_id(), key);
    }
    fn keys(&self) -> Vec<StateKey> {
        self.borrow_state()
            .get_logics(self.step_id())
            .keys()
            .cloned()
            .collect()
    }
}

pub(crate) struct OutputState {
    step_id: StepId,
    state_store: Rc<RefCell<StateStore>>,
}

impl StateManager for OutputState {
    fn borrow_state(&self) -> Ref<StateStore> {
        self.state_store.borrow()
    }

    fn borrow_state_mut(&self) -> RefMut<StateStore> {
        self.state_store.borrow_mut()
    }

    fn step_id(&self) -> &StepId {
        &self.step_id
    }
}

impl OutputState {
    pub fn hydrate(&mut self, sink: &outputs::FixedPartitionedSink) -> PyResult<()> {
        Python::with_gil(|py| {
            // Get the parts list. If any state_key is not part of this list,
            // it means the parts changed since last execution, and we can't
            // handle that.
            let parts_list = unwrap_any!(sink.list_parts(py));

            let builder = |state_key: &_, state| {
                assert!(
                    parts_list.contains(state_key),
                    "State found for unknown key {} in the recovery store for {}. \
                    Known partitions: {}. \
                    Fixed partitions cannot change between executions, aborting.",
                    state_key,
                    &self.step_id,
                    parts_list
                        .iter()
                        .map(|sk| format!("\"{}\"", sk.0))
                        .collect::<Vec<_>>()
                        .join(", ")
                );
                let part = sink
                    .build_part(py, &self.step_id, state_key, state)
                    .unwrap();
                StatefulLogicKind::Sink(part)
            };

            self.state_store
                .borrow_mut()
                .hydrate(&self.step_id, builder)
        })
    }

    pub fn new(step_id: StepId, state_store: Rc<RefCell<StateStore>>) -> Self {
        Self {
            step_id,
            state_store,
        }
    }

    pub fn write_batch(&self, py: Python, key: &StateKey, batch: Vec<PyObject>) -> PyResult<()> {
        self.state_store
            .borrow()
            .get(&self.step_id, key)
            .unwrap()
            .as_sink()
            .write_batch(py, batch)
    }
}

pub(crate) struct InputState {
    step_id: StepId,
    state_store: Rc<RefCell<StateStore>>,
}

impl StateManager for InputState {
    fn borrow_state(&self) -> Ref<StateStore> {
        self.state_store.borrow()
    }

    fn borrow_state_mut(&self) -> RefMut<StateStore> {
        self.state_store.borrow_mut()
    }

    fn step_id(&self) -> &StepId {
        &self.step_id
    }
}

impl InputState {
    pub fn new(step_id: StepId, state_store: Rc<RefCell<StateStore>>) -> Self {
        Self {
            step_id,
            state_store,
        }
    }

    pub fn hydrate(&mut self, source: &inputs::FixedPartitionedSource) -> PyResult<()> {
        Python::with_gil(|py| {
            // Get the parts list. If any state_key is not part of this list,
            // it means the parts changed since last execution, and we can't
            // handle that.
            let parts_list = unwrap_any!(source.list_parts(py));

            let builder = |state_key: &_, state| {
                assert!(
                    parts_list.contains(state_key),
                    "State found for unknown key {} in the recovery store for {}. \
                    Known partitions: {}. \
                    Fixed partitions cannot change between executions, aborting.",
                    state_key,
                    self.step_id,
                    parts_list
                        .iter()
                        .map(|sk| format!("\"{}\"", sk.0))
                        .collect::<Vec<_>>()
                        .join(", ")
                );

                let part = source
                    .build_part(py, &self.step_id, state_key, state)
                    .unwrap();
                StatefulLogicKind::Source(part)
            };

            self.state_store
                .borrow_mut()
                .hydrate(&self.step_id, builder)
        })
    }

    pub fn next_batch(&self, py: Python, key: &StateKey) -> PyResult<BatchResult> {
        self.state_store
            .borrow()
            .get(&self.step_id, key)
            .unwrap()
            .as_source()
            .next_batch(py)
            .reraise_with(|| {
                format!(
                    "error calling `StatefulSourcePartition.next_batch` in \
                    step {} for partition {}",
                    self.step_id, key
                )
            })
    }

    pub fn next_awake(&self, py: Python, key: &StateKey) -> PyResult<Option<DateTime<Utc>>> {
        self.state_store
            .borrow()
            .get(&self.step_id, key)
            .unwrap()
            .as_source()
            .next_awake(py)
            .reraise_with(|| {
                format!(
                    "error calling `StatefulSourcePartition.next_awake` in \
                    step {} for partition {}",
                    self.step_id, key
                )
            })
    }
}

pub(crate) struct StatefulBatchState {
    step_id: StepId,
    state_store: Rc<RefCell<StateStore>>,
}

impl StateManager for StatefulBatchState {
    fn borrow_state(&self) -> Ref<StateStore> {
        self.state_store.borrow()
    }

    fn borrow_state_mut(&self) -> RefMut<StateStore> {
        self.state_store.borrow_mut()
    }

    fn step_id(&self) -> &StepId {
        &self.step_id
    }
}

impl StatefulBatchState {
    pub fn new(step_id: StepId, state_store: Rc<RefCell<StateStore>>) -> Self {
        Self {
            step_id,
            state_store,
        }
    }
    pub fn hydrate(&mut self, builder: &TdPyCallable) -> PyResult<()> {
        Python::with_gil(|py| {
            let builder = builder.bind(py);
            let state_builder = |_state_key: &_, state| {
                let part = builder
                    .call1((state,))
                    .unwrap()
                    .extract::<StatefulBatchLogic>()
                    .unwrap();
                StatefulLogicKind::Stateful(part)
            };
            self.state_store
                .borrow_mut()
                .hydrate(&self.step_id, state_builder)
        })
    }
    pub fn on_batch(
        &self,
        py: Python,
        key: &StateKey,
        values: Vec<PyObject>,
    ) -> PyResult<(Vec<PyObject>, crate::operators::IsComplete)> {
        self.borrow_state()
            .get(&self.step_id, key)
            .unwrap()
            .as_stateful()
            .on_batch(py, values)
            .reraise_with(|| {
                format!(
                    "error calling `StatefulBatch.on_batch` in step {} for key {}",
                    self.step_id, &key
                )
            })
    }
    pub fn on_notify(
        &self,
        py: Python,
        key: &StateKey,
    ) -> PyResult<(Vec<PyObject>, crate::operators::IsComplete)> {
        self.state_store
            .borrow()
            .get(&self.step_id, key)
            .unwrap()
            .as_stateful()
            .on_notify(py)
            .reraise_with(|| {
                format!(
                    "error calling `StatefulBatchLogic.on_notify` in {} for key {}",
                    self.step_id, key
                )
            })
    }
    pub fn notify_at(&self, py: Python, key: &StateKey) -> PyResult<Option<DateTime<Utc>>> {
        if let Some(logic) = self.state_store.borrow().get(&self.step_id, key) {
            logic.as_stateful().notify_at(py).reraise_with(|| {
                format!(
                    "error calling `StatefulBatchLogic.notify_at` in {} for key {}",
                    self.step_id, key
                )
            })
        } else {
            Ok(None)
        }
    }
    pub fn on_eof(
        &self,
        py: Python,
        key: &StateKey,
    ) -> PyResult<(Vec<PyObject>, crate::operators::IsComplete)> {
        self.state_store
            .borrow()
            .get(&self.step_id, key)
            .unwrap()
            .as_stateful()
            .on_eof(py)
            .reraise_with(|| {
                format!(
                    "error calling `StatefulBatchLogic.on_eof` in {} for key {}",
                    self.step_id, key
                )
            })
    }
}

/// Metadata about a recovery db.
///
/// This represents a row in the `meta` table.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct ClusterMeta(
    FlowId,
    ExecutionNumber,
    WorkerIndex,
    WorkerCount,
    ClusterFrontier,
);

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct FlowId(String);

/// The oldest epoch for which work is still outstanding on the cluster.
#[derive(Debug, Copy, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct ClusterFrontier(u64);

/// Incrementing ID representing how many times a dataflow has been
/// executed to completion or failure.
///
/// This is used to ensure recovery progress information for a worker
/// `3` is not mis-interpreted to belong to an earlier cluster.
///
/// As you resume a dataflow, this will increase by 1 each time.
#[derive(Debug, Copy, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub(crate) struct ExecutionNumber(u64);

impl ExecutionNumber {
    pub(crate) fn next(mut self) -> Self {
        self.0 += 1;
        self
    }
}

impl fmt::Display for ExecutionNumber {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// The epoch a new dataflow execution should resume from the
/// beginning of.
///
/// This will be the dataflow frontier of the last execution.
#[derive(Debug, Copy, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct ResumeEpoch(pub(crate) u64);

impl fmt::Display for ResumeEpoch {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// Metadata about an execution.
///
/// This represents a row in in the `exs` table.
#[derive(Debug, Copy, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct ExecutionMeta(ExecutionNumber, WorkerCount, ResumeEpoch);

/// Metadata about the current frontier of a worker.
///
/// This represents a row in the `fronts` table.
#[derive(Debug, Copy, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct FrontierMeta(ExecutionNumber, WorkerIndex, ClusterFrontier);

/// To resume a dataflow execution, you need to know which epoch to
/// resume for state, but also which execution to label progress data
/// with.
///
/// This does not define [`Default`] and should only be calculated via
/// [`ResumeCalc::resume_from`].
#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub(crate) struct ResumeFrom(pub(crate) ExecutionNumber, pub(crate) ResumeEpoch);

impl Default for ResumeFrom {
    /// Starting execution and epoch if there is no recovery data.
    ///
    /// Note that the starting epoch is 1 and not 0 due to initial
    /// routing messages needing to be distributed in 0.
    fn default() -> Self {
        Self(ExecutionNumber(0), ResumeEpoch(1))
    }
}

/// Unique ID for a step in a dataflow.
///
/// Recovery data is keyed off of this to ensure state is not mixed
/// between operators.
#[derive(Debug, Clone, Hash, PartialEq, Eq, Serialize, Deserialize, FromPyObject)]
pub(crate) struct StepId(pub(crate) String);

impl IntoPy<Py<PyAny>> for StepId {
    fn into_py(self, py: Python<'_>) -> Py<PyAny> {
        self.0.into_py(py)
    }
}

impl ToPyObject for StepId {
    fn to_object(&self, py: Python<'_>) -> PyObject {
        self.0.to_object(py)
    }
}

/// Displays the step ID in quotes.
impl std::fmt::Display for StepId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // Format as a quoted string, but without the `StepId()` part.
        std::fmt::Debug::fmt(&self.0, f)
    }
}

/// Key to route state within a dataflow step.
///
/// This is the user-facing "state key" since users only work with
/// state within a step.
///
/// We place restraints on this, rather than allowing any Python type
/// to be routeable because the routing key interfaces with a lot of
/// Bytewax and Timely code which puts requirements on it: it has to
/// be hashable, have equality, debug printable, and is serde-able and
/// we can't guarantee those things are correct on any arbitrary
/// Python type.
#[derive(
    Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize, FromPyObject,
)]
pub(crate) struct StateKey(pub(crate) String);

impl IntoPy<Py<PyAny>> for StateKey {
    fn into_py(self, py: Python<'_>) -> Py<PyAny> {
        self.0.into_py(py)
    }
}

impl std::fmt::Display for StateKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // Format as a quoted string, but without the `StateKey()` part.
        std::fmt::Debug::fmt(&self.0, f)
    }
}

/// The epoch a snapshot was taken at the end of.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct SnapshotEpoch(u64);

/// A state snapshot for reading or writing to a recovery partition.
///
/// This represents a row in the `snaps` table.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct SerializedSnapshot(StepId, StateKey, SnapshotEpoch, Option<Vec<u8>>);

impl fmt::Display for SerializedSnapshot {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_fmt(format_args!("{}: {}, epoch {}", self.0, self.1, self.2 .0))
    }
}

/// Configuration settings for recovery.
///
/// :arg db_dir: Local filesystem directory to use for recovery
///     database partitions.
///
/// :type db_dir: pathlib.Path
///
/// :arg backup: Class to use to save recovery files to a durable
///     storage like amazon's S3.
///
/// :type backup: typing.Optional[bytewax.backup.Backup]
///
/// :arg batch_backup: Whether to take state snapshots at the end
///     of the epoch, rather than at every state change. Defaults
///     to False.
///
/// :type batch_backup: bool
#[pyclass(module = "bytewax.recovery")]
#[derive(Clone, Debug)]
pub(crate) struct RecoveryConfig {
    #[pyo3(get)]
    db_dir: PathBuf,
    #[pyo3(get)]
    backup: Backup,
    #[pyo3(get)]
    pub(crate) batch_backup: bool,
}

#[pymethods]
impl RecoveryConfig {
    #[new]
    fn new(db_dir: PathBuf, backup: Option<Backup>, batch_backup: Option<bool>) -> PyResult<Self> {
        let batch_backup = batch_backup.unwrap_or(false);

        // Manually unpack so we can propagate the error
        // if default initialization fails.
        let backup = if let Some(backup) = backup {
            backup
        } else {
            Python::with_gil(|py| {
                py.import_bound("bytewax.backup")
                    .reraise("Can't find backup module")?
                    .getattr("NoopBackup")
                    .reraise("Can't find NoopBackup")?
                    .call0()
                    .reraise("Error initializing NoopBackup")?
                    .extract()
            })?
        };

        Ok(Self {
            db_dir,
            batch_backup,
            backup,
        })
    }
}

impl RecoveryConfig {
    pub(crate) fn db_connection(
        &self,
        flow_id: &String,
        worker_index: usize,
    ) -> PyResult<Connection> {
        if !self.db_dir.is_dir() {
            return Err(PyFileNotFoundError::new_err(format!(
                "recovery directory {:?} does not exist; \
                see the `bytewax.recovery` module docstring for more info",
                self.db_dir
            )));
        }
        let file_name = format!("flow_{flow_id}_worker_{worker_index}.sqlite3");
        let mut path = self.db_dir.clone();
        path.push(file_name);

        setup_conn(path)
    }

    pub(crate) fn backup(&self) -> Backup {
        self.backup.clone()
    }
}

impl<T> PythonException<T> for Result<T, rusqlite::Error> {
    fn into_pyresult(self) -> PyResult<T> {
        self.map_err(|err| PyErr::new::<PyRuntimeError, _>(err.to_string()))
    }
}

impl<T> PythonException<T> for Result<T, rusqlite_migration::Error> {
    fn into_pyresult(self) -> PyResult<T> {
        self.map_err(|err| PyErr::new::<PyRuntimeError, _>(err.to_string()))
    }
}

// The `'static` lifetime within [`Migrations`] is saying that the
// [`str`]s composing the migrations are `'static`.
//
// Use [`GILOnceCell`] so we don't have to bring in a `lazy_static`
// crate dep.
static MIGRATIONS: GILOnceCell<Migrations<'static>> = GILOnceCell::new();

fn get_migrations(py: Python) -> &Migrations<'static> {
    MIGRATIONS.get_or_init(py, || {
        Migrations::new(vec![
            M::up(
                "CREATE TABLE meta (
                    modified_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    flow_id TEXT NOT NULL,
                    ex_num INTEGER NOT NULL,
                    worker_index INTEGER NOT NULL CHECK (worker_index >= 0),
                    worker_count INTEGER NOT NULL CHECK (worker_count > 0),
                    cluster_frontier INTEGER NOT NULL,
                    CHECK (worker_index < worker_count),
                    PRIMARY KEY (flow_id, ex_num, worker_index)
                 ) STRICT",
            ),
            M::up(
                "CREATE TABLE snaps (
                    modified_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    step_id TEXT NOT NULL,
                    state_key TEXT NOT NULL,
                    epoch INTEGER NOT NULL,
                    ser_change BLOB,
                    PRIMARY KEY (step_id, state_key, epoch)
                 ) STRICT",
            ),
        ])
    })
}

#[test]
fn migrations_valid() -> rusqlite_migration::Result<()> {
    pyo3::prepare_freethreaded_python();
    Python::with_gil(|py| get_migrations(py).validate())
}

pub(crate) trait WriteFrontiersOp<S>
where
    S: Scope,
{
    fn write_frontiers(&self, state_store: Rc<RefCell<StateStore>>) -> ClockStream<S>;
}

impl<S> WriteFrontiersOp<S> for ClockStream<S>
where
    S: Scope<Timestamp = u64>,
{
    fn write_frontiers(&self, state_store: Rc<RefCell<StateStore>>) -> ClockStream<S> {
        let mut op_builder = OperatorBuilder::new("frontier_compactor".to_string(), self.scope());
        let mut input = op_builder.new_input(self, Pipeline);
        let (mut output, clock) = op_builder.new_output();

        op_builder.build(move |init_caps| {
            let mut inbuffer = InBuffer::new();
            let mut ncater = EagerNotificator::new(init_caps, ());

            move |input_frontiers| {
                input.buffer_notify(&mut inbuffer, &mut ncater);

                ncater.for_each(
                    input_frontiers,
                    |_caps, ()| {},
                    |caps, ()| {
                        let cap = &caps[0];
                        let epoch = cap.time();
                        // Write cluster frontier
                        unwrap_any!(state_store.borrow_mut().write_frontier(*epoch));
                        // And finally allow the dataflow to advance its epoch.
                        output.activate().session(cap).give(());
                    },
                )
            }
        });
        clock
    }
}

pub(crate) trait CompactFrontiersOp<S>
where
    S: Scope,
{
    fn compact_frontiers(&self, state_store: Rc<RefCell<StateStore>>) -> Stream<S, PathBuf>;
}

impl<S> CompactFrontiersOp<S> for ClockStream<S>
where
    S: Scope<Timestamp = u64>,
{
    fn compact_frontiers(&self, state_store: Rc<RefCell<StateStore>>) -> Stream<S, PathBuf> {
        let mut op_builder = OperatorBuilder::new("frontier_compactor".to_string(), self.scope());
        let mut input = op_builder.new_input(self, Pipeline);
        let (mut segments_output, segments) = op_builder.new_output();

        op_builder.build(move |init_caps| {
            let mut inbuffer = InBuffer::new();
            let mut ncater = EagerNotificator::new(init_caps, ());

            move |input_frontiers| {
                input.buffer_notify(&mut inbuffer, &mut ncater);

                ncater.for_each(
                    input_frontiers,
                    |_caps, ()| {},
                    |caps, ()| {
                        let clock_cap = &caps[0];

                        let mut handle = segments_output.activate();
                        let mut session = handle.session(clock_cap);

                        let epochs = inbuffer.epochs().collect::<Vec<_>>();
                        let mut state = state_store.borrow_mut();
                        for epoch in epochs {
                            let segment = unwrap_any!(state.open_segment(epoch));
                            let file_name = segment.file_name();
                            inbuffer.remove(&epoch);
                            segment.write_frontier(epoch).unwrap();
                            session.give(file_name);
                        }
                    },
                )
            }
        });
        segments
    }
}

pub(crate) trait DurableBackupOp<S>
where
    S: Scope,
{
    fn durable_backup(&self, backup: Backup, immediate_backup: bool) -> ClockStream<S>;
}

impl<S> DurableBackupOp<S> for Stream<S, PathBuf>
where
    S: Scope<Timestamp = u64>,
{
    fn durable_backup(&self, backup: Backup, immediate_backup: bool) -> ClockStream<S> {
        let mut op_builder = OperatorBuilder::new("compactor".to_string(), self.scope());
        let mut input = op_builder.new_input(self, Pipeline);
        let (mut output, clock) = op_builder.new_output();

        op_builder.build(move |init_caps| {
            // TODO: EagerNotificator is probably not the best tool here
            //       since the logic is the same, only the eager one is optional
            //       depending on a condition, so a slightly different notificator
            //       might avoid some code duplication and the Rc<RefCell<_>>
            let inbuffer = Rc::new(RefCell::new(InBuffer::<u64, PathBuf>::new()));
            let mut ncater = EagerNotificator::new(init_caps, ());

            // This logic is reused in both eager and closing logic, but only
            // if immediate snapshot is True in the eager one.
            // Takes the inbuffer, empties it and uploads all the paths in the stream.
            let upload = move |inbuffer: Rc<RefCell<InBuffer<u64, PathBuf>>>| {
                Python::with_gil(|py| {
                    let epochs = inbuffer.borrow().epochs().collect::<Vec<_>>();
                    for epoch in epochs {
                        for path in inbuffer.borrow_mut().remove(&epoch).unwrap() {
                            backup
                                .upload(
                                    py,
                                    path.clone(),
                                    path.file_name().unwrap().to_string_lossy().to_string(),
                                )
                                .unwrap();
                        }
                    }
                });
            };

            move |input_frontiers| {
                input.buffer_notify(&mut inbuffer.borrow_mut(), &mut ncater);

                ncater.for_each(
                    input_frontiers,
                    |_caps, ()| {
                        if immediate_backup {
                            upload(inbuffer.clone());
                        }
                    },
                    |caps, ()| {
                        // The inbuffer has been emptied already if immediate_snapshot
                        // is set to True, so we can upload unconditionally here.
                        let cap = &caps[0];
                        upload(inbuffer.clone());
                        output.activate().session(cap).give(());
                    },
                )
            }
        });
        clock
    }
}

pub(crate) trait CompactorOp<S>
where
    S: Scope,
{
    fn compact_snapshots(&self, state_store: Rc<RefCell<StateStore>>) -> Stream<S, PathBuf>;
}

impl<S> CompactorOp<S> for Stream<S, SerializedSnapshot>
where
    S: Scope<Timestamp = u64>,
{
    fn compact_snapshots(&self, state_store: Rc<RefCell<StateStore>>) -> Stream<S, PathBuf> {
        let mut op_builder = OperatorBuilder::new("compactor".to_string(), self.scope());
        let mut input = op_builder.new_input(self, Pipeline);

        let (mut immediate_segments_output, immediate_segments) = op_builder.new_output();
        let (mut batch_segments_output, batch_segments) = op_builder.new_output();
        let segments = immediate_segments.concatenate(vec![batch_segments]);

        let immediate_snapshot = state_store.borrow().immediate_snapshot();

        op_builder.build(move |init_caps| {
            // TODO: EagerNotificator is probably not the best tool here
            //       since the logic is the same, only the eager one is optional
            //       depending on a condition, so a slightly different notificator
            //       might avoid some code duplication and the Rc<RefCell<_>>
            let inbuffer = Rc::new(RefCell::new(InBuffer::new()));
            let mut ncater = EagerNotificator::new(init_caps, ());

            move |input_frontiers| {
                input.buffer_notify(&mut inbuffer.borrow_mut(), &mut ncater);

                ncater.for_each(
                    input_frontiers,
                    |caps, ()| {
                        if immediate_snapshot {
                            let cap = &caps[0];

                            let mut handle = immediate_segments_output.activate();
                            let mut session = handle.session(cap);

                            let epochs = inbuffer.borrow().epochs().collect::<Vec<_>>();
                            let mut state = state_store.borrow_mut();

                            for epoch in epochs {
                                let segment = unwrap_any!(state.open_segment(epoch));
                                let file_name = segment.file_name();
                                let snaps = inbuffer.borrow_mut().remove(&epoch).unwrap();
                                segment.write_snapshots(snaps);
                                session.give(file_name);
                            }
                        }
                    },
                    |caps, ()| {
                        let cap = &caps[1];

                        let mut handle = batch_segments_output.activate();
                        let mut session = handle.session(cap);

                        let epochs = inbuffer.borrow().epochs().collect::<Vec<_>>();
                        let mut state = state_store.borrow_mut();

                        for epoch in epochs {
                            let segment = unwrap_any!(state.open_segment(epoch));
                            let file_name = segment.file_name();
                            let snaps = inbuffer.borrow_mut().remove(&epoch).unwrap();
                            segment.write_snapshots(snaps);
                            session.give(file_name);
                        }
                    },
                )
            }
        });
        segments
    }
}

pub(crate) fn register(_py: Python, m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<RecoveryConfig>()?;
    Ok(())
}
