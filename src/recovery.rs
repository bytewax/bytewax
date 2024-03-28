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
use rusqlite::Transaction;
use rusqlite_migration::Migrations;
use rusqlite_migration::M;
use serde::Deserialize;
use serde::Serialize;
use timely::dataflow::channels::pact::Pipeline;
use timely::dataflow::operators::generic::builder_rc::OperatorBuilder;
use timely::dataflow::operators::Concatenate;
use timely::dataflow::Scope;
use timely::dataflow::Stream;
use timely::Data;

use crate::errors::tracked_err;
use crate::errors::PythonException;
use crate::inputs;
use crate::inputs::BatchResult;
use crate::operators::StatefulBatchLogic;
use crate::outputs;
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

impl Default for Backup {
    fn default() -> Self {
        unwrap_any!(Python::with_gil(|py| {
            get_dummy_backup(py).reraise("Error getting default Backup class")
        }))
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

static BACKUP_MODULE: GILOnceCell<Py<PyModule>> = GILOnceCell::new();

fn get_backup_module(py: Python) -> PyResult<&Bound<'_, PyModule>> {
    Ok(BACKUP_MODULE
        .get_or_try_init(py, || -> PyResult<Py<PyModule>> {
            Ok(py.import_bound("bytewax.backup")?.into())
        })?
        .bind(py))
}

static BACKUP_ABC: GILOnceCell<Py<PyAny>> = GILOnceCell::new();

fn get_backup_abc(py: Python) -> PyResult<&Bound<'_, PyAny>> {
    Ok(BACKUP_ABC
        .get_or_try_init(py, || -> PyResult<Py<PyAny>> {
            Ok(get_backup_module(py)?.getattr("Backup")?.into())
        })?
        .bind(py))
}

static DUMMY_BACKUP: GILOnceCell<Backup> = GILOnceCell::new();

fn get_dummy_backup(py: Python) -> PyResult<Backup> {
    Ok(DUMMY_BACKUP
        .get_or_try_init(py, || -> PyResult<Backup> {
            get_backup_module(py)
                .reraise("Can't find backup module")?
                .getattr("TestingBackup")
                .reraise("Can't find TestingBackup")?
                .call0()
                .reraise("Error init")?
                .extract()
        })?
        .clone())
}

pub(crate) enum StatefulLogicKind {
    Stateful(StatefulBatchLogic),
    Input(inputs::StatefulPartition),
    Output(outputs::StatefulPartition),
}

impl From<StatefulBatchLogic> for StatefulLogicKind {
    fn from(val: StatefulBatchLogic) -> Self {
        StatefulLogicKind::Stateful(val)
    }
}

impl From<inputs::StatefulPartition> for StatefulLogicKind {
    fn from(val: inputs::StatefulPartition) -> Self {
        StatefulLogicKind::Input(val)
    }
}

impl From<outputs::StatefulPartition> for StatefulLogicKind {
    fn from(val: outputs::StatefulPartition) -> Self {
        StatefulLogicKind::Output(val)
    }
}

impl StatefulLogicKind {
    pub(crate) fn snapshot(&self, py: Python) -> PyResult<TdPyAny> {
        match self {
            Self::Stateful(logic) => logic.snapshot(py),
            Self::Input(logic) => logic.snapshot(py),
            Self::Output(logic) => logic.snapshot(py),
        }
    }

    pub(crate) fn as_input(&self) -> Result<&inputs::StatefulPartition, ()> {
        match self {
            Self::Input(logic) => Ok(logic),
            _ => Err(()),
        }
    }

    pub(crate) fn as_unary(&self) -> Result<&StatefulBatchLogic, ()> {
        match self {
            Self::Stateful(logic) => Ok(logic),
            _ => Err(()),
        }
    }

    pub(crate) fn as_output(&self) -> Result<&outputs::StatefulPartition, ()> {
        match self {
            Self::Output(logic) => Ok(logic),
            _ => Err(()),
        }
    }
}

// TODO: Split the responsabilities between different structs, this one
//       started as the state_store_cache but is now doing everything.
pub(crate) struct StateStoreCache {
    flow_id: String,
    worker_index: usize,
    worker_count: usize,
    logics: HashMap<StepId, BTreeMap<StateKey, StatefulLogicKind>>,

    recovery_config: Option<RecoveryConfig>,
    conn: Option<Rc<RefCell<Connection>>>,
    resume_from: ResumeFrom,
    seg_num: u64,
    prev_epoch: u64,
}

/// Trait to group common operations that are used in all stateful operators.
/// It just needs to know how to borrow the StateStoreCache and how to retrieve
/// the step_id to manipulate state and local store.
pub(crate) trait StateManager {
    fn borrow_state(&self) -> Ref<StateStoreCache>;
    fn borrow_state_mut(&self) -> RefMut<StateStoreCache>;
    fn step_id(&self) -> &StepId;

    fn snap(&self, py: Python, key: &StateKey, epoch: &u64) -> PyResult<SerializedSnapshot> {
        self.borrow_state().snap(py, self.step_id(), key, epoch)
    }
    fn write_snapshots(&self, snaps: Vec<SerializedSnapshot>) {
        self.borrow_state().write_snapshots(snaps);
    }
    fn recovery_on(&self) -> bool {
        self.borrow_state().recovery_config.is_some()
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
            .insert(self.step_id().clone(), key, logic.into());
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
    state_store_cache: Rc<RefCell<StateStoreCache>>,
}

impl StateManager for OutputState {
    fn borrow_state(&self) -> Ref<StateStoreCache> {
        self.state_store_cache.borrow()
    }

    fn borrow_state_mut(&self) -> RefMut<StateStoreCache> {
        self.state_store_cache.borrow_mut()
    }

    fn step_id(&self) -> &StepId {
        &self.step_id
    }
}

impl OutputState {
    pub fn hydrate(&mut self, sink: &outputs::FixedPartitionedSink) {
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
                StatefulLogicKind::Output(part)
            };

            self.state_store_cache
                .borrow_mut()
                .hydrate(&self.step_id, builder);
        })
    }

    pub fn new(step_id: StepId, state_store_cache: Rc<RefCell<StateStoreCache>>) -> Self {
        Self {
            step_id,
            state_store_cache,
        }
    }

    pub fn write_batch(&self, py: Python, key: &StateKey, batch: Vec<PyObject>) -> PyResult<()> {
        self.state_store_cache
            .borrow()
            .get(&self.step_id, key)
            .unwrap()
            .as_output()
            .unwrap()
            .write_batch(py, batch)
    }
}

pub(crate) struct InputState {
    step_id: StepId,
    state_store_cache: Rc<RefCell<StateStoreCache>>,
}

impl StateManager for InputState {
    fn borrow_state(&self) -> Ref<StateStoreCache> {
        self.state_store_cache.borrow()
    }

    fn borrow_state_mut(&self) -> RefMut<StateStoreCache> {
        self.state_store_cache.borrow_mut()
    }

    fn step_id(&self) -> &StepId {
        &self.step_id
    }
}

impl InputState {
    pub fn new(step_id: StepId, state_store_cache: Rc<RefCell<StateStoreCache>>) -> Self {
        Self {
            step_id,
            state_store_cache,
        }
    }

    pub fn hydrate(&mut self, source: &inputs::FixedPartitionedSource) {
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
                StatefulLogicKind::Input(part)
            };

            self.state_store_cache
                .borrow_mut()
                .hydrate(&self.step_id, builder);
        })
    }
    pub fn next_batch(&self, py: Python, key: &StateKey) -> PyResult<BatchResult> {
        self.state_store_cache
            .borrow()
            .get(&self.step_id, key)
            .unwrap()
            .as_input()
            .unwrap()
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
        self.state_store_cache
            .borrow()
            .get(&self.step_id, key)
            .unwrap()
            .as_input()
            .unwrap()
            .next_awake(py)
            .reraise_with(|| {
                format!(
                    "error calling \
                    `StatefulSourcePartition.next_awake` in \
                    step {} for partition {}",
                    self.step_id, key
                )
            })
    }
}

pub(crate) struct StatefulBatchState {
    step_id: StepId,
    state_store_cache: Rc<RefCell<StateStoreCache>>,
}

impl StateManager for StatefulBatchState {
    fn borrow_state(&self) -> Ref<StateStoreCache> {
        self.state_store_cache.borrow()
    }

    fn borrow_state_mut(&self) -> RefMut<StateStoreCache> {
        self.state_store_cache.borrow_mut()
    }

    fn step_id(&self) -> &StepId {
        &self.step_id
    }
}

impl StatefulBatchState {
    pub fn new(step_id: StepId, state_store_cache: Rc<RefCell<StateStoreCache>>) -> Self {
        Self {
            step_id,
            state_store_cache,
        }
    }
    pub fn hydrate(&mut self, builder: &TdPyCallable) {
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
            self.state_store_cache
                .borrow_mut()
                .hydrate(&self.step_id, state_builder);
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
            .as_unary()
            .unwrap()
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
        self.state_store_cache
            .borrow()
            .get(&self.step_id, key)
            .unwrap()
            .as_unary()
            .unwrap()
            .on_notify(py)
            .reraise_with(|| {
                format!(
                    "error calling `StatefulBatchLogic.on_notify` in {} for key {}",
                    self.step_id, key
                )
            })
    }
    pub fn notify_at(&self, py: Python, key: &StateKey) -> PyResult<Option<DateTime<Utc>>> {
        if let Some(logic) = self.state_store_cache.borrow().get(&self.step_id, key) {
            logic.as_unary().unwrap().notify_at(py).reraise_with(|| {
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
        self.state_store_cache
            .borrow()
            .get(&self.step_id, key)
            .unwrap()
            .as_unary()
            .unwrap()
            .on_eof(py)
            .reraise_with(|| {
                format!(
                    "error calling `StatefulBatchLogic.on_eof` in {} for key {}",
                    self.step_id, key
                )
            })
    }
}

impl StateStoreCache {
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

        // Wrap the connection into Rc and RefCell so we can share it among operators.
        let conn = conn.map(|conn| Rc::new(RefCell::new(conn)));

        Ok(Self {
            logics: HashMap::new(),
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

    fn add_step(&mut self, step_id: StepId) {
        // We never want to add a step twice, so panic if that's tried.
        assert!(
            !self.logics.contains_key(&step_id),
            "Trying to hydrate state twice, this is probably a bug"
        );
        self.logics.insert(step_id.clone(), Default::default());
    }

    pub fn hydrate(
        &mut self,
        step_id: &StepId,
        builder: impl Fn(&StateKey, Option<PyObject>) -> StatefulLogicKind,
    ) {
        self.add_step(step_id.clone());

        // If no db connection is present, we don't need to do anything else.
        if self.conn.is_none() {
            return;
        }

        // Get all the snapshots in the store for this specific step_id,
        // deserialize them, then call the builder function to make them
        // the right StatefulLogicKind variant.
        let deserialized_snaps: Vec<_> = Python::with_gil(|py| {
            let pickle = unwrap_any!(py.import_bound("pickle"));
            self.conn
                .as_ref()
                .unwrap()
                .borrow_mut()
                .prepare(
                    "SELECT step_id, state_key, epoch, ser_change
                FROM snaps
                WHERE epoch = ?1 AND step_id = ?2
                ",
                )
                .unwrap()
                .query_map((self.resume_from.1 .0, &step_id.0), |row| {
                    Ok(SerializedSnapshot(
                        StepId(row.get(0)?),
                        StateKey(row.get(1)?),
                        SnapshotEpoch(row.get(2)?),
                        row.get(3)?,
                    ))
                })
                .unwrap()
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
                .collect()
        });

        for (state_key, state) in deserialized_snaps {
            self.insert(
                step_id.clone(),
                state_key.clone(),
                builder(&state_key, state),
            );
        }
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

    pub fn get(&self, step_id: &StepId, key: &StateKey) -> Option<&StatefulLogicKind> {
        self.logics.get(step_id).and_then(|s| s.get(key))
    }

    pub fn get_logics(&self, step_id: &StepId) -> &BTreeMap<StateKey, StatefulLogicKind> {
        self.logics.get(step_id).unwrap()
    }

    pub fn contains_key(&self, step_id: &StepId, key: &StateKey) -> bool {
        self.logics.get(step_id).unwrap().contains_key(key)
    }

    pub fn insert(&mut self, step_id: StepId, key: StateKey, logic: StatefulLogicKind) {
        self.logics.entry(step_id).or_default().insert(key, logic);
    }

    pub fn remove(&mut self, step_id: &StepId, key: &StateKey) -> Option<StatefulLogicKind> {
        self.logics.get_mut(step_id).and_then(|s| s.remove(key))
    }

    fn ser_snap(
        &self,
        py: Python,
        step_id: StepId,
        key: StateKey,
        snap_change: StateChange,
        epoch: u64,
    ) -> PyResult<SerializedSnapshot> {
        let ser_change = match snap_change {
            StateChange::Upsert(snap) => {
                let snap = PyObject::from(snap);
                let pickle = py.import_bound("pickle").unwrap();
                let ser_snap = pickle
                    .call_method1(intern!(py, "dumps"), (snap.bind(py),))
                    .unwrap()
                    .downcast::<PyBytes>()
                    .unwrap()
                    .as_bytes()
                    .to_vec();
                Some(ser_snap)
            }
            StateChange::Discard => None,
        };

        let snap_epoch = SnapshotEpoch(epoch);
        Ok(SerializedSnapshot(step_id, key, snap_epoch, ser_change))
    }

    pub(crate) fn write_snapshots(&self, snaps: Vec<SerializedSnapshot>) {
        let mut conn = self.conn.as_ref().unwrap().borrow_mut();
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

    /// Generates and returns a fully serialized snapshot
    pub fn snap(
        &self,
        py: Python,
        step_id: &StepId,
        key: &StateKey,
        epoch: &u64,
    ) -> PyResult<SerializedSnapshot> {
        let change = if let Some(logic) = self.get(step_id, key) {
            let state = logic.snapshot(py).reraise_with(|| {
                format!("error calling `StatefulBatchLogic.snapshot` in {step_id} for key {key}")
            })?;
            StateChange::Upsert(state)
        } else {
            // It's ok if there's no logic, because during this epoch it might have been discarded
            // due to one of the `on_*` methods returning `IsComplete::Discard`.
            StateChange::Discard
        };
        self.ser_snap(py, step_id.clone(), key.clone(), change.clone(), *epoch)
    }

    pub fn open_segment(&mut self, epoch: u64) -> (Connection, PathBuf) {
        let worker_index = self.worker_index;
        let ex_num = self.resume_from.0;
        if epoch != self.prev_epoch {
            self.seg_num = 1;
            self.prev_epoch = epoch;
        } else {
            self.seg_num += 1;
        }
        let seg_num = self.seg_num;

        let file_name =
            format!("ex-{ex_num}:epoch-{epoch}:segment-{seg_num}:_:worker-{worker_index}.sqlite3");
        let mut path = self.recovery_config.as_ref().unwrap().db_dir.clone();
        path.push(file_name);
        let mut conn = Connection::open_with_flags(
            path.clone(),
            OpenFlags::SQLITE_OPEN_READ_WRITE
                | OpenFlags::SQLITE_OPEN_CREATE
                | OpenFlags::SQLITE_OPEN_NO_MUTEX,
        )
        .reraise("can't open recovery DB")
        .unwrap();
        rusqlite::vtab::series::load_module(&conn).unwrap();
        conn.pragma_update(None, "foreign_keys", "ON").unwrap();
        conn.pragma_update(None, "journal_mode", "WAL").unwrap();
        conn.pragma_update(None, "busy_timeout", "5000").unwrap();
        Python::with_gil(|py| {
            get_migrations(py).to_latest(&mut conn).unwrap();
        });
        (conn, path)
    }

    pub fn frontier_query(&self, txn: &Transaction, epoch: u64) -> PyResult<()> {
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
        .reraise("Error initing transaction")
        .map(|_| ())
    }

    pub fn write_frontier(&mut self, epoch: u64) -> PyResult<()> {
        if let Some(conn) = self.conn.as_ref() {
            let mut borrowed_conn = conn.borrow_mut();
            let txn = borrowed_conn.transaction().unwrap();
            tracing::trace!("Writing epoch {epoch:?}");
            self.frontier_query(&txn, epoch)?;
            txn.commit().reraise("Error committing cluster_frontier")
        } else {
            Err(tracked_err::<PyRuntimeError>(
                "DB Connection lost before writing frontier",
            ))
        }
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

/// Each operator's state is modeled as as key-value store, with
/// [`StateKey`] being the key, and this enum representing changes to
/// the value.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) enum StateChange {
    /// Value was updated.
    Upsert(TdPyAny),
    /// Key was deleted.
    Discard,
}

/// The snapshot of state for a key in an operator.
///
/// This is the API that stateful operators must adhere to: emit these
/// downstream at the end of every epoch, and load them on resume.
///
/// The epoch is stored by the recovery machinery and is not part of
/// the operator's API.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct Snapshot(
    pub(crate) StepId,
    pub(crate) StateKey,
    pub(crate) StateChange,
);

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
                get_dummy_backup(py).reraise("Error getting default Backup class")
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

        let mut conn = Connection::open_with_flags(
            path,
            OpenFlags::SQLITE_OPEN_READ_WRITE
                | OpenFlags::SQLITE_OPEN_CREATE
                | OpenFlags::SQLITE_OPEN_NO_MUTEX,
        )
        .reraise("can't open recovery DB")?;
        rusqlite::vtab::series::load_module(&conn).unwrap();
        conn.pragma_update(None, "foreign_keys", "ON").unwrap();
        conn.pragma_update(None, "journal_mode", "WAL").unwrap();
        conn.pragma_update(None, "busy_timeout", "5000").unwrap();
        Python::with_gil(|py| get_migrations(py).to_latest(&mut conn))
            .reraise("Error running migrations on the db")?;
        Ok(conn)
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

trait FrontierOp<S, D>
where
    S: Scope,
    D: Data,
{
    /// Emit downstream this worker's current frontier.
    ///
    /// Although the [`ExecutionNumber`] and [`WorkerIndex`] are both
    /// already within the [`FrontierMeta`], duplicate them in the key
    /// position so we can partition and route on them.
    ///
    /// The emit happens just before the frontier advances, and thus
    /// is actually within the previous epoch.
    ///
    /// Doesn't emit the "empty frontier" (even though that is the
    /// true frontier) on dataflow termination to allow dataflow
    /// continuation.
    fn frontier(
        &self,
        resume_from: ResumeFrom,
    ) -> Stream<S, ((ExecutionNumber, WorkerIndex), FrontierMeta)>;
}

impl<S, D> FrontierOp<S, D> for Stream<S, D>
where
    S: Scope<Timestamp = u64>,
    D: Data,
{
    fn frontier(
        &self,
        resume_from: ResumeFrom,
    ) -> Stream<S, ((ExecutionNumber, WorkerIndex), FrontierMeta)> {
        let worker_index = self.scope().w_index();
        let ResumeFrom(ex_num, resume_epoch) = resume_from;

        // We can't use a notificator for progress because it's
        // possible a worker, due to partitioning, will have no input
        // data. We still need to write out that the worker made it
        // through the resume epoch in that case.
        let name = String::from("worker_frontier");
        let mut op_builder = OperatorBuilder::new(name.clone(), self.scope());

        let mut input = op_builder.new_input(self, Pipeline);

        let (mut frontiers_output, frontiers) = op_builder.new_output();

        // Sort of "emit at end of epoch" but Timely doesn't give us
        // that.
        op_builder.build(move |mut init_caps| {
            // Since we might emit downstream without any incoming
            // items, like reporting progress on EOF, ensure we FFWD
            // to the resume epoch.
            init_caps.downgrade_all(&resume_epoch.0);
            let mut cap = init_caps.pop();

            let mut inbuf = Vec::new();

            move |input_frontiers| {
                tracing::debug_span!("operator", operator = name).in_scope(|| {
                    input.for_each(|_cap, incoming| {
                        assert!(inbuf.is_empty());
                        incoming.swap(&mut inbuf);
                        // We have to drain the incoming data, but we just
                        // care about the epoch so drop it.
                        inbuf.clear();
                    });

                    cap = cap.take().and_then(|cap| {
                        let frontier = input_frontiers.simplify();
                        // EOF counts as progress. This will also filter
                        // out the flash of 0 epoch upon resume.
                        let frontier_progressed = frontier.map_or(true, |f| f > *cap.time());
                        if frontier_progressed {
                            // There's no way to guarantee that "last
                            // frontier + 1" is actually the resume epoch
                            // on the next execution, but mark that this
                            // worker is ready to resume there on
                            // EOF. It's also possible that this results
                            // in a "too small" resume epoch: if for some
                            // reason this operator isn't activated during
                            // every epoch, we might miss the "largest"
                            // epoch and so we'll mark down the resume
                            // epoch as one too small. That's fine, we
                            // just might resume further back than is
                            // optimal.
                            let frontier_epoch = frontier.unwrap_or(*cap.time() + 1);
                            let front =
                                FrontierMeta(ex_num, worker_index, ClusterFrontier(frontier_epoch));
                            tracing::trace!("Frontier now epoch {frontier_epoch:?}");
                            let key = (ex_num, worker_index);

                            // Do not delay cap before the write; we will
                            // delay downstream progress messages longer than
                            // necessary if so. This would manifest as
                            // resuming from an epoch that seems "too
                            // early". Write out the progress at the end of
                            // each epoch and where the frontier has moved.
                            frontiers_output.activate().session(&cap).give((key, front));

                            // If EOF, drop caps after the write.
                            frontier.map(|f| {
                                // We should never delay to something like
                                // frontier + 1, otherwise chained progress
                                // operators will "drift" forward and GC will
                                // happen too early. If the frontier is empty,
                                // also drop the capability.
                                cap.delayed(&f)
                            })
                        // If there was no frontier progress on this
                        // awake, maintain the current cap and do nothing.
                        } else {
                            Some(cap)
                        }
                    });
                });
            }
        });

        frontiers
    }
}

pub(crate) trait CompactFrontiersOp<S>
where
    S: Scope,
{
    fn compact_frontiers(&self, state_store_cache: Rc<RefCell<StateStoreCache>>) -> ClockStream<S>;
}

impl<S> CompactFrontiersOp<S> for ClockStream<S>
where
    S: Scope<Timestamp = u64>,
{
    fn compact_frontiers(&self, state_store_cache: Rc<RefCell<StateStoreCache>>) -> ClockStream<S> {
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
                        unwrap_any!(state_store_cache.borrow_mut().write_frontier(*epoch));
                        // And finally allow the dataflow to advance its epoch.
                        output.activate().session(cap).give(());
                    },
                )
            }
        });
        clock
    }
}

pub(crate) trait FrontierSegmentOp<S>
where
    S: Scope,
{
    fn frontier_segment(
        &self,
        state_store_cache: Rc<RefCell<StateStoreCache>>,
    ) -> Stream<S, PathBuf>;
}

impl<S> FrontierSegmentOp<S> for ClockStream<S>
where
    S: Scope<Timestamp = u64>,
{
    fn frontier_segment(
        &self,
        state_store_cache: Rc<RefCell<StateStoreCache>>,
    ) -> Stream<S, PathBuf> {
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
                        for epoch in epochs {
                            let (mut conn, file_name) =
                                state_store_cache.borrow_mut().open_segment(epoch);
                            let txn = conn.transaction().unwrap();
                            inbuffer.remove(&epoch);
                            state_store_cache
                                .borrow()
                                .frontier_query(&txn, epoch)
                                .unwrap();
                            txn.commit().unwrap();
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
            // TODO: EagerNotificator is probably not the right tool here
            //       since the logic is the same, only the eager one is optional
            //       depending on a condition, so a slightly different notificator
            //       might avoid some code duplication and the Rc<RefCell<_>>
            let inbuffer = Rc::new(RefCell::new(InBuffer::new()));
            let mut ncater = EagerNotificator::new(init_caps, ());

            move |input_frontiers| {
                input.buffer_notify(&mut inbuffer.borrow_mut(), &mut ncater);

                ncater.for_each(
                    input_frontiers,
                    |_caps, ()| {
                        if immediate_backup {
                            // Do the upload
                            Python::with_gil(|py| {
                                let epochs = inbuffer.borrow().epochs().collect::<Vec<_>>();
                                for epoch in epochs {
                                    for path in inbuffer.borrow_mut().remove(&epoch).unwrap() {
                                        backup
                                            .upload(py, path.clone(), "test_key".to_string())
                                            .unwrap();
                                    }
                                }
                            });
                        }
                    },
                    |caps, ()| {
                        let cap = &caps[0];
                        // Do the upload
                        Python::with_gil(|py| {
                            let epochs = inbuffer.borrow().epochs().collect::<Vec<_>>();
                            for epoch in epochs {
                                for path in inbuffer.borrow_mut().remove(&epoch).unwrap() {
                                    backup
                                        .upload(py, path.clone(), "test_key".to_string())
                                        .unwrap();
                                }
                            }
                        });
                        // And then
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
    fn compact_snapshots(
        &self,
        state_store_cache: Rc<RefCell<StateStoreCache>>,
    ) -> Stream<S, PathBuf>;
}

impl<S> CompactorOp<S> for Stream<S, SerializedSnapshot>
where
    S: Scope<Timestamp = u64>,
{
    fn compact_snapshots(
        &self,
        state_store_cache: Rc<RefCell<StateStoreCache>>,
    ) -> Stream<S, PathBuf> {
        let mut op_builder = OperatorBuilder::new("compactor".to_string(), self.scope());
        let mut input = op_builder.new_input(self, Pipeline);

        let (mut immediate_segments_output, immediate_segments) = op_builder.new_output();
        let (mut batch_segments_output, batch_segments) = op_builder.new_output();
        let segments = immediate_segments.concatenate(vec![batch_segments]);

        let immediate_snapshot = state_store_cache.borrow().immediate_snapshot();

        op_builder.build(move |init_caps| {
            // TODO: EagerNotificator is probably not the right tool here
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
                            for epoch in epochs {
                                let (mut conn, file_name) =
                                    state_store_cache.borrow_mut().open_segment(epoch);
                                let txn = conn.transaction().unwrap();
                                for snap in inbuffer.borrow_mut().remove(&epoch).unwrap() {
                                    let SerializedSnapshot(
                                        step_id,
                                        state_key,
                                        snap_epoch,
                                        ser_change,
                                    ) = snap;
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
                                session.give(file_name);
                            }
                        }
                    },
                    |caps, ()| {
                        let cap = &caps[1];

                        let mut handle = batch_segments_output.activate();
                        let mut session = handle.session(cap);

                        let epochs = inbuffer.borrow().epochs().collect::<Vec<_>>();
                        for epoch in epochs {
                            let (mut conn, file_name) =
                                state_store_cache.borrow_mut().open_segment(epoch);
                            let txn = conn.transaction().unwrap();
                            for snap in inbuffer.borrow_mut().remove(&epoch).unwrap() {
                                let SerializedSnapshot(step_id, state_key, snap_epoch, ser_change) =
                                    snap;
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
