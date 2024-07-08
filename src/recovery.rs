//! Internal code for implementing recovery.
//!
//! For a user-centric version of recovery, read the
//! `bytewax.recovery` Python module docstring. Read that first.

use std::cell::RefCell;
use std::collections::BTreeMap;
use std::collections::HashMap;
use std::fmt;
use std::fmt::Debug;
use std::hash::Hash;
use std::ops::Deref;
use std::path::Path;
use std::path::PathBuf;
use std::rc::Rc;

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
use crate::timely::*;

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

    pub(crate) fn download(&self, py: Python, from_key: &String, to_local: &Path) -> PyResult<()> {
        self.0
            .call_method_bound(py, intern!(py, "download"), (from_key, to_local), None)?;
        Ok(())
    }

    pub(crate) fn delete(&self, py: Python, key: String) -> PyResult<()> {
        self.0.call_method1(py, intern!(py, "delete"), (key,))?;
        Ok(())
    }
}

type LogicBuilder = Box<dyn Fn(Python, StateKey, Option<PyObject>) -> PyResult<PyObject>>;

/// Stores that state for all the stateful operators.
/// This is basically a wrapper around a HashMap that's
/// shared between all stateful operators.
// TODO: To implement larger than memory state handling,
//       this could become a trait with two implementation:
//       one that behaves like this, and another one which
//       requires a connection to the local state store and
//       can load and offload data from there.
pub(crate) struct StateStoreCache {
    cache: HashMap<StepId, BTreeMap<StateKey, PyObject>>,
    builders: HashMap<StepId, LogicBuilder>,
}

impl StateStoreCache {
    pub fn new() -> Self {
        Self {
            cache: HashMap::new(),
            builders: HashMap::new(),
        }
    }

    pub fn add_step(&mut self, step_id: StepId, builder: LogicBuilder) {
        self.cache.insert(step_id.clone(), Default::default());
        self.builders.insert(step_id, builder);
    }

    pub fn contains_key(&self, step_id: &StepId, key: &StateKey) -> bool {
        self.cache.get(step_id).unwrap().contains_key(key)
    }

    pub fn insert(&mut self, py: Python, step_id: &StepId, key: StateKey, state: Option<PyObject>) {
        let logic = (self.builders.get(step_id).unwrap())(py, key.clone(), state).unwrap();
        self.cache.get_mut(step_id).unwrap().insert(key, logic);
    }

    pub fn get(&self, step_id: &StepId, key: &StateKey) -> Option<&PyObject> {
        self.cache.get(step_id).unwrap().get(key)
    }

    pub fn keys(&self, step_id: &StepId) -> impl Iterator<Item = &StateKey> {
        self.cache.get(step_id).unwrap().keys()
    }

    pub fn remove(&mut self, step_id: &StepId, key: &StateKey) -> Option<PyObject> {
        self.cache.get_mut(step_id).unwrap().remove(key)
    }

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
                let snap = logic
                    .call_method0(py, intern!(py, "snapshot"))
                    .reraise_with(|| {
                        format!("error calling `snapshot` in {} for key {}", step_id, key)
                    })?;

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

        Ok(SerializedSnapshot::new(step_id, key, epoch, ser_change))
    }
}

/// Stores that state for all the stateful operators.
/// Offers an api to interact with the state of each step_id,
/// to generate snapshot of each state and
/// to manage the connection to the local db where the snapshots are saved.
pub(crate) struct LocalStateStore {
    resume_from: ResumeFrom,
    conn: Connection,
    db_dir: PathBuf,
    worker_index: usize,
    backup: Backup,
    snapshot_mode: SnapshotMode,

    // Writers for snapshots and frontiers
    frontier_writer: FrontierWriter,
    snapshot_writer: SnapshotWriter,

    // Segment handling internal data
    current_epoch: u64,
    seg_num: u64,
}

impl LocalStateStore {
    pub fn new(
        flow_id: String,
        worker_index: usize,
        worker_count: usize,
        recovery_config: &Bound<'_, RecoveryConfig>,
    ) -> PyResult<Self> {
        // Set local_state_dir and open main connection to the db.
        let local_state_dir = recovery_config.borrow().local_state_dir.clone();
        if !local_state_dir.is_dir() {
            return Err(PyFileNotFoundError::new_err(format!(
                "local state directory {:?} does not exist; \
                see the `bytewax.recovery` module docstring for more info",
                local_state_dir
            )));
        }

        // Calculate resume_from.
        // First we need to retrieve the latest frontier segment from the durable store,
        // as we never store frontier info in the local store.
        // Start with the default value, then if the durable store does have a frontier
        // segment, update it with the fetched value.
        let mut resume_from = ResumeFrom::default();

        let backup = recovery_config.borrow().backup.clone();
        let snapshot_mode = recovery_config.borrow().snapshot_mode;
        // `keys` is the list of files in the durable store
        let keys = Python::with_gil(|py| backup.list_keys(py).unwrap());
        let mut frontier_segments: Vec<(u64, u64, String)> = keys
            .into_iter()
            // We are only interested in frontier segments, the convention is
            // that the name of those starts with `frontier:`
            .filter(|key| key.starts_with("frontier:"))
            .filter_map(|key| {
                // The filename is composed this way:
                // "frontier:ex-{ex_num}:epoch-{epoch}:_:worker-{worker_index}.sqlite3"
                // So strip the extension first:
                let split: Vec<&str> = key.strip_suffix(".sqlite3").unwrap().split(':').collect();

                // Filter current worker's segments
                let worker = split[4]
                    .strip_prefix("worker-")
                    .unwrap()
                    .parse::<usize>()
                    .unwrap();
                if worker != worker_index {
                    return None;
                }

                // Extract execution_number and epoch
                let ex_num = split[1]
                    .strip_prefix("ex-")
                    .unwrap()
                    .parse::<u64>()
                    .unwrap();
                let epoch = split[2]
                    .strip_prefix("epoch-")
                    .unwrap()
                    .parse::<u64>()
                    .unwrap();
                Some((ex_num, epoch, key))
            })
            .collect();
        frontier_segments.sort_by_key(|&(ex_num, epoch, _)| (ex_num, epoch));

        if let Some((_, _, frontier_segment)) = frontier_segments.last() {
            let segment_file = local_state_dir.join(frontier_segment);
            Python::with_gil(|py| {
                backup
                    .download(py, frontier_segment, segment_file.as_path())
                    .unwrap()
            });
            let conn = setup_conn(segment_file)?;
            let res = conn.query_row(
                "SELECT ex_num, cluster_frontier, worker_count, worker_index \
                FROM meta \
                WHERE (ex_num, flow_id) IN (SELECT MAX(ex_num), flow_id FROM meta)",
                (),
                |row| {
                    let ex_num = row.get::<_, Option<u64>>(0)?.map(ExecutionNumber);
                    let resume_epoch = row.get::<_, Option<u64>>(1)?.map(ResumeEpoch);
                    let worker_count = row.get::<_, Option<usize>>(2)?.map(WorkerCount);
                    Ok((
                        ex_num
                            .zip(resume_epoch)
                            // Advance the execution number here.
                            .map(|(en, re)| ResumeFrom::new(en.next(), re)),
                        worker_count,
                    ))
                },
            );
            match res {
                // If no rows in the db, it was empty, so use the default.
                Err(rusqlite::Error::QueryReturnedNoRows) => resume_from = ResumeFrom::default(),
                // Any other error was an error reading from the db, we should stop here.
                Err(err) => std::panic::panic_any(err),
                Ok((resume, w_count)) => {
                    resume_from = resume.unwrap_or_default();
                    // TODO!
                    if w_count.unwrap().0 != worker_count {
                        panic!("Rescaling not supported yet!");
                    }
                }
            };
            tracing::info!(
                "Resuming from execution: {}, epoch {}",
                resume_from.execution(),
                resume_from.epoch()
            );
        }

        let frontier_writer = FrontierWriter::new(
            flow_id.clone(),
            resume_from.execution(),
            worker_index,
            worker_count,
        );

        let file_name = format!("flow_{flow_id}_worker_{worker_index}.sqlite3");
        // Now we need to check two conditions.
        // The durable store tells us if this is the first execution.
        // If that's not the case, but the file does not exist, we need
        // to do a full resume.
        let is_first_execution = resume_from.execution().0 == 0;
        let db_exists = local_state_dir.join(file_name.clone()).exists();
        // TODO: full resume is not implemented yet, so we crash here.
        if !is_first_execution && !db_exists {
            return Err(PyErr::new::<PyRuntimeError, _>("Missing db file."));
        }
        // The other case is if this is the first execution, but the file is present.
        // This is not ok, but we don't want to automatically remove the file,
        // so crash and ask the user to take action instead.
        if is_first_execution && db_exists {
            return Err(PyErr::new::<PyRuntimeError, _>(format!(
                "A file with the name '{}' already exists in '{}', \
                but durable backup indicates that this is the first execution. \
                Please rename or remove the file before running the dataflow.",
                file_name,
                local_state_dir.to_string_lossy()
            )));
        }

        // Finally open or create the file.
        let conn = setup_conn(local_state_dir.join(file_name))?;

        Ok(Self {
            conn,
            db_dir: local_state_dir,
            backup,
            current_epoch: resume_from.epoch().0,
            worker_index,
            frontier_writer,
            snapshot_writer: SnapshotWriter {},
            resume_from,
            snapshot_mode,
            seg_num: 0,
        })
    }

    pub fn snapshot_mode(&self) -> &SnapshotMode {
        &self.snapshot_mode
    }

    pub fn resume_from_epoch(&self) -> ResumeEpoch {
        self.resume_from.epoch()
    }

    pub fn update_resume_epoch(&mut self, resume_epoch: ResumeEpoch) {
        self.resume_from.1 = resume_epoch;
    }

    pub fn backup(&self) -> Backup {
        // TODO: use clone_ref and Py<Backup> here?
        self.backup.clone()
    }

    /// Hydrate the local cache with all the snapshots for a step_id.
    /// Pass a builder function that turns a `(state_key, state)` tuple
    /// into a `StatefulLogicKind`, and it will be called with data coming
    /// from each deserialized snapshot.
    pub fn get_snaps(
        &self,
        py: Python,
        step_id: &StepId,
    ) -> PyResult<Vec<(StateKey, Option<PyObject>)>> {
        // Get all the snapshots in the store for this specific step_id,
        // and deserialize them.
        let pickle = py.import_bound("pickle")?;
        self.conn
            // Retrieve all the snapshots for the latest epoch saved
            // in the local store that's <= than resume_from.
            .prepare(
                "SELECT step_id, state_key, MAX(epoch) as epoch, ser_change \
                FROM snaps \
                WHERE epoch <= ?1 AND step_id = ?2 \
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
            .reraise("Error binding query parameters in local state store")?
            .map(|res| res.expect("Error unpacking SerializedSnapshot"))
            .map(|SerializedSnapshot(_, key, _, ser_state)| {
                let state = ser_state.map_or_else(
                    || Ok::<Option<PyObject>, PyErr>(None),
                    |ser_state| {
                        let state = pickle
                            .call_method1(
                                intern!(py, "loads"),
                                (PyBytes::new_bound(py, &ser_state),),
                            )?
                            .unbind();
                        Ok(Some(state))
                    },
                )?;
                Ok((key, state))
            })
            .collect()
    }

    pub(crate) fn write_snapshots_segment(
        &mut self,
        snaps: Vec<SerializedSnapshot>,
        epoch: u64,
    ) -> PyResult<PathBuf> {
        if epoch >= self.current_epoch {
            self.current_epoch = epoch;
            self.seg_num = 1;
        } else {
            self.seg_num += 1;
        }
        let file_name = format!(
            "ex-{}:epoch-{}:segment-{}:_:worker-{}.sqlite3",
            self.resume_from.execution(),
            epoch,
            self.seg_num,
            self.worker_index
        );
        let path = self.db_dir.join(file_name);
        let mut conn = setup_conn(path.clone())?;
        self.snapshot_writer.write_batch(&mut conn, snaps)?;
        Ok(path)
    }

    pub(crate) fn write_frontier_segment(&mut self, epoch: u64) -> PyResult<PathBuf> {
        let file_name = format!(
            "frontier:ex-{}:epoch-{}:_:worker-{}.sqlite3",
            self.resume_from.execution(),
            epoch,
            self.worker_index
        );
        let path = self.db_dir.join(file_name);
        let conn = setup_conn(path.clone())?;
        self.frontier_writer.write(&conn, epoch)?;
        Ok(path)
    }

    /// Write a vec of serialized snapshots to the local db.
    pub(crate) fn write_snapshots(&mut self, snaps: Vec<SerializedSnapshot>) {
        self.snapshot_writer
            .write_batch(&mut self.conn, snaps)
            .unwrap();
    }
}

fn setup_conn(path: PathBuf) -> PyResult<Connection> {
    let mut conn = Connection::open_with_flags(
        path,
        OpenFlags::SQLITE_OPEN_READ_WRITE
            | OpenFlags::SQLITE_OPEN_CREATE
            | OpenFlags::SQLITE_OPEN_NO_MUTEX,
    )
    .reraise("can't open DB")?;
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

/// Defines a component that can write an item or a batch of items into a db.
pub(crate) trait DbWriter {
    /// Item type to be able to write.
    type Item;

    /// Write a single item to the db
    fn write(&self, conn: &Connection, item: Self::Item) -> PyResult<()>;

    /// Write a batch of items. This is automatically put into a transaction
    /// wrapping the function to write a single item.
    fn write_batch(&self, conn: &mut Connection, items: Vec<Self::Item>) -> PyResult<()> {
        let txn = conn.transaction().reraise("Recovery db error")?;
        let conn = txn.deref();
        for item in items {
            self.write(conn, item)?;
        }
        txn.commit().reraise("Recovery db error")?;
        Ok(())
    }
}

struct SnapshotWriter {}

impl DbWriter for SnapshotWriter {
    type Item = SerializedSnapshot;

    fn write(&self, conn: &Connection, item: Self::Item) -> PyResult<()> {
        let SerializedSnapshot(step_id, state_key, snap_epoch, ser_change) = item;
        conn.execute(
            "INSERT INTO snaps (step_id, state_key, epoch, ser_change) \
            VALUES (?1, ?2, ?3, ?4) \
            ON CONFLICT (step_id, state_key, epoch) DO UPDATE \
            SET ser_change = EXCLUDED.ser_change",
            (step_id.0, state_key.0, snap_epoch.0, ser_change),
        )
        .reraise("Recovery db error")?;
        Ok(())
    }
}

struct FrontierWriter {
    flow_id: String,
    ex_num: ExecutionNumber,
    worker_index: usize,
    worker_count: usize,
}

impl FrontierWriter {
    pub fn new(
        flow_id: String,
        ex_num: ExecutionNumber,
        worker_index: usize,
        worker_count: usize,
    ) -> Self {
        Self {
            flow_id,
            ex_num,
            worker_index,
            worker_count,
        }
    }
}

impl DbWriter for FrontierWriter {
    type Item = u64;

    fn write(&self, conn: &Connection, epoch: u64) -> PyResult<()> {
        conn.execute(
            "INSERT INTO meta (flow_id, ex_num, worker_index, worker_count, cluster_frontier) \
            VALUES (?1, ?2, ?3, ?4, ?5) \
            ON CONFLICT (flow_id, ex_num, worker_index) DO UPDATE \
            SET cluster_frontier = EXCLUDED.cluster_frontier",
            (
                &self.flow_id,
                self.ex_num.0,
                self.worker_index,
                self.worker_count,
                epoch,
            ),
        )
        .reraise("Error initing transaction")?;
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct FlowId(String);

/// Incrementing ID representing how many times a dataflow has been
/// executed to completion or failure.
///
/// This is used to ensure recovery progress information for a worker
/// `3` is not mis-interpreted to belong to an earlier cluster.
///
/// As you resume a dataflow, this will increase by 1 each time.
#[derive(Debug, Copy, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub(crate) struct ExecutionNumber(pub(crate) u64);

impl ExecutionNumber {
    pub(crate) fn next(self) -> Self {
        Self(self.0 + 1)
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

/// To resume a dataflow execution, you need to know which epoch to
/// resume for state, but also which execution to label progress data
/// with.
///
/// This does not define [`Default`] and should only be calculated via
/// [`ResumeCalc::resume_from`].
#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub(crate) struct ResumeFrom(ExecutionNumber, ResumeEpoch);

impl ResumeFrom {
    pub fn new(ex_num: ExecutionNumber, resume_epoch: ResumeEpoch) -> Self {
        Self(ex_num, resume_epoch)
    }

    pub fn execution(&self) -> ExecutionNumber {
        self.0
    }
    pub fn epoch(&self) -> ResumeEpoch {
        self.1
    }
}

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

impl SerializedSnapshot {
    pub fn new(step_id: StepId, state_key: StateKey, epoch: u64, state: Option<Vec<u8>>) -> Self {
        Self(step_id, state_key, SnapshotEpoch(epoch), state)
    }
}

impl fmt::Display for SerializedSnapshot {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_fmt(format_args!("{}: {}, epoch {}", self.0, self.1, self.2 .0))
    }
}

#[pyclass(module = "bytewax.recovery")]
#[derive(Copy, Clone, Debug)]
pub enum SnapshotMode {
    Immediate,
    Batch,
}

impl SnapshotMode {
    pub fn immediate(&self) -> bool {
        matches!(self, Self::Immediate)
    }

    pub fn batch(&self) -> bool {
        matches!(self, Self::Batch)
    }
}

/// Configuration settings for recovery.
///
/// :arg local_state_dir: Local filesystem directory to use for recovery
///     database partitions.
///
/// :type local_state_dir: pathlib.Path
///
/// :arg backup: Class to use to save recovery files to a durable
///     storage like amazon's S3.
///
/// :type backup: bytewax.backup.Backup
///
/// :arg snapshot_mode: Whether to take state snapshots at the end
///     of the epoch (SnapshotMode.Batch), or as soon as a change
///     happens (SnapshotMode.Immediate).
///     Defaults to SnapshotMode.Immediate.
///
/// :type snapshot_mode: SnapshotMode
#[pyclass(module = "bytewax.recovery")]
#[derive(Clone, Debug)]
pub(crate) struct RecoveryConfig {
    #[pyo3(get)]
    pub(crate) local_state_dir: PathBuf,
    #[pyo3(get)]
    pub(crate) backup: Backup,
    #[pyo3(get)]
    pub(crate) snapshot_mode: SnapshotMode,
}

#[pymethods]
impl RecoveryConfig {
    #[new]
    fn new(
        local_state_dir: PathBuf,
        backup: Backup,
        snapshot_mode: Option<SnapshotMode>,
    ) -> PyResult<Self> {
        let snapshot_mode = snapshot_mode.unwrap_or(SnapshotMode::Immediate);
        Ok(Self {
            local_state_dir,
            snapshot_mode,
            backup,
        })
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

pub(crate) trait BackupOp<S>
where
    S: Scope,
{
    fn backup(&self, backup: Backup) -> ClockStream<S>;
}

impl<S> BackupOp<S> for Stream<S, PathBuf>
where
    S: Scope<Timestamp = u64>,
{
    fn backup(&self, backup: Backup) -> ClockStream<S> {
        let mut op_builder = OperatorBuilder::new("backup".to_string(), self.scope());
        let mut input = op_builder.new_input(self, Pipeline);
        let (mut output, clock) = op_builder.new_output();

        op_builder.build(move |init_caps| {
            let inbuffer = Rc::new(RefCell::new(InBuffer::<u64, PathBuf>::new()));
            let mut ncater = EagerNotificator::new(init_caps, ());

            // This logic is reused in both eager and closing logic.
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
                        upload(inbuffer.clone());
                    },
                    |caps, ()| {
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
    fn compactor(&self, local_state_store: Rc<RefCell<LocalStateStore>>) -> Stream<S, PathBuf>;
}

impl<S> CompactorOp<S> for Stream<S, SerializedSnapshot>
where
    S: Scope<Timestamp = u64>,
{
    fn compactor(&self, local_state_store: Rc<RefCell<LocalStateStore>>) -> Stream<S, PathBuf> {
        let mut op_builder = OperatorBuilder::new("compactor".to_string(), self.scope());
        let mut input = op_builder.new_input(self, Pipeline);

        let (mut immediate_segments_output, immediate_segments) = op_builder.new_output();
        let (mut batch_segments_output, batch_segments) = op_builder.new_output();
        let segments = immediate_segments.concatenate(vec![batch_segments]);

        op_builder.build(move |init_caps| {
            let inbuffer = Rc::new(RefCell::new(InBuffer::new()));
            let mut ncater = EagerNotificator::new(init_caps, ());

            move |input_frontiers| {
                input.buffer_notify(&mut inbuffer.borrow_mut(), &mut ncater);

                ncater.for_each(
                    input_frontiers,
                    |caps, ()| {
                        let cap = &caps[0];

                        let mut handle = immediate_segments_output.activate();
                        let mut session = handle.session(cap);

                        let epochs = inbuffer.borrow().epochs().collect::<Vec<_>>();

                        for epoch in epochs {
                            let snaps = inbuffer.borrow_mut().remove(&epoch).unwrap();
                            let path = local_state_store
                                .borrow_mut()
                                .write_snapshots_segment(snaps, epoch)
                                .unwrap();
                            session.give(path);
                        }
                    },
                    |caps, ()| {
                        let cap = &caps[1];

                        let mut handle = batch_segments_output.activate();
                        let mut session = handle.session(cap);

                        let epochs = inbuffer.borrow().epochs().collect::<Vec<_>>();

                        for epoch in epochs {
                            let snaps = inbuffer.borrow_mut().remove(&epoch).unwrap();
                            let path = local_state_store
                                .borrow_mut()
                                .write_snapshots_segment(snaps, epoch)
                                .unwrap();
                            session.give(path);
                        }
                    },
                )
            }
        });
        segments
    }
}

impl<S> CompactorOp<S> for ClockStream<S>
where
    S: Scope<Timestamp = u64>,
{
    fn compactor(&self, local_state_store: Rc<RefCell<LocalStateStore>>) -> Stream<S, PathBuf> {
        let mut op_builder = OperatorBuilder::new("frontier_compactor".to_string(), self.scope());
        let mut input = op_builder.new_input(self, Pipeline);
        let (mut early_segments_output, early_segments) = op_builder.new_output();
        let (mut closing_segments_output, closing_segments) = op_builder.new_output();
        let segments = early_segments.concatenate(vec![closing_segments]);

        op_builder.build(move |init_caps| {
            let inbuffer = Rc::new(RefCell::new(InBuffer::new()));
            let mut ncater = EagerNotificator::new(init_caps, ());

            move |input_frontiers| {
                input.buffer_notify(&mut inbuffer.borrow_mut(), &mut ncater);

                ncater.for_each(
                    input_frontiers,
                    |caps, ()| {
                        let clock_cap = &caps[0];

                        let mut handle = early_segments_output.activate();
                        let mut session = handle.session(clock_cap);

                        let epochs = inbuffer.borrow().epochs().collect::<Vec<_>>();
                        for epoch in epochs {
                            inbuffer.borrow_mut().remove(&epoch);
                            let path = local_state_store
                                .borrow_mut()
                                .write_frontier_segment(epoch)
                                .unwrap();
                            session.give(path);
                        }
                    },
                    |caps, ()| {
                        let clock_cap = &caps[1];

                        let mut handle = closing_segments_output.activate();
                        let mut session = handle.session(clock_cap);

                        let epochs = inbuffer.borrow().epochs().collect::<Vec<_>>();
                        for epoch in epochs {
                            inbuffer.borrow_mut().remove(&epoch);
                            let path = local_state_store
                                .borrow_mut()
                                .write_frontier_segment(epoch)
                                .unwrap();
                            session.give(path);
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
    m.add_class::<SnapshotMode>()?;
    Ok(())
}
