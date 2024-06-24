//! Internal code for implementing recovery.
//!
//! For a user-centric version of recovery, read the
//! `bytewax.recovery` Python module docstring. Read that first.

use std::cell::RefCell;
use std::cmp::Ordering;
use std::collections::BTreeMap;
use std::collections::HashMap;
use std::fmt;
use std::fmt::Debug;
use std::hash::Hash;
use std::ops::Deref;
use std::path::Path;
use std::path::PathBuf;
use std::rc::Rc;

use opentelemetry::trace::FutureExt;
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

/// Module that holds all the queries used for recovery.
mod queries {
    /// Get the meta data from the most recent execution number
    pub(crate) const GET_META: &str = "\
        SELECT ex_num, cluster_frontier, worker_count, worker_index \
        FROM meta \
        WHERE (ex_num, flow_id) IN (SELECT MAX(ex_num), flow_id FROM meta)";
}

pub(crate) struct StateStoreCache {
    cache: HashMap<StepId, BTreeMap<StateKey, PyObject>>,
}

impl StateStoreCache {
    pub fn new() -> Self {
        Self {
            cache: HashMap::new(),
        }
    }

    pub fn add_step(&mut self, step_id: StepId) {
        self.cache.insert(step_id, Default::default());
    }

    pub fn contains_key(&self, step_id: &StepId, key: &StateKey) -> bool {
        self.cache.get(step_id).unwrap().contains_key(key)
    }

    pub fn insert(&mut self, step_id: &StepId, key: StateKey, logic: PyObject) {
        self.cache.get_mut(step_id).unwrap().insert(key, logic);
    }

    pub fn get(&self, step_id: &StepId, key: &StateKey) -> Option<&PyObject> {
        self.cache.get(step_id).unwrap().get(key)
    }

    pub fn keys(&self, step_id: &StepId) -> Vec<StateKey> {
        self.cache.get(step_id).unwrap().keys().cloned().collect()
    }

    pub fn remove(&mut self, step_id: &StepId, key: &StateKey) -> Option<PyObject> {
        self.cache.get_mut(step_id).unwrap().remove(key)
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

    // Writers for snapshots and frontiers
    frontier_writer: FrontierWriter,
    snapshot_writer: SnapshotWriter,

    // Segment handling internal data
    current_epoch: u64,
    seg_num: u64,
}

impl LocalStateStore {
    pub fn new(
        // db_dir: PathBuf,
        flow_id: String,
        worker_index: usize,
        worker_count: usize,
        recovery_config: &Bound<'_, RecoveryConfig>,
    ) -> PyResult<Self> {
        // Set db_dir and open main connection to the local store.
        let db_dir = recovery_config.borrow().db_dir.clone();
        if !db_dir.is_dir() {
            return Err(PyFileNotFoundError::new_err(format!(
                "recovery directory {:?} does not exist; \
                see the `bytewax.recovery` module docstring for more info",
                db_dir
            )));
        }
        let file_name = format!("flow_{flow_id}_worker_{worker_index}.sqlite3");
        let conn = setup_conn(db_dir.join(file_name))?;

        // Calculate resume_from.
        // First we need to retrieve the latest frontier segment from
        // the durable store, as we never store frontier info in the
        // local store, so we have a single source of truth.
        // Start with the default value, then if the durable store
        // does have a frontier segment, update it with the fetched value.
        let mut resume_from = ResumeFrom::default();
        let backup = recovery_config.borrow().backup.clone();
        let keys = Python::with_gil(|py| backup.list_keys(py).unwrap());
        let mut frontier_segments: Vec<(u64, u64, String)> = keys
            .into_iter()
            .filter(|key| key.starts_with("frontier:"))
            .filter_map(|key| {
                let split: Vec<&str> = key.strip_suffix(".sqlite3").unwrap().split(':').collect();
                // First only filter current worker's segments
                let worker = split[4]
                    .strip_prefix("worker-")
                    .unwrap()
                    .parse::<usize>()
                    .unwrap();
                if worker != worker_index {
                    return None;
                }
                // Then extract execution_number and epoch
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
        frontier_segments.sort_by(|a, b| {
            // If execution_number if different, order based on that
            let ex_cmp = a.0.cmp(&b.0);
            if ex_cmp != Ordering::Equal {
                return ex_cmp;
            }
            // Otherwise order based on epoch
            a.1.cmp(&b.1)
        });

        if let Some((_, _, frontier_segment)) = frontier_segments.last() {
            let segment_file = db_dir.join(frontier_segment);
            Python::with_gil(|py| {
                backup
                    .download(py, frontier_segment, segment_file.as_path())
                    .unwrap()
            });
            let conn = setup_conn(segment_file)?;
            let res = conn.query_row(queries::GET_META, (), |row| {
                let ex_num = row.get::<_, Option<u64>>(0)?.map(ExecutionNumber);
                let resume_epoch = row.get::<_, Option<u64>>(1)?.map(ResumeEpoch);
                Ok(ex_num
                    .zip(resume_epoch)
                    // Advance the execution number here.
                    .map(|(en, re)| ResumeFrom::new(en.next(), re)))
            });
            resume_from = match res {
                // If no rows in the db, it was empty, so use the default.
                Err(rusqlite::Error::QueryReturnedNoRows) => ResumeFrom::default(),
                // Any other error was an error reading from the db, we should stop here.
                Err(err) => std::panic::panic_any(err),
                Ok(row) => row.unwrap_or_default(),
            };
            println!(
                "Resuming from ex: {}, epoch {}",
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

        Ok(Self {
            conn,
            db_dir,
            backup,
            current_epoch: resume_from.epoch().0,
            worker_index,
            frontier_writer,
            snapshot_writer: SnapshotWriter {},
            resume_from,
            seg_num: 0,
        })
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
        &mut self,
        py: Python,
        step_id: &StepId,
    ) -> PyResult<Vec<(StateKey, Option<PyObject>)>> {
        // Get all the snapshots in the store for this specific step_id,
        // and deserialize them.
        let pickle = py.import_bound("pickle")?;
        self.conn
            // Retrieve all the snapshots for the latest epoch saved
            // in the recovery store that's <= than resume_from.
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
            .reraise("Error binding query parameters in recovery store")?
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
    pub(crate) db_dir: PathBuf,
    #[pyo3(get)]
    pub(crate) backup: Backup,
    #[pyo3(get)]
    pub(crate) snapshot_mode: SnapshotMode,
}

#[pymethods]
impl RecoveryConfig {
    #[new]
    fn new(
        db_dir: PathBuf,
        backup: Option<Backup>,
        snapshot_mode: Option<SnapshotMode>,
    ) -> PyResult<Self> {
        let snapshot_mode = snapshot_mode.unwrap_or(SnapshotMode::Immediate);

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

// pub(crate) trait WriteFrontiersOp<S>
// where
//     S: Scope,
// {
//     fn write_frontiers(&self, state_store: Rc<RefCell<LocalStateStore>>) -> ClockStream<S>;
// }

// impl<S> WriteFrontiersOp<S> for ClockStream<S>
// where
//     S: Scope<Timestamp = u64>,
// {
//     fn write_frontiers(&self, state_store: Rc<RefCell<LocalStateStore>>) -> ClockStream<S> {
//         let mut op_builder = OperatorBuilder::new("frontier_compactor".to_string(), self.scope());
//         let mut input = op_builder.new_input(self, Pipeline);
//         let (mut output, clock) = op_builder.new_output();

//         op_builder.build(move |init_caps| {
//             let mut inbuffer = InBuffer::new();
//             let mut ncater = EagerNotificator::new(init_caps, ());

//             move |input_frontiers| {
//                 input.buffer_notify(&mut inbuffer, &mut ncater);

//                 ncater.for_each(
//                     input_frontiers,
//                     |_caps, ()| {},
//                     |caps, ()| {
//                         let cap = &caps[0];
//                         let epoch = cap.time();
//                         // Write cluster frontier
//                         unwrap_any!(state_store.borrow_mut().write_frontier(*epoch));
//                         // And finally allow the dataflow to advance its epoch.
//                         output.activate().session(cap).give(());
//                     },
//                 )
//             }
//         });
//         clock
//     }
// }

// pub(crate) trait CompactFrontiersOp<S>
// where
//     S: Scope,
// {
//     fn compact_frontiers(&self, state_store: Rc<RefCell<LocalStateStore>>) -> Stream<S, PathBuf>;
// }

// impl<S> CompactFrontiersOp<S> for ClockStream<S>
// where
//     S: Scope<Timestamp = u64>,
// {
//     fn compact_frontiers(&self, state_store: Rc<RefCell<LocalStateStore>>) -> Stream<S, PathBuf> {
//         let mut op_builder = OperatorBuilder::new("frontier_compactor".to_string(), self.scope());
//         let mut input = op_builder.new_input(self, Pipeline);
//         let (mut segments_output, segments) = op_builder.new_output();

//         op_builder.build(move |init_caps| {
//             let mut inbuffer = InBuffer::new();
//             let mut ncater = EagerNotificator::new(init_caps, ());

//             move |input_frontiers| {
//                 input.buffer_notify(&mut inbuffer, &mut ncater);

//                 ncater.for_each(
//                     input_frontiers,
//                     |_caps, ()| {},
//                     |caps, ()| {
//                         let clock_cap = &caps[0];

//                         let mut handle = segments_output.activate();
//                         let mut session = handle.session(clock_cap);

//                         let epochs = inbuffer.epochs().collect::<Vec<_>>();
//                         let mut state = state_store.borrow_mut();
//                         for epoch in epochs {
//                             let segment = unwrap_any!(state.open_segment(epoch));
//                             let file_name = segment.file_name();
//                             inbuffer.remove(&epoch);
//                             segment.write_frontier(epoch).unwrap();
//                             session.give(file_name);
//                         }
//                     },
//                 )
//             }
//         });
//         segments
//     }
// }

// pub(crate) trait DurableBackupOp<S>
// where
//     S: Scope,
// {
//     fn durable_backup(&self, backup: Backup, immediate_backup: bool) -> ClockStream<S>;
// }

// impl<S> DurableBackupOp<S> for Stream<S, PathBuf>
// where
//     S: Scope<Timestamp = u64>,
// {
//     fn durable_backup(&self, backup: Backup, immediate_backup: bool) -> ClockStream<S> {
//         let mut op_builder = OperatorBuilder::new("compactor".to_string(), self.scope());
//         let mut input = op_builder.new_input(self, Pipeline);
//         let (mut output, clock) = op_builder.new_output();

//         op_builder.build(move |init_caps| {
//             // TODO: EagerNotificator is probably not the best tool here
//             //       since the logic is the same, only the eager one is optional
//             //       depending on a condition, so a slightly different notificator
//             //       might avoid some code duplication and the Rc<RefCell<_>>
//             let inbuffer = Rc::new(RefCell::new(InBuffer::<u64, PathBuf>::new()));
//             let mut ncater = EagerNotificator::new(init_caps, ());

//             // This logic is reused in both eager and closing logic, but only
//             // if immediate snapshot is True in the eager one.
//             // Takes the inbuffer, empties it and uploads all the paths in the stream.
//             let upload = move |inbuffer: Rc<RefCell<InBuffer<u64, PathBuf>>>| {
//                 Python::with_gil(|py| {
//                     let epochs = inbuffer.borrow().epochs().collect::<Vec<_>>();
//                     for epoch in epochs {
//                         for path in inbuffer.borrow_mut().remove(&epoch).unwrap() {
//                             backup
//                                 .upload(
//                                     py,
//                                     path.clone(),
//                                     path.file_name().unwrap().to_string_lossy().to_string(),
//                                 )
//                                 .unwrap();
//                         }
//                     }
//                 });
//             };

//             move |input_frontiers| {
//                 input.buffer_notify(&mut inbuffer.borrow_mut(), &mut ncater);

//                 ncater.for_each(
//                     input_frontiers,
//                     |_caps, ()| {
//                         if immediate_backup {
//                             upload(inbuffer.clone());
//                         }
//                     },
//                     |caps, ()| {
//                         // The inbuffer has been emptied already if immediate_snapshot
//                         // is set to True, so we can upload unconditionally here.
//                         let cap = &caps[0];
//                         upload(inbuffer.clone());
//                         output.activate().session(cap).give(());
//                     },
//                 )
//             }
//         });
//         clock
//     }
// }

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
                        upload(inbuffer.clone());
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

pub(crate) trait RealCompactorOp<S>
where
    S: Scope,
{
    fn compactor(&self, local_state_store: Rc<RefCell<LocalStateStore>>) -> Stream<S, PathBuf>;
}

impl<S> RealCompactorOp<S> for Stream<S, SerializedSnapshot>
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

impl<S> RealCompactorOp<S> for ClockStream<S>
where
    S: Scope<Timestamp = u64>,
{
    fn compactor(&self, local_state_store: Rc<RefCell<LocalStateStore>>) -> Stream<S, PathBuf> {
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
                            inbuffer.remove(&epoch);
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
