//! Internal code for implementing recovery.
//!
//! For a user-centric version of recovery, read the
//! `bytewax.recovery` Python module docstring. Read that first.

use std::cell::RefCell;
use std::collections::BTreeSet;
use std::collections::HashMap;
use std::ffi::OsStr;
use std::fmt;
use std::fmt::Debug;
use std::fs;
use std::hash::BuildHasherDefault;
use std::hash::Hash;
use std::path::Path;
use std::path::PathBuf;
use std::rc::Rc;

use chrono::TimeDelta;
use pyo3::create_exception;
use pyo3::exceptions::PyFileNotFoundError;
use pyo3::exceptions::PyRuntimeError;
use pyo3::exceptions::PyTypeError;
use pyo3::exceptions::PyValueError;
use pyo3::intern;
use pyo3::prelude::*;
use pyo3::sync::GILOnceCell;
use pyo3::types::PyBytes;
use rusqlite::Connection;
use rusqlite::OpenFlags;
use rusqlite_migration::Migrations;
use rusqlite_migration::M;
use seahash::SeaHasher;
use serde::Deserialize;
use serde::Serialize;
use timely::dataflow::channels::pact::Pipeline;
use timely::dataflow::operators::generic::builder_rc::OperatorBuilder;
use timely::dataflow::operators::Broadcast;
use timely::dataflow::operators::Concat;
use timely::dataflow::operators::Delay;
use timely::dataflow::operators::Map;
use timely::dataflow::operators::Operator;
use timely::dataflow::Scope;
use timely::dataflow::Stream;
use timely::progress::Timestamp;
use timely::Data;
use tracing::instrument;

use crate::errors::PythonException;
use crate::inputs::EpochInterval;
use crate::pyo3_extensions::TdPyAny;
use crate::timely::*;
use crate::unwrap_any;

/// IDs a specific recovery partition.
///
/// The inner value will be up to [`PartitionCount`].
#[derive(Debug, Copy, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub(crate) struct PartitionIndex(usize);

impl fmt::Display for PartitionIndex {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// Total number of recovery partitions.
#[derive(Debug, Copy, Clone, PartialEq, Eq, Serialize, Deserialize, FromPyObject)]
pub(crate) struct PartitionCount(usize);

impl PartitionCount {
    /// Return an iter of all partitions.
    fn iter(&self) -> impl Iterator<Item = PartitionIndex> {
        (0..self.0).map(PartitionIndex)
    }
}

impl fmt::Display for PartitionCount {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// Metadata about a recovery partition.
///
/// This represents a row in the `parts` table.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct PartitionMeta(PartitionIndex, PartitionCount);

/// Incrementing ID representing how many times a dataflow has been
/// executed to completion or failure.
///
/// This is used to ensure recovery progress information for a worker
/// `3` is not mis-interpreted to belong to an earlier cluster.
///
/// As you resume a dataflow, this will increase by 1 each time.
#[derive(Debug, Copy, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub(crate) struct ExecutionNumber(u64);

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

/// The oldest epoch for which work is still outstanding on a worker.
#[derive(Debug, Copy, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct WorkerFrontier(u64);

impl fmt::Display for WorkerFrontier {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// Metadata about the current frontier of a worker.
///
/// This represents a row in the `fronts` table.
#[derive(Debug, Copy, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct FrontierMeta(ExecutionNumber, WorkerIndex, WorkerFrontier);

/// Metadata about a commit in a recovery partition.
///
/// This represents a row in the `commits` table.
#[derive(Debug, Copy, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct CommitMeta(PartitionIndex, u64);

/// System time duration to keep around state snapshots, even after
/// they are no longer needed by the current execution.
///
/// This is used to delay GC of state data so that if durable backup
/// of recovery partitions are not instantaneous or synchronized, we
/// can ensure that there's some resume epoch shared by all partitions
/// we can use when resuming from a backup that might not be the most
/// recent.
#[derive(Debug, Copy, Clone)]
pub(crate) struct BackupInterval(TimeDelta);

impl Default for BackupInterval {
    fn default() -> Self {
        Self(TimeDelta::zero())
    }
}

impl IntoPy<Py<PyAny>> for BackupInterval {
    fn into_py(self, py: Python<'_>) -> Py<PyAny> {
        self.0.into_py(py)
    }
}

impl<'py> FromPyObject<'py> for BackupInterval {
    fn extract_bound(obj: &Bound<'py, PyAny>) -> PyResult<Self> {
        if let Ok(duration) = obj.extract::<TimeDelta>() {
            Ok(Self(duration))
        } else {
            Err(PyTypeError::new_err(
                "backup interval must be a `datetime.timedelta`",
            ))
        }
    }
}

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
struct SerializedSnapshot(StepId, StateKey, SnapshotEpoch, Option<Vec<u8>>);

/// Configuration settings for recovery.
///
/// :arg db_dir: Local filesystem directory to search for recovery
///     database partitions.
///
/// :type db_dir: pathlib.Path
///
/// :arg backup_interval: Amount of system time to wait to permanently
///     delete a state snapshot after it is no longer needed. You
///     should set this to the interval at which you are backing up
///     the recovery partitions off of the workers into archival
///     storage (e.g. S3). Defaults to zero duration.
///
/// :type backup_interval: typing.Optional[datetime.timedelta]
#[pyclass(module = "bytewax.recovery")]
pub(crate) struct RecoveryConfig {
    #[pyo3(get)]
    db_dir: PathBuf,
    #[pyo3(get)]
    backup_interval: BackupInterval,
}

#[pymethods]
impl RecoveryConfig {
    #[new]
    fn new(db_dir: PathBuf, backup_interval: Option<BackupInterval>) -> Self {
        Self {
            db_dir,
            backup_interval: backup_interval.unwrap_or_default(),
        }
    }
}

impl RecoveryConfig {
    /// Build the Rust-side bundle from the Python-side recovery
    /// config.
    #[instrument(name = "build_recovery", skip_all)]
    pub(crate) fn build(&self, py: Python) -> PyResult<(RecoveryBundle, BackupInterval)> {
        let mut part_paths = HashMap::new();
        let sqlite_ext = OsStr::new("sqlite3");
        if !self.db_dir.is_dir() {
            return Err(PyFileNotFoundError::new_err(format!(
                "recovery directory {:?} does not exist; see the `bytewax.recovery` module docstring for more info",
                self.db_dir
            )));
        }
        for entry in fs::read_dir(self.db_dir.clone()).reraise("Error listing recovery DB dir")? {
            let path = entry.reraise("Error accessing recovery DB file")?.path();
            if path.extension().map_or(false, |ext| *ext == *sqlite_ext) {
                let part =
                    RecoveryPart::open(py, &path).reraise("Error opening recovery DB file")?;
                let mut part_loader = part.part_loader();
                while let Some(batch) = part_loader.next_batch() {
                    for PartitionMeta(index, _count) in batch {
                        tracing::info!("Access to partition {index:?} at {path:?}");
                        part_paths.insert(index, path.clone());
                    }
                }
            }
        }

        let bundle = RecoveryBundle {
            part_paths: Rc::new(part_paths),
            built_parts: Rc::new(RefCell::new(HashMap::new())),
        };
        let backup_interval = self.backup_interval;

        Ok((bundle, backup_interval))
    }
}

/// Clone-able reference to all local recovery partition info.
pub(crate) struct RecoveryBundle {
    /// This is a map to all known local partitions.
    ///
    /// It is an [`Rc`] because the builder functions created by
    /// [`new_builder`] need to retain a handle to this to be able to
    /// look up the relevant path. No [`RefCell`] because they don't
    /// need to modify it.
    part_paths: Rc<HashMap<PartitionIndex, PathBuf>>,
    /// This is a cache of already built [`RecoveryDB`].
    ///
    /// The map itself is an [`Rc<RefCell>`] because the builder
    /// functions need to own a reference and update the cache so only
    /// one partition is built, even if it is requested multiple
    /// times. The values are [`Rc<RefCell<RecoveryDb>`] so that this
    /// cache and the Timely operators themselves all have ownership
    /// access to the partition.
    built_parts: Rc<RefCell<HashMap<PartitionIndex, Rc<RefCell<RecoveryPart>>>>>,
}

impl RecoveryBundle {
    pub(crate) fn clone_ref(&self, _py: Python) -> Self {
        Self {
            part_paths: self.part_paths.clone(),
            built_parts: self.built_parts.clone(),
        }
    }

    fn local_parts(&self) -> Vec<PartitionIndex> {
        self.part_paths.keys().copied().collect()
    }

    /// Create a new builder function that the partitioned read and
    /// write and commit operators can use to build partitions.
    ///
    /// This clones all the [`Rc`]s appropriately internally so that
    /// the cache is used.
    fn new_builder(&self) -> impl FnMut(&PartitionIndex) -> Rc<RefCell<RecoveryPart>> {
        let part_paths = self.part_paths.clone();
        let built_parts = self.built_parts.clone();
        move |part_key| {
            built_parts
                .borrow_mut()
                .entry(*part_key)
                .or_insert_with_key(|part_key| {
                    let path = part_paths
                        .get(part_key)
                        .unwrap_or_else(|| {
                            panic!("Trying to build RecoveryPartition for {part_key:?} but no path is known");
                        });

                    let part = unwrap_any!(Python::with_gil(|py| RecoveryPart::open(py, path)));

                    Rc::new(RefCell::new(part))
                })
                .clone()
        }
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

/// Wrapper around an SQLite DB connection with methods for our
/// recovery operations.
struct RecoveryPart {
    /// This is [`Rc<RefCell>`] so that our reader and writer structs
    /// can maintain an internal connection reference across batches.
    conn: Rc<RefCell<Connection>>,
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
                "CREATE TABLE parts (
                 created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                 part_index INTEGER PRIMARY KEY NOT NULL CHECK (part_index >= 0),
                 part_count INTEGER NOT NULL CHECK (part_count > 0),
                 CHECK (part_index < part_count)
                 ) STRICT",
            ),
            // This is a sharded table.
            M::up(
                "CREATE TABLE exs (
                 created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                 ex_num INTEGER NOT NULL PRIMARY KEY,
                 worker_count INTEGER NOT NULL CHECK (worker_count > 0),
                 resume_epoch INTEGER NOT NULL
                 ) STRICT",
            ),
            // This is a sharded table.
            //
            // We can't do a foreign key constraint because we don't
            // know what partition the row in `ex` will be in; we'd
            // need a "sharded foreign key" kinda thing.
            M::up(
                "CREATE TABLE fronts (
                 created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                 ex_num INTEGER NOT NULL,
                 worker_index INTEGER NOT NULL CHECK (worker_index >= 0),
                 worker_frontier INTEGER NOT NULL,
                 PRIMARY KEY (ex_num, worker_index)
                 ) STRICT",
            ),
            // This is _not_ a sharded table. Commits affect a whole
            // partition and thus will only be written to the same
            // shard as partition definitions. We don't use a foreign
            // key here, though so we don't have to deal with flow_id.
            M::up(
                "CREATE TABLE commits (
                 created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                 part_index INTEGER PRIMARY KEY NOT NULL,
                 commit_epoch INTEGER NOT NULL
                 ) STRICT",
            ),
            // This is a sharded table.
            M::up(
                "CREATE TABLE snaps (
                 created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                 step_id TEXT NOT NULL,
                 state_key TEXT NOT NULL,
                 snap_epoch INTEGER NOT NULL,
                 ser_change BLOB,
                 PRIMARY KEY (step_id, state_key, snap_epoch)
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

/// Setup our connection-level pragmas. Run this on each connection.
fn setup_conn(py: Python, conn: &Rc<RefCell<Connection>>) {
    let mut conn = conn.borrow_mut();

    rusqlite::vtab::series::load_module(&conn).unwrap();
    conn.pragma_update(None, "foreign_keys", "ON").unwrap();
    // These are recommended by Litestream.
    conn.pragma_update(None, "journal_mode", "WAL").unwrap();
    conn.pragma_update(None, "busy_timeout", "5000").unwrap();
    get_migrations(py).to_latest(&mut conn).unwrap();
}

struct PartitionMetaWriter {
    conn: Rc<RefCell<Connection>>,
}

impl Writer for PartitionMetaWriter {
    type Item = PartitionMeta;

    fn write_batch(&mut self, items: Vec<Self::Item>) {
        let mut conn = self.conn.borrow_mut();
        let txn = conn.transaction().unwrap();
        for part in items {
            tracing::trace!("Writing {part:?}");
            let PartitionMeta(part_index, part_count) = part;
            txn.execute(
                "INSERT INTO parts (part_index, part_count)
                 VALUES (?1, ?2)",
                (part_index.0, part_count.0),
            )
            .unwrap();
        }
        txn.commit().unwrap();
    }
}

struct ExecutionMetaWriter {
    conn: Rc<RefCell<Connection>>,
}

impl Writer for ExecutionMetaWriter {
    type Item = ExecutionMeta;

    fn write_batch(&mut self, items: Vec<Self::Item>) {
        let mut conn = self.conn.borrow_mut();
        let txn = conn.transaction().unwrap();
        for ex in items {
            tracing::trace!("Writing {ex:?}");
            let ExecutionMeta(ex_num, worker_count, resume_epoch) = ex;
            // Do not upsert because we should never see an execution
            // twice.
            txn.execute(
                "INSERT INTO exs (ex_num, worker_count, resume_epoch)
                 VALUES (?1, ?2, ?3)",
                (ex_num.0, worker_count.0, resume_epoch.0),
            )
            .unwrap();
        }
        txn.commit().unwrap();
    }
}

struct FrontierWriter {
    conn: Rc<RefCell<Connection>>,
}

impl Writer for FrontierWriter {
    type Item = FrontierMeta;

    fn write_batch(&mut self, items: Vec<Self::Item>) {
        let mut conn = self.conn.borrow_mut();
        let txn = conn.transaction().unwrap();
        for front in items {
            tracing::trace!("Writing {front:?}");
            let FrontierMeta(ex, worker_count, wf) = front;
            txn.execute(
                "INSERT INTO fronts (ex_num, worker_index, worker_frontier)
                 VALUES (?1, ?2, ?3)
                 ON CONFLICT (ex_num, worker_index) DO UPDATE
                 SET worker_frontier = EXCLUDED.worker_frontier",
                (ex.0, worker_count.0, wf.0),
            )
            .unwrap();
        }
        txn.commit().unwrap();
    }
}

struct SerializedSnapshotWriter {
    conn: Rc<RefCell<Connection>>,
}

impl Writer for SerializedSnapshotWriter {
    type Item = SerializedSnapshot;

    fn write_batch(&mut self, items: Vec<Self::Item>) {
        let mut conn = self.conn.borrow_mut();
        let txn = conn.transaction().unwrap();
        for snap in items {
            tracing::trace!("Writing {snap:?}");
            let SerializedSnapshot(step_id, state_key, snap_epoch, ser_change) = snap;
            txn.execute(
                "INSERT INTO snaps (step_id, state_key, snap_epoch, ser_change)
                 VALUES (?1, ?2, ?3, ?4)
                 ON CONFLICT (step_id, state_key, snap_epoch) DO UPDATE
                 SET ser_change = EXCLUDED.ser_change",
                (step_id.0, state_key.0, snap_epoch.0, ser_change),
            )
            .unwrap();
        }
        txn.commit().unwrap();
    }
}

struct CommitWriter {
    conn: Rc<RefCell<Connection>>,
}

impl Writer for CommitWriter {
    type Item = CommitMeta;

    fn write_batch(&mut self, items: Vec<Self::Item>) {
        let mut conn = self.conn.borrow_mut();
        let txn = conn.transaction().unwrap();
        for commit in items {
            tracing::trace!("Writing {commit:?}");
            let CommitMeta(part_idx, commit_epoch) = commit;
            txn.execute(
                "INSERT INTO commits (part_index, commit_epoch)
                 VALUES (?1, ?2)",
                (part_idx.0, commit_epoch),
            )
            .unwrap();
        }
        txn.commit().unwrap();
    }
}

struct PartitionMetaLoader {
    conn: Rc<RefCell<Connection>>,
    done: bool,
}

impl PartitionMetaLoader {
    fn new(conn: Rc<RefCell<Connection>>) -> Self {
        Self { conn, done: false }
    }
}

impl BatchIterator for PartitionMetaLoader {
    type Item = PartitionMeta;

    fn next_batch(&mut self) -> Option<Vec<Self::Item>> {
        if !self.done {
            let batch = self
                .conn
                .borrow_mut()
                .prepare(
                    "SELECT part_index, part_count
                     FROM parts",
                )
                .unwrap()
                .query_map((), |row| {
                    Ok(PartitionMeta(
                        PartitionIndex(row.get(0)?),
                        PartitionCount(row.get(1)?),
                    ))
                })
                .unwrap()
                .map(|res| res.expect("error unpacking PartitionMeta"))
                // We have to collect so that we don't need to retain a
                // reference to the connection in the iterator. Progress
                // info is "small" so it's ok to load it all into
                // memory. TODO: One day we could make a generic version
                // of [`SnapIterator`] that keeps an [`Rc`] and paginates.
                .collect();
            self.done = true;
            Some(batch)
        } else {
            None
        }
    }
}

struct ExecutionMetaLoader {
    conn: Rc<RefCell<Connection>>,
    done: bool,
}

impl ExecutionMetaLoader {
    fn new(conn: Rc<RefCell<Connection>>) -> Self {
        Self { conn, done: false }
    }
}

impl BatchIterator for ExecutionMetaLoader {
    type Item = ExecutionMeta;

    fn next_batch(&mut self) -> Option<Vec<Self::Item>> {
        if !self.done {
            let batch = self
                .conn
                .borrow_mut()
                .prepare(
                    "SELECT ex_num, worker_count, resume_epoch
                     FROM exs",
                )
                .unwrap()
                .query_map((), |row| {
                    Ok(ExecutionMeta(
                        ExecutionNumber(row.get(0)?),
                        WorkerCount(row.get(1)?),
                        ResumeEpoch(row.get(2)?),
                    ))
                })
                .unwrap()
                .map(|res| res.expect("error unpacking ExecutionMeta"))
                .collect();
            self.done = true;
            Some(batch)
        } else {
            None
        }
    }
}

struct FrontierLoader {
    conn: Rc<RefCell<Connection>>,
    done: bool,
}

impl FrontierLoader {
    fn new(conn: Rc<RefCell<Connection>>) -> Self {
        Self { conn, done: false }
    }
}

impl BatchIterator for FrontierLoader {
    type Item = FrontierMeta;

    fn next_batch(&mut self) -> Option<Vec<Self::Item>> {
        if !self.done {
            let batch = self
                .conn
                .borrow()
                .prepare(
                    "SELECT ex_num, worker_index, worker_frontier
                     FROM fronts",
                )
                .unwrap()
                .query_map((), |row| {
                    Ok(FrontierMeta(
                        ExecutionNumber(row.get(0)?),
                        WorkerIndex(row.get(1)?),
                        WorkerFrontier(row.get(2)?),
                    ))
                })
                .unwrap()
                .map(|res| res.expect("error unpacking FrontierMeta"))
                .collect();
            self.done = true;
            Some(batch)
        } else {
            None
        }
    }
}

enum Cursor<T> {
    /// We haven't started reading the table.
    Uninit,
    /// We should read from position T next.
    InProgress(T),
    /// We're done reading the table.
    Done,
}

/// Iterator that keeps a connection ref so we don't keep an open txn
/// the entire time we're dumping batches out of all state snapshots.
struct SerializedSnapshotLoader {
    conn: Rc<RefCell<Connection>>,
    before: ResumeEpoch,
    batch_size: usize,
    cursor: Cursor<(StepId, StateKey)>,
}

impl SerializedSnapshotLoader {
    fn new(conn: Rc<RefCell<Connection>>, before: ResumeEpoch, batch_size: usize) -> Self {
        Self {
            conn,
            before,
            batch_size,
            cursor: Cursor::Uninit,
        }
    }

    fn select(
        &self,
        cursor: Option<(&StepId, &StateKey)>,
    ) -> (Vec<SerializedSnapshot>, Cursor<(StepId, StateKey)>) {
        let (cursor_step_id, cursor_state_key) = cursor.unzip();

        let batch: Vec<_> = self
            .conn
            .borrow()
            // Filters in SQL down to just the last relevant snapshot per
            // (step_id, state_key) to reduce redundant reads. Remember,
            // must be <, not <= resume epoch.  The WHERE clause is whack
            // because we want to use the "most recently read (step_id,
            // state_key)" as a "resume position". We're ordering the
            // results by that, and since there is only one row per
            // (step_id, state_key), the LIMIT clause causes it to batch.
            .prepare(
                "WITH max_epoch_snaps AS (
                 SELECT step_id, state_key, MAX(snap_epoch) AS snap_epoch
                 FROM snaps
                 WHERE snap_epoch < ?1
                 GROUP BY step_id, state_key
                 )
                 SELECT step_id, state_key, snap_epoch, ser_change
                 FROM snaps
                 JOIN max_epoch_snaps USING (step_id, state_key, snap_epoch)
                 WHERE ?2 IS NULL OR ?3 IS NULL OR (step_id, state_key) > (?2, ?3)
                 ORDER BY step_id, state_key
                 LIMIT ?4",
            )
            .unwrap()
            .query_map(
                (
                    self.before.0,
                    cursor_step_id.map(|s| &s.0),
                    cursor_state_key.map(|s| &s.0),
                    self.batch_size,
                ),
                |row| {
                    Ok(SerializedSnapshot(
                        StepId(row.get(0)?),
                        StateKey(row.get(1)?),
                        SnapshotEpoch(row.get(2)?),
                        row.get(3)?,
                    ))
                },
            )
            .unwrap()
            .map(|res| {
                let snap = res.expect("error unpacking SerializedSnapshot");
                tracing::trace!("Read {snap:?}");
                snap
            })
            .collect();

        let cursor = if let Some(SerializedSnapshot(step_id, state_key, _snap_epoch, _ser_change)) =
            batch.last()
        {
            Cursor::InProgress((step_id.clone(), state_key.clone()))
        } else {
            Cursor::Done
        };

        (batch, cursor)
    }
}

impl BatchIterator for SerializedSnapshotLoader {
    type Item = SerializedSnapshot;

    fn next_batch(&mut self) -> Option<Vec<Self::Item>> {
        let (batch, next_cursor) = match &self.cursor {
            Cursor::Uninit => {
                let (batch, cursor) = self.select(None);
                (Some(batch), cursor)
            }
            Cursor::InProgress((step_id, state_key)) => {
                let (batch, cursor) = self.select(Some((step_id, state_key)));
                (Some(batch), cursor)
            }
            Cursor::Done => (None, Cursor::Done),
        };

        self.cursor = next_cursor;
        batch
    }
}

struct CommitLoader {
    conn: Rc<RefCell<Connection>>,
    done: bool,
}

impl CommitLoader {
    fn new(conn: Rc<RefCell<Connection>>) -> Self {
        Self { conn, done: false }
    }
}

impl BatchIterator for CommitLoader {
    type Item = CommitMeta;

    fn next_batch(&mut self) -> Option<Vec<Self::Item>> {
        if !self.done {
            let batch = self
                .conn
                .borrow()
                .prepare(
                    "SELECT part_index, commit_epoch
                     FROM commits",
                )
                .unwrap()
                .query_map((), |row| {
                    Ok(CommitMeta(PartitionIndex(row.get(0)?), row.get(1)?))
                })
                .unwrap()
                .map(|res| res.expect("error unpacking CommitMeta"))
                .collect();
            self.done = true;
            Some(batch)
        } else {
            None
        }
    }
}

struct RecoveryCommitter {
    conn: Rc<RefCell<Connection>>,
    part_key: PartitionIndex,
}

impl Committer<u64> for RecoveryCommitter {
    /// This will be called when `epoch` is the earliest possible
    /// resume epoch.
    fn commit(&mut self, epoch: &u64) {
        tracing::trace!("Committing / GCing epoch {epoch:?}");
        let mut conn = self.conn.borrow_mut();
        let txn = conn.transaction().unwrap();
        txn.execute(
            "INSERT INTO commits (part_index, commit_epoch)
             VALUES (?1, ?2)
             ON CONFLICT (part_index) DO UPDATE
             SET commit_epoch = EXCLUDED.commit_epoch",
            (self.part_key.0, epoch),
        )
        .unwrap();
        // Find the most recent snapshot including the commited epoch
        // (since we can GC everything before that epoch). Then find
        // all less recent snapshots and delete those. So we never
        // want to delete a snapshot in the commited epoch, but since
        // the most recent snapshot is not deleted it's ok for this to
        // be `<=`.
        txn.execute(
            "WITH max_epoch_snapshots AS (
             SELECT step_id, state_key, MAX(snap_epoch) AS snap_epoch
             FROM snaps
             WHERE snap_epoch <= ?1
             GROUP BY step_id, state_key
             ),
             garbage_snapshots AS (
             SELECT step_id, state_key, snaps.snap_epoch
             FROM snaps
             JOIN max_epoch_snapshots USING (step_id, state_key)
             WHERE snaps.snap_epoch < max_epoch_snapshots.snap_epoch
             )
             DELETE FROM snaps
             WHERE (step_id, state_key, snap_epoch) IN garbage_snapshots",
            (epoch,),
        )
        .unwrap();
        txn.commit().unwrap();
    }
}

#[test]
fn gc_leaves_only_final_snap() {
    pyo3::prepare_freethreaded_python();
    let conn = Python::with_gil(|py| RecoveryPart::init_open_mem(py));
    conn.snap_writer().write_batch(vec![
        SerializedSnapshot(
            StepId(String::from("step_1")),
            StateKey(String::from("a")),
            SnapshotEpoch(1),
            Some("PICKLED_DATA1".as_bytes().to_vec()),
        ),
        SerializedSnapshot(
            StepId(String::from("step_1")),
            StateKey(String::from("a")),
            SnapshotEpoch(2),
            Some("PICKLED_DATA2".as_bytes().to_vec()),
        ),
        SerializedSnapshot(
            StepId(String::from("step_1")),
            StateKey(String::from("a")),
            SnapshotEpoch(5),
            Some("PICKLED_DATA5".as_bytes().to_vec()),
        ),
    ]);
    conn.committer(PartitionIndex(0)).commit(&5);

    let found = conn
        .conn
        .borrow()
        .prepare(
            "SELECT step_id, state_key, COUNT(*) AS num_snaps
            FROM snaps
            GROUP BY step_id, state_key
            HAVING num_snaps > 1",
        )
        .unwrap()
        .query_map((), |row| {
            let step_id = StepId(row.get(0)?);
            let state_key = StateKey(row.get(1)?);
            let num_snaps: usize = row.get(2)?;

            Ok((step_id, state_key, num_snaps))
        })
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    let expected = Vec::new();
    assert_eq!(found, expected);
}

create_exception!(
    bytewax.recovery,
    InconsistentPartitionsError,
    PyValueError,
    "Raised when two recovery partitions are from very different times.

Bytewax only keeps around state snapshots for the backup interval.
This means that if you are resuming a dataflow with one recovery
partition much newer than another, it's not possible to find a
consistent set of snapshots between them.

This is probably due to not restoring a consistent set of recovery
partition backups onto all workers or the backup process has been
continously failing on only some workers."
);

create_exception!(
    bytewax.recovery,
    NoPartitionsError,
    PyFileNotFoundError,
    "Raised when no recovery partitions are found on any worker.

This is probably due to the wrong recovery directory being specified."
);

create_exception!(
    bytewax.recovery,
    MissingPartitionsError,
    PyFileNotFoundError,
    "Raised when an incomplete set of recovery partitions is detected."
);

impl RecoveryPart {
    fn init(py: Python, file: &Path, index: PartitionIndex, count: PartitionCount) -> PyResult<()> {
        tracing::debug!("Init recovery partition {index:?} / {count:?} at {file:?}");
        let conn = Rc::new(RefCell::new(
            Connection::open_with_flags(
                file,
                OpenFlags::SQLITE_OPEN_READ_WRITE
                    | OpenFlags::SQLITE_OPEN_CREATE
                    | OpenFlags::SQLITE_OPEN_NO_MUTEX,
            )
            .reraise("can't open recovery DB")?,
        ));
        setup_conn(py, &conn);

        let _self = Self { conn };
        _self
            .part_writer()
            .write_batch(vec![PartitionMeta(index, count)]);

        Ok(())
    }

    fn open(py: Python, file: &Path) -> PyResult<Self> {
        tracing::debug!("Opening recovery partition at {file:?}");
        let conn = Rc::new(RefCell::new(
            Connection::open_with_flags(
                file,
                OpenFlags::SQLITE_OPEN_READ_WRITE | OpenFlags::SQLITE_OPEN_NO_MUTEX,
            )
            .reraise("can't open recovery DB")?,
        ));
        setup_conn(py, &conn);

        Ok(Self { conn })
    }

    fn init_open_mem(py: Python) -> Self {
        let conn = Rc::new(RefCell::new(Connection::open_in_memory().unwrap()));
        setup_conn(py, &conn);

        Self { conn }
    }

    fn part_writer(&self) -> PartitionMetaWriter {
        PartitionMetaWriter {
            conn: self.conn.clone(),
        }
    }

    fn ex_writer(&self) -> ExecutionMetaWriter {
        ExecutionMetaWriter {
            conn: self.conn.clone(),
        }
    }

    fn front_writer(&self) -> FrontierWriter {
        FrontierWriter {
            conn: self.conn.clone(),
        }
    }

    fn snap_writer(&self) -> SerializedSnapshotWriter {
        SerializedSnapshotWriter {
            conn: self.conn.clone(),
        }
    }

    fn commit_writer(&self) -> CommitWriter {
        CommitWriter {
            conn: self.conn.clone(),
        }
    }

    fn part_loader(&self) -> PartitionMetaLoader {
        PartitionMetaLoader::new(self.conn.clone())
    }

    fn ex_loader(&self) -> ExecutionMetaLoader {
        ExecutionMetaLoader::new(self.conn.clone())
    }

    fn front_loader(&self) -> FrontierLoader {
        FrontierLoader::new(self.conn.clone())
    }

    /// Will only read the most recent snapshot (epoch-wise) for each
    /// `(step_id, state_key)` from before the provided epoch.
    fn snap_loader(&self, before: ResumeEpoch) -> SerializedSnapshotLoader {
        // TODO: Do we need to futz with the batch size?
        SerializedSnapshotLoader::new(self.conn.clone(), before, 1000)
    }

    fn commit_loader(&self) -> CommitLoader {
        CommitLoader::new(self.conn.clone())
    }

    fn committer(&self, part_key: PartitionIndex) -> RecoveryCommitter {
        RecoveryCommitter {
            conn: self.conn.clone(),
            part_key,
        }
    }

    /// Calculate the resume execution and epoch. You must use this on
    /// a DB that has all progress data from all partitions written to
    /// it, so it must be an in-mem one during the resume from
    /// calculation.
    fn resume_from(&self) -> PyResult<ResumeFrom> {
        let mut conn = self.conn.borrow_mut();
        let txn = conn.transaction().unwrap();

        let part_counts = txn
            .prepare("SELECT DISTINCT(part_count) FROM parts")
            .unwrap()
            .query_map((), |row| Ok(PartitionCount(row.get(0)?)))
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
        if part_counts.is_empty() {
            let msg = "No recovery partitions found on any worker; can't resume";
            return Err(NoPartitionsError::new_err(msg));
        } else if part_counts.len() > 1 {
            let msg = "Inconsistent partition counts in recovery partitions; can't resume";
            return Err(PyValueError::new_err(msg));
        }

        let part_count = part_counts[0];
        let expected_parts: BTreeSet<_> = part_count.iter().collect();
        let found_parts = txn
            .prepare("SELECT part_index FROM parts")
            .unwrap()
            .query_map((), |row| Ok(PartitionIndex(row.get(0)?)))
            .unwrap()
            .collect::<Result<BTreeSet<_>, _>>()
            .unwrap();
        let missing_parts = &expected_parts - &found_parts;
        if !missing_parts.is_empty() {
            let msg = format!(
                "Missing recovery partitions {missing_parts:?} of {part_count}; can't resume"
            );
            return Err(MissingPartitionsError::new_err(msg));
        }

        let resume_from = txn
            .query_row(
                "WITH max_ex AS (
                 SELECT ex_num, worker_count, resume_epoch
                 FROM exs
                 WHERE ex_num = (SELECT MAX(ex_num) FROM exs)
                 ),
                 default_progress AS (
                 SELECT ex_num, value AS worker_index, resume_epoch AS worker_frontier
                 FROM generate_series(0, (SELECT worker_count - 1 FROM max_ex))
                 CROSS JOIN max_ex
                 ),
                 explicit_progress AS (
                 SELECT ex_num, worker_index, worker_frontier
                 FROM fronts
                 WHERE ex_num = (SELECT MAX(ex_num) FROM exs)
                 ),
                 max_progress AS (
                 SELECT ex_num, worker_index, MAX(worker_frontier) AS worker_frontier
                 FROM (SELECT * FROM explicit_progress UNION SELECT * FROM default_progress)
                 GROUP BY ex_num, worker_index
                 )
                 SELECT ex_num + 1, MIN(worker_frontier)
                 FROM max_progress",
                (),
                // `MIN(worker_frontier)` always returns a
                // single row with possible NULL, so we have
                // to handle the NULL within the row fn,
                // instead of assuming the result set might be
                // empty.
                |row| {
                    let ex_num = row.get::<_, Option<u64>>(0)?.map(ExecutionNumber);
                    let resume_epoch = row.get::<_, Option<u64>>(1)?.map(ResumeEpoch);
                    Ok(ex_num.zip(resume_epoch).map(|(en, re)| ResumeFrom(en, re)))
                },
            )
            .unwrap()
            .unwrap_or_default();

        let ResumeFrom(_resume_ex, resume_epoch) = resume_from;
        // These are partitions which for some reason have already
        // been GC'd and so might be missing data in the resume epoch.
        let state_missing_parts = txn
            .prepare("SELECT part_index FROM commits WHERE commit_epoch > ?1")
            .unwrap()
            .query_map((resume_epoch.0,), |row| Ok(PartitionIndex(row.get(0)?)))
            .unwrap()
            .collect::<Result<BTreeSet<_>, _>>()
            .unwrap();
        if !state_missing_parts.is_empty() {
            let delayed_parts = &expected_parts - &state_missing_parts;
            let msg = format!(
                "Recovery partitions {delayed_parts:?} of {part_count} are too old to resume from epoch {resume_epoch} without data loss; \
                 do you have a newer backup of these partitions?"
            );
            return Err(InconsistentPartitionsError::new_err(msg));
        }

        Ok(resume_from)
    }
}

#[test]
fn resume_from_only_parts() {
    pyo3::prepare_freethreaded_python();
    let conn = Python::with_gil(|py| RecoveryPart::init_open_mem(py));
    conn.part_writer()
        .write_batch(vec![PartitionMeta(PartitionIndex(0), PartitionCount(1))]);

    let found = conn.resume_from().unwrap();
    let expected = ResumeFrom(ExecutionNumber(0), ResumeEpoch(1));
    assert_eq!(found, expected);
}

#[test]
fn resume_from_all_explict_fronts() {
    pyo3::prepare_freethreaded_python();
    let conn = Python::with_gil(|py| RecoveryPart::init_open_mem(py));
    conn.part_writer()
        .write_batch(vec![PartitionMeta(PartitionIndex(0), PartitionCount(1))]);
    conn.ex_writer().write_batch(vec![ExecutionMeta(
        ExecutionNumber(1),
        WorkerCount(3),
        ResumeEpoch(11),
    )]);
    conn.front_writer().write_batch(vec![
        FrontierMeta(ExecutionNumber(1), WorkerIndex(0), WorkerFrontier(13)),
        FrontierMeta(ExecutionNumber(1), WorkerIndex(1), WorkerFrontier(12)),
        FrontierMeta(ExecutionNumber(1), WorkerIndex(2), WorkerFrontier(13)),
    ]);
    conn.commit_writer()
        .write_batch(vec![CommitMeta(PartitionIndex(0), 12)]);

    let found = conn.resume_from().unwrap();
    let expected = ResumeFrom(ExecutionNumber(2), ResumeEpoch(12));
    assert_eq!(found, expected);
}

#[test]
fn resume_from_default_fronts() {
    pyo3::prepare_freethreaded_python();
    let conn = Python::with_gil(|py| RecoveryPart::init_open_mem(py));
    conn.part_writer()
        .write_batch(vec![PartitionMeta(PartitionIndex(0), PartitionCount(1))]);
    conn.ex_writer().write_batch(vec![ExecutionMeta(
        ExecutionNumber(1),
        WorkerCount(3),
        ResumeEpoch(11),
    )]);
    conn.front_writer().write_batch(vec![
        FrontierMeta(ExecutionNumber(1), WorkerIndex(0), WorkerFrontier(13)),
        FrontierMeta(ExecutionNumber(1), WorkerIndex(2), WorkerFrontier(13)),
    ]);
    conn.commit_writer()
        .write_batch(vec![CommitMeta(PartitionIndex(0), 11)]);

    let found = conn.resume_from().unwrap();
    let expected = ResumeFrom(ExecutionNumber(2), ResumeEpoch(11));
    assert_eq!(found, expected);
}

#[test]
fn resume_from_inconsistent_error() {
    pyo3::prepare_freethreaded_python();
    Python::with_gil(|py| {
        let conn = RecoveryPart::init_open_mem(py);
        conn.part_writer().write_batch(vec![
            PartitionMeta(PartitionIndex(0), PartitionCount(2)),
            PartitionMeta(PartitionIndex(1), PartitionCount(2)),
        ]);
        conn.ex_writer().write_batch(vec![ExecutionMeta(
            ExecutionNumber(1),
            WorkerCount(3),
            ResumeEpoch(11),
        )]);
        conn.front_writer().write_batch(vec![
            FrontierMeta(ExecutionNumber(1), WorkerIndex(0), WorkerFrontier(13)),
            FrontierMeta(ExecutionNumber(1), WorkerIndex(2), WorkerFrontier(13)),
        ]);
        conn.commit_writer().write_batch(vec![
            CommitMeta(PartitionIndex(0), 12),
            CommitMeta(PartitionIndex(1), 12),
        ]);

        let found = conn.resume_from().unwrap_err();
        assert!(found.is_instance_of::<InconsistentPartitionsError>(py));
    });
}

/// Create and init a set of empty recovery partitions.
///
/// :arg db_dir: Local directory to create partitions in.
///
/// :type db_dir: pathlib.Path
///
/// :arg count: Number of partitions to create.
///
/// :type count: int
#[pyfunction]
fn init_db_dir(py: Python, db_dir: PathBuf, count: PartitionCount) -> PyResult<()> {
    tracing::warn!("Creating {count:?} recovery partitions in {db_dir:?}");
    if !db_dir.is_dir() {
        return Err(PyFileNotFoundError::new_err(format!(
            "recovery directory {:?} does not exist; please create it with `mkdir`",
            db_dir
        )));
    }
    for index in count.iter() {
        let part_file = db_dir.join(format!("part-{}.sqlite3", index.0));
        RecoveryPart::init(py, &part_file, index, count)
            .reraise("error init-ing recovery partition")?;
    }
    Ok(())
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
                                FrontierMeta(ex_num, worker_index, WorkerFrontier(frontier_epoch));
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

trait SerializeSnapshotOp<S>
where
    S: Scope<Timestamp = u64>,
{
    /// Serialize state snapshots using the provided serde.
    ///
    /// Although the [`StepId`] and [`StateKey`] are both already
    /// within the [`SerializedSnapshot`], duplicate them in the key
    /// position so we can partition and route on them.
    fn ser_snap(&self) -> Stream<S, ((StepId, StateKey), SerializedSnapshot)>;
}

impl<S> SerializeSnapshotOp<S> for Stream<S, Snapshot>
where
    S: Scope<Timestamp = u64>,
{
    fn ser_snap(&self) -> Stream<S, ((StepId, StateKey), SerializedSnapshot)> {
        // Effectively map-with-epoch.
        self.unary(Pipeline, "ser_snap", move |_init_cap, _info| {
            let mut inbuf = Vec::new();
            let pickle = Python::with_gil(|py| unwrap_any!(py.import("pickle")).unbind());

            move |snaps_input, ser_snaps_output| {
                snaps_input.for_each(|cap, incoming| {
                    incoming.swap(&mut inbuf);

                    let epoch = cap.time();
                    Python::with_gil(|py| {
                        let ser_snaps =
                            inbuf
                                .drain(..)
                                .map(|Snapshot(step_id, state_key, snap_change)| {
                                    let ser_change = match snap_change {
                                        StateChange::Upsert(snap) => {
                                            let snap = PyObject::from(snap);
                                            let bytes = unwrap_any!(|| -> PyResult<Vec<u8>> {
                                                Ok(pickle
                                                    .bind(py)
                                                    .call_method1(
                                                        intern!(py, "dumps"),
                                                        (snap.bind(py),),
                                                    )?
                                                    .downcast::<PyBytes>()?
                                                    .as_bytes()
                                                    .to_vec())
                                            }(
                                            ));
                                            Some(bytes)
                                        }
                                        StateChange::Discard => None,
                                    };

                                    let snap_epoch = SnapshotEpoch(*epoch);
                                    let ser_snap = SerializedSnapshot(
                                        step_id.clone(),
                                        state_key.clone(),
                                        snap_epoch,
                                        ser_change,
                                    );
                                    let key = (step_id, state_key);

                                    (key, ser_snap)
                                });
                        ser_snaps_output.session(&cap).give_iterator(ser_snaps);
                    });
                });
            }
        })
    }
}

trait DeserializeSnapshotOp<S>
where
    S: Scope,
{
    /// Deserialize state snapshots using the provided serde.
    fn de_snap(&self) -> Stream<S, Snapshot>;
}

impl<S> DeserializeSnapshotOp<S> for Stream<S, SerializedSnapshot>
where
    S: Scope,
{
    fn de_snap(&self) -> Stream<S, Snapshot> {
        self.map(
            move |SerializedSnapshot(step_id, state_key, _snap_epoch, ser_change)| {
                let snap_change = match ser_change {
                    Some(ser_snap) => {
                        let snap = unwrap_any!(Python::with_gil(|py| -> PyResult<PyObject> {
                            let pickle = py.import("pickle")?;
                            Ok(pickle
                                .call_method1(intern!(py, "loads"), (PyBytes::new(py, &ser_snap),))?
                                .unbind())
                        }));
                        StateChange::Upsert(snap.into())
                    }
                    None => StateChange::Discard,
                };

                Snapshot(step_id, state_key, snap_change)
            },
        )
    }
}

pub(crate) trait LoadSnapsOp<S>
where
    S: Scope<Timestamp = u64>,
{
    /// Read state data from the recovery partitions into the production dataflow.
    ///
    /// You will still need to route it to the correct steps. See
    /// [`FilterSnapsOp::filter_snaps`].
    ///
    /// This will dump all state in the epoch of the snapshot so that
    /// operators can load in epoch order.
    fn load_snaps(&mut self, before: ResumeEpoch, bundle: RecoveryBundle) -> Stream<S, Snapshot>
    where
        S: Scope;
}

impl<S> LoadSnapsOp<S> for S
where
    S: Scope<Timestamp = u64>,
{
    fn load_snaps(&mut self, before: ResumeEpoch, bundle: RecoveryBundle) -> Stream<S, Snapshot> {
        let mut new_part = bundle.new_builder();

        self.partd_load(
            String::from("load_snaps"),
            bundle.local_parts(),
            move |part| new_part(part).borrow().snap_loader(before),
            S::Timestamp::minimum(),
        )
        .delay(
            |SerializedSnapshot(_step_id, _state_key, snap_epoch, _ser_change), _load_epoch| {
                snap_epoch.0
            },
        )
        .de_snap()
    }
}

pub(crate) trait FilterSnapsOp<S>
where
    S: Scope,
{
    /// Filter a stream of snapshots to just one for this step.
    ///
    /// In general, you'll use this within a stateful operator.
    ///
    /// This strips out all the extraneous data from the snapshot.
    fn filter_snaps(&self, for_step: StepId) -> Stream<S, (StateKey, StateChange)>;
}

impl<S> FilterSnapsOp<S> for Stream<S, Snapshot>
where
    S: Scope,
{
    fn filter_snaps(&self, for_step: StepId) -> Stream<S, (StateKey, StateChange)> {
        self.flat_map(move |Snapshot(step_id, state_key, snap_change)| {
            if step_id == for_step {
                Some((state_key, snap_change))
            } else {
                None
            }
        })
    }
}

pub(crate) trait RecoveryWriteOp<S>
where
    S: Scope<Timestamp = u64>,
{
    /// Write out a stream of all snapshot data being produced by all
    /// stateful steps in a dataflow. This is basically the entire
    /// production dataflow recovery system.
    ///
    /// You'll add this on at the end of the production dataflow.
    ///
    /// Probe the downstream clock to rate limit the dataflow.
    fn write_recovery(
        &self,
        resume_from: ResumeFrom,
        bundle: RecoveryBundle,
        epoch_interval: EpochInterval,
        backup_interval: BackupInterval,
    ) -> ClockStream<S>;
}

impl<S> RecoveryWriteOp<S> for Stream<S, Snapshot>
where
    S: Scope<Timestamp = u64>,
{
    fn write_recovery(
        &self,
        resume_from: ResumeFrom,
        bundle: RecoveryBundle,
        epoch_interval: EpochInterval,
        backup_interval: BackupInterval,
    ) -> ClockStream<S> {
        let scope = self.scope();
        let local_parts = bundle.local_parts();

        let mut new_ex_part = bundle.new_builder();

        let ResumeFrom(ex_num, resume_epoch) = resume_from;
        let write_ex_clock = Some((ex_num, ExecutionMeta(ex_num, scope.w_count(), resume_epoch)))
            .into_stream_once_at(&scope, resume_epoch.0)
            .partd_write(
                String::from("recovery_ex_writer"),
                local_parts.clone(),
                BuildHasherDefault::<SeaHasher>::default(),
                move |part_key| {
                    let part = new_ex_part(part_key);
                    let writer = part.borrow().ex_writer();
                    writer
                },
            );

        let mut new_snap_part = bundle.new_builder();
        let mut new_front_part = bundle.new_builder();
        let mut new_commit_part = bundle.new_builder();

        let write_snap_clock = self.ser_snap().partd_write(
            String::from("recovery_snap_writer"),
            local_parts.clone(),
            BuildHasherDefault::<SeaHasher>::default(),
            move |part_key| {
                let part = new_snap_part(part_key);
                let writer = part.borrow().snap_writer();
                writer
            },
        );

        write_ex_clock
            .concat(&write_snap_clock)
            // We do not have to monitor output progress because that
            // only makes sense on stateful outputs and their progress
            // will be captured in the snapshot stream already.
            .frontier(resume_from)
            .partd_write(
                String::from("recovery_front_writer"),
                local_parts.clone(),
                BuildHasherDefault::<SeaHasher>::default(),
                move |part_key| {
                    let part = new_front_part(part_key);
                    let writer = part.borrow().front_writer();
                    writer
                },
            )
            .broadcast()
            .partd_commit(
                String::from("recovery_committer"),
                local_parts,
                move |part_key| {
                    let part = new_commit_part(part_key);
                    let committer = part.borrow().committer(*part_key);
                    committer
                },
                epoch_interval.epochs_per(backup_interval.0),
            )
    }
}

pub(crate) type ProgressStream<S> = (
    Stream<S, PartitionMeta>,
    Stream<S, ExecutionMeta>,
    Stream<S, FrontierMeta>,
    Stream<S, CommitMeta>,
);

pub(crate) trait ReadProgressOp {
    /// Read all progress data into a dataflow.
    ///
    /// This'll be used in the calculate resume dataflow.
    fn read_progress<S>(self, scope: &mut S) -> ProgressStream<S>
    where
        S: Scope<Timestamp = u64>;
}

impl ReadProgressOp for RecoveryBundle {
    fn read_progress<S>(self, scope: &mut S) -> ProgressStream<S>
    where
        S: Scope<Timestamp = u64>,
    {
        let mut new_part_part = self.new_builder();
        let mut new_ex_part = self.new_builder();
        let mut new_front_part = self.new_builder();
        let mut new_commit_part = self.new_builder();
        let parts = scope.partd_load(
            String::from("recovery_part_loader"),
            self.local_parts(),
            move |part| new_part_part(part).borrow().part_loader(),
            S::Timestamp::minimum(),
        );
        let exs = scope.partd_load(
            String::from("recovery_ex_loader"),
            self.local_parts(),
            move |part| new_ex_part(part).borrow().ex_loader(),
            S::Timestamp::minimum(),
        );
        let fronts = scope.partd_load(
            String::from("recovery_front_loader"),
            self.local_parts(),
            move |part| new_front_part(part).borrow().front_loader(),
            S::Timestamp::minimum(),
        );
        let commits = scope.partd_load(
            String::from("recovery_commit_loader"),
            self.local_parts(),
            move |part| new_commit_part(part).borrow().commit_loader(),
            S::Timestamp::minimum(),
        );
        (parts, exs, fronts, commits)
    }
}

/// Use to calculate [`ResumeEpoch`] with
/// [`ResumeFromOp::resume_from`].
pub(crate) struct ResumeCalc(RecoveryPart);

impl ResumeCalc {
    pub(crate) fn new(py: Python) -> Self {
        Self(RecoveryPart::init_open_mem(py))
    }

    pub(crate) fn resume_from(&self) -> PyResult<ResumeFrom> {
        self.0.resume_from()
    }
}

pub(crate) trait ResumeFromOp<S>
where
    S: Scope<Timestamp = u64>,
{
    /// Read in streams of progress data and at the end of each epoch
    /// emit the calculated resume from.
    ///
    /// This'll be used in the calculate resume dataflow. Since all
    /// progress data is read in a single epoch in that dataflow, this
    /// works.
    fn resume_from(
        &self,
        parts: &Stream<S, PartitionMeta>,
        exs: &Stream<S, ExecutionMeta>,
        fronts: &Stream<S, FrontierMeta>,
        commits: &Stream<S, CommitMeta>,
        resume_calc: Rc<RefCell<ResumeCalc>>,
    ) -> Stream<S, ()>;
}

impl<S> ResumeFromOp<S> for S
where
    S: Scope<Timestamp = u64>,
{
    fn resume_from(
        &self,
        parts: &Stream<S, PartitionMeta>,
        exs: &Stream<S, ExecutionMeta>,
        fronts: &Stream<S, FrontierMeta>,
        commits: &Stream<S, CommitMeta>,
        resume_calc: Rc<RefCell<ResumeCalc>>,
    ) -> Stream<S, ()> {
        let mut op_builder = OperatorBuilder::new(String::from("resume_from"), self.clone());

        let mut parts_input = op_builder.new_input(parts, Pipeline);
        let mut exs_input = op_builder.new_input(exs, Pipeline);
        let mut fronts_input = op_builder.new_input(fronts, Pipeline);
        let mut commits_input = op_builder.new_input(commits, Pipeline);

        let (mut resume_from_output, resume_from) = op_builder.new_output();

        op_builder.build(move |init_caps| {
            let mut parts_inbuf = InBuffer::new();
            let mut exs_inbuf = InBuffer::new();
            let mut fronts_inbuf = InBuffer::new();
            let mut commits_inbuf = InBuffer::new();
            let mut ncater = EagerNotificator::new(init_caps, resume_calc);

            move |input_frontiers| {
                parts_input.buffer_notify(&mut parts_inbuf, &mut ncater);
                exs_input.buffer_notify(&mut exs_inbuf, &mut ncater);
                fronts_input.buffer_notify(&mut fronts_inbuf, &mut ncater);
                commits_input.buffer_notify(&mut commits_inbuf, &mut ncater);

                ncater.for_each(
                    input_frontiers,
                    |caps, resume_calc| {
                        let cap = &caps[0];
                        let epoch = cap.time();

                        let in_mem = &resume_calc.borrow().0;

                        if let Some(parts) = parts_inbuf.remove(epoch) {
                            let mut part_writer = in_mem.part_writer();
                            part_writer.write_batch(parts);
                        }
                        if let Some(exs) = exs_inbuf.remove(epoch) {
                            let mut ex_writer = in_mem.ex_writer();
                            ex_writer.write_batch(exs);
                        }
                        if let Some(fronts) = fronts_inbuf.remove(epoch) {
                            let mut front_writer = in_mem.front_writer();
                            front_writer.write_batch(fronts);
                        }
                        if let Some(commits) = commits_inbuf.remove(epoch) {
                            let mut commit_writer = in_mem.commit_writer();
                            commit_writer.write_batch(commits);
                        }
                    },
                    |caps, _in_mem| {
                        let cap = &caps[0];

                        resume_from_output.activate().session(cap).give(());
                    },
                );
            }
        });

        resume_from
    }
}

pub(crate) fn register(py: Python, m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(init_db_dir, m)?)?;
    m.add_class::<RecoveryConfig>()?;
    m.add(
        "InconsistentPartitionsError",
        py.get_type::<InconsistentPartitionsError>(),
    )?;
    m.add(
        "MissingPartitionsError",
        py.get_type::<MissingPartitionsError>(),
    )?;
    m.add("NoPartitionsError", py.get_type::<NoPartitionsError>())?;
    Ok(())
}
