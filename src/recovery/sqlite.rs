use std::borrow::Cow;
use std::path::Path;
use std::path::PathBuf;

use futures::StreamExt;
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;

use sqlx::encode::IsNull;
use sqlx::error::BoxDynError;
use sqlx::query;
use sqlx::sqlite::SqliteArgumentValue;
use sqlx::sqlite::SqliteConnectOptions;
use sqlx::sqlite::SqliteRow;
use sqlx::sqlite::SqliteTypeInfo;
use sqlx::sqlite::SqliteValueRef;
use sqlx::Connection;
use sqlx::Decode;
use sqlx::Encode;
use sqlx::Row;
use sqlx::Sqlite;
use sqlx::SqliteConnection;
use sqlx::Type;
use tokio::runtime::Runtime;

use crate::recovery::Progress;
use crate::recovery::ProgressOp;
use crate::recovery::ProgressRecoveryKey;
use crate::recovery::State;
use crate::recovery::StateOp;

use super::ProgressReader;
use super::ProgressUpdate;
use super::ProgressWriter;
use super::RecoveryConfig;
use super::StateBytes;
use super::StateCollector;
use super::StateKey;
use super::StateReader;
use super::StateRecoveryKey;
use super::StateUpdate;
use super::StateWriter;
use super::StepId;
use super::WorkerIndex;

/// Use [SQLite](https://sqlite.org/index.html) to store recovery
/// data.
///
/// Creates a SQLite DB per-worker in a given directory. Multiple DBs
/// are used to allow workers to write without contention.
///
/// Use a distinct directory per dataflow so recovery data is not
/// mixed.
///
/// >>> from bytewax.execution import run_main
/// >>> from bytewax.inputs import TestingInputConfig
/// >>> from bytewax.outputs import StdOutputConfig
/// >>> flow = Dataflow()
/// >>> flow.input("inp", TestingInputConfig(range(3)))
/// >>> flow.capture(StdOutputConfig())
/// >>> tmp_dir = TemporaryDirectory()  # We'll store this somewhere temporary for this test.
/// >>> recovery_config = SqliteRecoveryConfig(tmp_dir)
/// >>> run_main(
/// ...     flow,
/// ...     recovery_config=recovery_config,
/// ... )  # doctest: +ELLIPSIS
/// (...)
///
/// DB files and tables will automatically be created if there's no
/// previous recovery data.
///
/// Args:
///
///   db_dir (Path): Existing directory to store per-worker DBs
///       in. Must be distinct per-dataflow. DB files will have
///       names like `"worker0.sqlite3"`. You can use `"."` for the
///       current directory.
///
/// Returns:
///
///   Config object. Pass this as the `recovery_config` argument to
///   your execution entry point.
#[pyclass(module="bytewax.recovery", extends=RecoveryConfig)]
#[pyo3(text_signature = "(db_dir)")]
pub(crate) struct SqliteRecoveryConfig {
    #[pyo3(get)]
    db_dir: PathBuf,
}

#[pymethods]
impl SqliteRecoveryConfig {
    #[new]
    #[args(db_dir)]
    pub(crate) fn new(db_dir: PathBuf) -> (Self, RecoveryConfig) {
        (Self { db_dir }, RecoveryConfig {})
    }

    /// Pickle as a tuple.
    fn __getstate__(&self) -> (&str, PathBuf) {
        ("SqliteRecoveryConfig", self.db_dir.clone())
    }

    /// Egregious hack because pickling assumes the type has "empty"
    /// mutable objects.
    ///
    /// Pickle always calls `__new__(*__getnewargs__())` but notice we
    /// don't have access to the pickled `db_file_path` yet, so we
    /// have to pass in some dummy string value that will be
    /// overwritten by `__setstate__()` shortly.
    fn __getnewargs__(&self) -> (&str,) {
        ("UNINIT_PICKLED_STRING",)
    }

    /// Unpickle from tuple of arguments.
    fn __setstate__(&mut self, state: &PyAny) -> PyResult<()> {
        if let Ok(("SqliteRecoveryConfig", db_dir)) = state.extract() {
            self.db_dir = db_dir;
            Ok(())
        } else {
            Err(PyValueError::new_err(format!(
                "bad pickle contents for SqliteRecoveryConfig: {state:?}"
            )))
        }
    }
}

impl SqliteRecoveryConfig {
    pub(crate) fn db_file(&self, worker_index: usize) -> PathBuf {
        self.db_dir.join(format!("worker{worker_index}.sqlite3"))
    }
}

pub(crate) struct SqliteStateWriter {
    rt: Runtime,
    conn: SqliteConnection,
    table_name: String,
}

impl SqliteStateWriter {
    pub(crate) fn new(db_file: &Path) -> Self {
        let table_name = "state".to_string();

        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();

        let mut options = SqliteConnectOptions::new().filename(db_file);
        options = options.create_if_missing(true);
        let future = SqliteConnection::connect_with(&options);
        let mut conn = rt.block_on(future).unwrap();
        tracing::debug!("Opened Sqlite connection to {db_file:?}");

        // TODO: SQLite doesn't let you bind to table names. Can
        // we do this in a slightly safer way? I'm not as worried
        // because this value is not from items in the dataflow
        // stream, but from the config which should be under
        // developer control.
        let sql = format!("CREATE TABLE IF NOT EXISTS {table_name} (step_id TEXT, state_key TEXT, epoch INTEGER, snapshot BLOB, next_awake TEXT, PRIMARY KEY (step_id, state_key, epoch));");
        let future = query(&sql).execute(&mut conn);
        rt.block_on(future).unwrap();

        Self {
            rt,
            conn,
            table_name,
        }
    }
}

impl Type<Sqlite> for StepId {
    fn type_info() -> SqliteTypeInfo {
        <String as Type<Sqlite>>::type_info()
    }
}

impl<'q> Encode<'q, Sqlite> for StepId {
    fn encode(self, args: &mut Vec<SqliteArgumentValue<'q>>) -> IsNull {
        args.push(SqliteArgumentValue::Text(Cow::Owned(self.0)));
        IsNull::No
    }

    fn encode_by_ref(&self, args: &mut Vec<SqliteArgumentValue<'q>>) -> IsNull {
        args.push(SqliteArgumentValue::Text(Cow::Owned(self.0.clone())));
        IsNull::No
    }
}

impl<'r> Decode<'r, Sqlite> for StepId {
    fn decode(value: SqliteValueRef<'r>) -> Result<Self, BoxDynError> {
        let value = <String as Decode<Sqlite>>::decode(value)?;
        Ok(Self(value))
    }
}

impl Type<Sqlite> for StateKey {
    fn type_info() -> SqliteTypeInfo {
        <String as Type<Sqlite>>::type_info()
    }
}

/// sqlx doesn't support the fact that SQLite lets you have weakly
/// typed columns, so we can't store an int when it's worker index
/// (for input component state) and a string when it's a generic
/// hash. Instead do a light "encoding" of the type. We could break
/// out full serde JSON for this, but it seems like overkill and would
/// make querying the table via SQL harder.
impl<'q> Encode<'q, Sqlite> for StateKey {
    fn encode_by_ref(&self, args: &mut Vec<SqliteArgumentValue<'q>>) -> IsNull {
        let value = match self {
            Self::Hash(string) => {
                format!("H:{string}")
            }
            Self::Worker(worker_index) => {
                format!("W:{}", worker_index.0)
            }
        };
        args.push(SqliteArgumentValue::Text(Cow::Owned(value)));
        IsNull::No
    }
}

impl<'r> Decode<'r, Sqlite> for StateKey {
    fn decode(value: SqliteValueRef<'r>) -> Result<Self, BoxDynError> {
        let value = <String as Decode<Sqlite>>::decode(value)?;
        if let Some(string) = value.strip_prefix("H:") {
            Ok(Self::Hash(string.to_string()))
        } else if let Some(worker_index_str) = value.strip_prefix("W:") {
            Ok(Self::Worker(WorkerIndex(worker_index_str.parse()?)))
        } else {
            panic!("Un-parseable state_key: {value:?}");
        }
    }
}

impl Type<Sqlite> for StateBytes {
    fn type_info() -> SqliteTypeInfo {
        <Vec<u8> as Type<Sqlite>>::type_info()
    }
}

impl<'q> Encode<'q, Sqlite> for StateBytes {
    fn encode(self, args: &mut Vec<SqliteArgumentValue<'q>>) -> IsNull {
        args.push(SqliteArgumentValue::Blob(Cow::Owned(self.0)));
        IsNull::No
    }

    fn encode_by_ref(&self, args: &mut Vec<SqliteArgumentValue<'q>>) -> IsNull {
        args.push(SqliteArgumentValue::Blob(Cow::Owned(self.0.clone())));
        IsNull::No
    }
}

impl<'r> Decode<'r, Sqlite> for StateBytes {
    fn decode(value: SqliteValueRef<'r>) -> Result<Self, BoxDynError> {
        let value = <Vec<u8> as Decode<Sqlite>>::decode(value)?;
        Ok(Self(value))
    }
}

impl Type<Sqlite> for WorkerIndex {
    fn type_info() -> SqliteTypeInfo {
        // For some reason this does not like using i64 /
        // SqliteArgumentValue::Int64. We get a similar error to
        // https://github.com/launchbadge/sqlx/issues/2093. Maybe
        // SQLite bug?
        <i32 as Type<Sqlite>>::type_info()
    }
}

impl<'q> Encode<'q, Sqlite> for WorkerIndex {
    fn encode_by_ref(&self, args: &mut Vec<SqliteArgumentValue<'q>>) -> IsNull {
        args.push(SqliteArgumentValue::Int(self.0 as i32));
        IsNull::No
    }
}

impl<'r> Decode<'r, Sqlite> for WorkerIndex {
    fn decode(value: SqliteValueRef<'r>) -> Result<Self, BoxDynError> {
        let value = <i32 as Decode<Sqlite>>::decode(value)?;
        Ok(Self(value as usize))
    }
}

impl StateWriter<u64> for SqliteStateWriter {
    #[tracing::instrument(name = "SqliteStateWriter.write", level = "trace", skip_all)]
    fn write(&mut self, update: &StateUpdate<u64>) {
        let StateUpdate(recovery_key, op) = update;
        let StateRecoveryKey {
            step_id,
            state_key,
            epoch,
        } = recovery_key;
        let (snapshot, next_awake) = match op {
            StateOp::Upsert(State {
                snapshot,
                next_awake,
            }) => (Some(snapshot), next_awake.as_ref()),
            StateOp::Discard => (None, None),
        };

        let sql = format!("INSERT INTO {} (step_id, state_key, epoch, snapshot, next_awake) VALUES (?1, ?2, ?3, ?4, ?5) ON CONFLICT (step_id, state_key, epoch) DO UPDATE SET snapshot = EXCLUDED.snapshot, next_awake = EXCLUDED.next_awake", self.table_name);
        let future = query(&sql)
            .bind(step_id)
            .bind(state_key)
            .bind(<u64 as TryInto<i64>>::try_into(*epoch).expect("epoch can't fit into SQLite int"))
            // Remember, reset state is stored as an explicit NULL in the
            // DB.
            .bind(snapshot)
            .bind(next_awake)
            .execute(&mut self.conn);
        self.rt.block_on(future).unwrap();

        tracing::trace!("Wrote state update {update:?}");
    }
}

impl StateCollector<u64> for SqliteStateWriter {
    #[tracing::instrument(name = "SqliteStateWriter.delete", level = "trace", skip_all)]
    fn delete(&mut self, recovery_key: &StateRecoveryKey<u64>) {
        let StateRecoveryKey {
            step_id,
            state_key,
            epoch,
        } = recovery_key;
        let sql = format!(
            "DELETE FROM {} WHERE step_id = ?1 AND state_key = ?2 AND epoch = ?3",
            self.table_name
        );
        let future = query(&sql)
            .bind(step_id)
            .bind(state_key)
            .bind(<u64 as TryInto<i64>>::try_into(*epoch).expect("epoch can't fit into SQLite int"))
            .execute(&mut self.conn);
        self.rt.block_on(future).unwrap();

        tracing::trace!("Deleted state for {recovery_key:?}");
    }
}

pub(crate) struct SqliteStateReader {
    rt: Runtime,
    rx: tokio::sync::mpsc::Receiver<StateUpdate<u64>>,
}

impl SqliteStateReader {
    pub(crate) fn new(db_file: &Path) -> Self {
        let table_name = "state";

        // Bootstrap off writer to get table creation.
        let writer = SqliteStateWriter::new(db_file);
        let rt = writer.rt;
        let mut conn = writer.conn;

        let (tx, rx) = tokio::sync::mpsc::channel(1);

        rt.spawn(async move {
            let sql = format!(
                "SELECT step_id, state_key, epoch, snapshot, next_awake FROM {table_name} ORDER BY epoch ASC"
            );
            let mut stream = query(&sql)
                .map(|row: SqliteRow| {
                    let recovery_key = StateRecoveryKey {
                        step_id: row.get(0),
                        state_key: row.get(1),
                        epoch: row
                            .get::<i64, _>(2)
                            .try_into()
                            .expect("SQLite int can't fit into epoch; might be negative"),
                    };
                    let op = match (row.get(3), row.get(4)) {
                        (None, Some(_)) => panic!("Missing snapshot in reading state SQLite table"),
                        (Some(snapshot), next_awake) => StateOp::Upsert(State { snapshot, next_awake }),
                        (None, None) => StateOp::Discard,
                    };
                    StateUpdate(recovery_key, op)
                })
                .fetch(&mut conn)
                .map(|result| result.expect("Error selecting from SQLite"));

            while let Some(update) = stream.next().await {
                tracing::trace!("Read state update {update:?}");
                tx.send(update).await.unwrap();
            }
        });

        Self { rt, rx }
    }
}

impl StateReader<u64> for SqliteStateReader {
    #[tracing::instrument(name = "SqliteStateReader.read", level = "trace", skip_all)]
    fn read(&mut self) -> Option<StateUpdate<u64>> {
        self.rt.block_on(self.rx.recv())
    }
}

pub(crate) struct SqliteProgressWriter {
    rt: Runtime,
    conn: SqliteConnection,
    table_name: String,
}

impl SqliteProgressWriter {
    pub(crate) fn new(db_file: &Path) -> Self {
        let table_name = "progress".to_string();

        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();

        let mut options = SqliteConnectOptions::new().filename(db_file);
        options = options.create_if_missing(true);
        let future = SqliteConnection::connect_with(&options);
        let mut conn = rt.block_on(future).unwrap();
        tracing::debug!("Opened Sqlite connection to {db_file:?}");

        let sql = format!(
            "CREATE TABLE IF NOT EXISTS {table_name} (worker_index INTEGER PRIMARY KEY, frontier INTEGER);"
        );
        let future = query(&sql).execute(&mut conn);
        rt.block_on(future).unwrap();

        Self {
            rt,
            conn,
            table_name,
        }
    }
}

impl ProgressWriter<u64> for SqliteProgressWriter {
    #[tracing::instrument(name = "SqliteProgressWriter.write", level = "trace", skip_all)]
    fn write(&mut self, update: &ProgressUpdate<u64>) {
        let ProgressUpdate(key, op) = update;
        let ProgressRecoveryKey { worker_index } = key;
        let ProgressOp::Upsert(Progress { frontier }) = op;
        let sql = format!("INSERT INTO {} (worker_index, frontier) VALUES (?1, ?2) ON CONFLICT (worker_index) DO UPDATE SET frontier = EXCLUDED.frontier", self.table_name);
        let future = query(&sql)
            .bind(worker_index)
            .bind(
                <u64 as TryInto<i64>>::try_into(*frontier)
                    .expect("frontier can't fit into SQLite int"),
            )
            .execute(&mut self.conn);
        self.rt.block_on(future).unwrap();

        tracing::trace!("Wrote progress update {update:?}");
    }
}

pub(crate) struct SqliteProgressReader {
    rt: Runtime,
    rx: tokio::sync::mpsc::Receiver<ProgressUpdate<u64>>,
}

impl SqliteProgressReader {
    pub(crate) fn new(db_file: &Path) -> Self {
        let table_name = "progress";

        let writer = SqliteProgressWriter::new(db_file);
        let rt = writer.rt;
        let mut conn = writer.conn;

        let (tx, rx) = tokio::sync::mpsc::channel(1);

        rt.spawn(async move {
            let sql = format!("SELECT worker_index, frontier FROM {table_name}");
            let mut stream = query(&sql)
                .map(|row: SqliteRow| {
                    let key = ProgressRecoveryKey {
                        worker_index: row.get(0),
                    };
                    let op = ProgressOp::Upsert(Progress {
                        frontier: row
                            .get::<i64, _>(1)
                            .try_into()
                            .expect("SQLite int can't fit into frontier; might be negative"),
                    });
                    ProgressUpdate(key, op)
                })
                .fetch(&mut conn)
                .map(|result| result.expect("Error selecting from SQLite"));

            while let Some(update) = stream.next().await {
                tracing::trace!("Read progress update {update:?}");
                tx.send(update).await.unwrap();
            }
        });

        Self { rt, rx }
    }
}

impl ProgressReader<u64> for SqliteProgressReader {
    #[tracing::instrument(name = "SqliteProgressReader.read", level = "trace", skip_all)]
    fn read(&mut self) -> Option<ProgressUpdate<u64>> {
        self.rt.block_on(self.rx.recv())
    }
}
