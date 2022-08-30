use std::borrow::Cow;
use std::path::Path;
use std::path::PathBuf;

use futures::StreamExt;
use log::debug;
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;

use serde::Deserialize;
use serde::Serialize;
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

use super::FrontierBackup;
use super::ProgressReader;
use super::ProgressWriter;
use super::RecoveryConfig;
use super::StateBackup;
use super::StateCollector;
use super::StateKey;
use super::StateReader;
use super::StateUpdate;
use super::StateWriter;
use super::StepId;
use super::{from_bytes, to_bytes, RecoveryKey};

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
        debug!("Opened Sqlite connection to {db_file:?}");

        // TODO: SQLite doesn't let you bind to table names. Can
        // we do this in a slightly safer way? I'm not as worried
        // because this value is not from items in the dataflow
        // stream, but from the config which should be under
        // developer control.
        let sql = format!("CREATE TABLE IF NOT EXISTS {table_name} (step_id, key, epoch INTEGER, state, PRIMARY KEY (step_id, key, epoch));");
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
        <&[u8] as Type<Sqlite>>::type_info()
    }
}

impl<'q> Encode<'q, Sqlite> for StateKey {
    fn encode_by_ref(&self, args: &mut Vec<SqliteArgumentValue<'q>>) -> IsNull {
        let bytes = to_bytes(self);
        args.push(SqliteArgumentValue::Blob(Cow::Owned(bytes)));
        IsNull::No
    }
}

impl<'r> Decode<'r, Sqlite> for StateKey {
    fn decode(value: SqliteValueRef<'r>) -> Result<Self, BoxDynError> {
        let value = <&[u8] as Decode<Sqlite>>::decode(value)?;
        Ok(from_bytes(value))
    }
}

impl Type<Sqlite> for StateUpdate {
    fn type_info() -> SqliteTypeInfo {
        <&[u8] as Type<Sqlite>>::type_info()
    }
}

impl<'q> Encode<'q, Sqlite> for StateUpdate {
    fn encode_by_ref(&self, args: &mut Vec<SqliteArgumentValue<'q>>) -> IsNull {
        let bytes = to_bytes(self);
        args.push(SqliteArgumentValue::Blob(Cow::Owned(bytes)));
        IsNull::No
    }
}

impl<'r> Decode<'r, Sqlite> for StateUpdate {
    fn decode(value: SqliteValueRef<'r>) -> Result<Self, BoxDynError> {
        let value = <&[u8] as Decode<Sqlite>>::decode(value)?;
        Ok(from_bytes(value))
    }
}

impl<T> Type<Sqlite> for FrontierBackup<T> {
    fn type_info() -> SqliteTypeInfo {
        <&[u8] as Type<Sqlite>>::type_info()
    }
}

impl<'q, T: Serialize> Encode<'q, Sqlite> for FrontierBackup<T> {
    fn encode_by_ref(&self, args: &mut Vec<SqliteArgumentValue<'q>>) -> IsNull {
        let bytes = to_bytes(self);
        args.push(SqliteArgumentValue::Blob(Cow::Owned(bytes)));
        IsNull::No
    }
}

impl<'r, T: Deserialize<'r>> Decode<'r, Sqlite> for FrontierBackup<T> {
    fn decode(value: SqliteValueRef<'r>) -> Result<Self, BoxDynError> {
        let value = <&[u8] as Decode<Sqlite>>::decode(value)?;
        Ok(from_bytes(value))
    }
}

impl StateWriter<u64> for SqliteStateWriter {
    fn write(&mut self, backup: &StateBackup<u64>) {
        let StateBackup(recovery_key, state_update) = backup;
        let RecoveryKey(step_id, key, epoch) = recovery_key;
        let sql = format!("INSERT INTO {} (step_id, key, epoch, state) VALUES (?1, ?2, ?3, ?4) ON CONFLICT (step_id, key, epoch) DO UPDATE SET state = EXCLUDED.state", self.table_name);
        let future = query(&sql)
            .bind(step_id)
            .bind(key)
            .bind(<u64 as TryInto<i64>>::try_into(*epoch).expect("epoch can't fit into SQLite int"))
            // Remember, reset state is stored as an explicit NULL in the
            // DB.
            .bind(state_update)
            .execute(&mut self.conn);
        self.rt.block_on(future).unwrap();

        debug!("sqlite state write backup={backup:?}");
    }
}

impl StateCollector<u64> for SqliteStateWriter {
    fn delete(&mut self, recovery_key: &RecoveryKey<u64>) {
        let RecoveryKey(step_id, key, epoch) = recovery_key;
        let sql = format!(
            "DELETE FROM {} WHERE step_id = ?1 AND key = ?2 AND epoch = ?3",
            self.table_name
        );
        let future = query(&sql)
            .bind(step_id)
            .bind(key)
            .bind(<u64 as TryInto<i64>>::try_into(*epoch).expect("epoch can't fit into SQLite int"))
            .execute(&mut self.conn);
        self.rt.block_on(future).unwrap();

        debug!("sqlite state delete recovery_key={recovery_key:?}");
    }
}

pub(crate) struct SqliteStateReader {
    rt: Runtime,
    rx: tokio::sync::mpsc::Receiver<StateBackup<u64>>,
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
            let sql =
                format!("SELECT step_id, key, epoch, state FROM {table_name} ORDER BY epoch ASC");
            let mut stream = query(&sql)
                .map(|row: SqliteRow| {
                    let recovery_key = RecoveryKey(
                        row.get(0),
                        row.get(1),
                        row.get::<i64, _>(2)
                            .try_into()
                            .expect("SQLite int can't fit into epoch; might be negative"),
                    );
                    StateBackup(recovery_key, row.get(3))
                })
                .fetch(&mut conn)
                .map(|result| result.expect("Error selecting from SQLite"));

            while let Some(backup) = stream.next().await {
                tx.send(backup).await.unwrap();
            }
        });

        Self { rt, rx }
    }
}

impl StateReader<u64> for SqliteStateReader {
    fn read(&mut self) -> Option<StateBackup<u64>> {
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
        debug!("Opened Sqlite connection to {db_file:?}");

        let sql = format!("CREATE TABLE IF NOT EXISTS {table_name} (name PRIMARY KEY, antichain);");
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
    fn write(&mut self, backup: &FrontierBackup<u64>) {
        let sql = format!("INSERT INTO {} (name, antichain) VALUES (?1, ?2) ON CONFLICT (name) DO UPDATE SET antichain = EXCLUDED.antichain", self.table_name);
        let future = query(&sql)
            .bind("worker_frontier")
            .bind(backup)
            .execute(&mut self.conn);
        self.rt.block_on(future).unwrap();

        debug!("sqlite frontier write backup={backup:?}");
    }
}

pub(crate) struct SqliteProgressReader {
    rt: Runtime,
    rx: tokio::sync::mpsc::Receiver<FrontierBackup<u64>>,
}

impl SqliteProgressReader {
    pub(crate) fn new(db_file: &Path) -> Self {
        let table_name = "progress";

        let writer = SqliteProgressWriter::new(db_file);
        let rt = writer.rt;
        let mut conn = writer.conn;

        let (tx, rx) = tokio::sync::mpsc::channel(1);

        rt.spawn(async move {
            let sql =
                format!("SELECT antichain FROM {table_name} WHERE name = \"worker_frontier\"");
            let mut stream = query(&sql)
                .map(|row: SqliteRow| row.get::<FrontierBackup<u64>, _>(0))
                .fetch(&mut conn)
                .map(|result| result.expect("Error selecting from SQLite"));

            while let Some(backup) = stream.next().await {
                debug!("sqlite frontier read backup={backup:?}");
                tx.send(backup).await.unwrap();
            }
        });

        Self { rt, rx }
    }
}

impl ProgressReader<u64> for SqliteProgressReader {
    fn read(&mut self) -> Option<FrontierBackup<u64>> {
        self.rt.block_on(self.rx.recv())
    }
}
