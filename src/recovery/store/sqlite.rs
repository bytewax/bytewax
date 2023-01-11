//! SQLite implementation of state and progress stores.

use std::borrow::Cow;
use std::path::Path;

use futures::StreamExt;

use sqlx::encode::IsNull;
use sqlx::error::BoxDynError;
use sqlx::query;
use sqlx::sqlite::SqliteArgumentValue;
use sqlx::sqlite::SqliteConnectOptions;
use sqlx::sqlite::SqliteRow;
use sqlx::sqlite::SqliteTypeInfo;
use sqlx::sqlite::SqliteValueRef;
use sqlx::ConnectOptions;
use sqlx::Connection;
use sqlx::Decode;
use sqlx::Encode;
use sqlx::Row;
use sqlx::Sqlite;
use sqlx::SqliteConnection;
use sqlx::Type;
use tokio::runtime::Runtime;

use crate::recovery::model::*;

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

pub struct SqliteStateWriter {
    rt: Runtime,
    conn: SqliteConnection,
    table_name: String,
}

impl SqliteStateWriter {
    pub fn new(db_file: &Path) -> Self {
        let table_name = "state".to_string();

        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();

        let mut options = SqliteConnectOptions::new().filename(db_file);
        options = options.create_if_missing(true);
        options.disable_statement_logging();
        let future = SqliteConnection::connect_with(&options);
        tracing::debug!("Opening Sqlite connection to {db_file:?}");
        let mut conn = rt.block_on(future).unwrap();

        // TODO: SQLite doesn't let you bind to table names. Can
        // we do this in a slightly safer way? I'm not as worried
        // because this value is not from items in the dataflow
        // stream, but from the config which should be under
        // developer control.
        let sql = format!("CREATE TABLE IF NOT EXISTS {table_name} (step_id TEXT, state_key TEXT, epoch INTEGER, snapshot BLOB, PRIMARY KEY (step_id, state_key, epoch));");
        let future = query(&sql).execute(&mut conn);
        rt.block_on(future).unwrap();

        Self {
            rt,
            conn,
            table_name,
        }
    }
}

impl KWriter<StoreKey, Change<StateBytes>> for SqliteStateWriter {
    fn write(&mut self, kchange: KChange<StoreKey, Change<StateBytes>>) {
        tracing::trace!("Writing state change {kchange:?}");
        let KChange(store_key, recovery_change) = kchange;
        let StoreKey(epoch, FlowKey(step_id, state_key)) = store_key;

        match recovery_change {
            Change::Upsert(step_change) => {
                let snapshot = match step_change {
                    Change::Upsert(snapshot) => Some(snapshot),
                    Change::Discard => None,
                };
                let sql = format!("INSERT INTO {} (step_id, state_key, epoch, snapshot) VALUES (?1, ?2, ?3, ?4) ON CONFLICT (step_id, state_key, epoch) DO UPDATE SET snapshot = EXCLUDED.snapshot", self.table_name);
                let future = query(&sql)
                    .bind(step_id)
                    .bind(state_key)
                    .bind(
                        <u64 as TryInto<i64>>::try_into(epoch.0)
                            .expect("epoch can't fit into SQLite int"),
                    )
                    // Remember, reset state is stored as an explicit NULL in the
                    // DB.
                    .bind(snapshot)
                    .execute(&mut self.conn);
                self.rt.block_on(future).unwrap();
            }
            Change::Discard => {
                let sql = format!(
                    "DELETE FROM {} WHERE step_id = ?1 AND state_key = ?2 AND epoch = ?3",
                    self.table_name
                );
                let future = query(&sql)
                    .bind(step_id)
                    .bind(state_key)
                    .bind(
                        <u64 as TryInto<i64>>::try_into(epoch.0)
                            .expect("epoch can't fit into SQLite int"),
                    )
                    .execute(&mut self.conn);
                self.rt.block_on(future).unwrap();
            }
        }
    }
}

pub(crate) struct SqliteStateReader {
    rt: Runtime,
    rx: tokio::sync::mpsc::Receiver<StoreChange>,
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
                "SELECT step_id, state_key, epoch, snapshot FROM {table_name} ORDER BY epoch ASC"
            );
            let mut stream = query(&sql)
                .map(|row: SqliteRow| {
                    let step_id: StepId = row.get(0);
                    let state_key: StateKey = row.get(1);
                    let epoch = SnapshotEpoch(
                        row.get::<i64, _>(2)
                            .try_into()
                            .expect("SQLite int can't fit into epoch; might be negative"),
                    );
                    let store_key = StoreKey(epoch, FlowKey(step_id, state_key));
                    let step_change = if let Some(snapshot) = row.get(3) {
                        Change::Upsert(snapshot)
                    } else {
                        Change::Discard
                    };
                    let recovery_change = Change::Upsert(step_change);
                    KChange(store_key, recovery_change)
                })
                .fetch(&mut conn)
                .map(|result| result.expect("Error selecting from SQLite"));

            while let Some(kchange) = stream.next().await {
                tracing::trace!("Reading state change {kchange:?}");
                tx.send(kchange).await.unwrap();
            }
        });

        Self { rt, rx }
    }
}

impl KReader<StoreKey, Change<StateBytes>> for SqliteStateReader {
    fn read(&mut self) -> Option<StoreChange> {
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
        options.disable_statement_logging();
        let future = SqliteConnection::connect_with(&options);
        tracing::debug!("Opening Sqlite connection to {db_file:?}");
        let mut conn = rt.block_on(future).unwrap();

        let sql = format!(
            "CREATE TABLE IF NOT EXISTS {table_name} (worker_index INTEGER PRIMARY KEY, border INTEGER);"
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

impl KWriter<WorkerKey, BorderEpoch> for SqliteProgressWriter {
    fn write(&mut self, kchange: ProgressChange) {
        tracing::trace!("Writing progress change {kchange:?}");
        let KChange(worker_key, change) = kchange;
        let WorkerKey(worker_index) = worker_key;

        match change {
            Change::Upsert(epoch) => {
                let sql = format!("INSERT INTO {} (worker_index, border) VALUES (?1, ?2) ON CONFLICT (worker_index) DO UPDATE SET border = EXCLUDED.border", self.table_name);
                let future = query(&sql)
                    .bind(worker_index)
                    .bind(
                        <u64 as TryInto<i64>>::try_into(epoch.0)
                            .expect("epoch can't fit into SQLite int"),
                    )
                    .execute(&mut self.conn);
                self.rt.block_on(future).unwrap();
            }
            Change::Discard => {
                let sql = format!("DELETE FROM {} WHERE worker_index = ?1", self.table_name);
                let future = query(&sql).bind(worker_index).execute(&mut self.conn);
                self.rt.block_on(future).unwrap();
            }
        }
    }
}

pub(crate) struct SqliteProgressReader {
    rt: Runtime,
    rx: tokio::sync::mpsc::Receiver<ProgressChange>,
}

impl SqliteProgressReader {
    pub(crate) fn new(db_file: &Path) -> Self {
        let table_name = "progress";

        let writer = SqliteProgressWriter::new(db_file);
        let rt = writer.rt;
        let mut conn = writer.conn;

        let (tx, rx) = tokio::sync::mpsc::channel(1);

        rt.spawn(async move {
            let sql = format!("SELECT worker_index, border FROM {table_name}");
            let mut stream = query(&sql)
                .map(|row: SqliteRow| {
                    let worker_index: WorkerIndex = row.get(0);
                    let worker_key = WorkerKey(worker_index);
                    let border = BorderEpoch(
                        row.get::<i64, _>(1)
                            .try_into()
                            .expect("SQLite int can't fit into epoch; might be negative"),
                    );
                    KChange(worker_key, Change::Upsert(border))
                })
                .fetch(&mut conn)
                .map(|result| result.expect("Error selecting from SQLite"));

            while let Some(kchange) = stream.next().await {
                tracing::trace!("Reading progress change {kchange:?}");
                tx.send(kchange).await.unwrap();
            }
        });

        Self { rt, rx }
    }
}

impl KReader<WorkerKey, BorderEpoch> for SqliteProgressReader {
    fn read(&mut self) -> Option<ProgressChange> {
        self.rt.block_on(self.rx.recv())
    }
}

impl StateWriter for SqliteStateWriter {}
impl StateReader for SqliteStateReader {}
impl ProgressWriter for SqliteProgressWriter {}
impl ProgressReader for SqliteProgressReader {}
