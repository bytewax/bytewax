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
use sqlx::Decode;
use sqlx::Encode;
use sqlx::Row;
use sqlx::Sqlite;
use sqlx::SqlitePool;
use sqlx::Type;
use tokio::runtime::Runtime;

use crate::recovery::model::*;
use crate::worker::WorkerCount;
use crate::worker::WorkerIndex;

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

impl<'q> Encode<'q, Sqlite> for StateKey {
    fn encode(self, args: &mut Vec<SqliteArgumentValue<'q>>) -> IsNull {
        args.push(SqliteArgumentValue::Text(Cow::Owned(self.0)));
        IsNull::No
    }

    fn encode_by_ref(&self, args: &mut Vec<SqliteArgumentValue<'q>>) -> IsNull {
        args.push(SqliteArgumentValue::Text(Cow::Owned(self.0.clone())));
        IsNull::No
    }
}

impl<'r> Decode<'r, Sqlite> for StateKey {
    fn decode(value: SqliteValueRef<'r>) -> Result<Self, BoxDynError> {
        let value = <String as Decode<Sqlite>>::decode(value)?;
        Ok(Self(value))
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

impl Type<Sqlite> for WorkerCount {
    fn type_info() -> SqliteTypeInfo {
        // For some reason this does not like using i64 /
        // SqliteArgumentValue::Int64. We get a similar error to
        // https://github.com/launchbadge/sqlx/issues/2093. Maybe
        // SQLite bug?
        <i32 as Type<Sqlite>>::type_info()
    }
}

impl<'q> Encode<'q, Sqlite> for WorkerCount {
    fn encode_by_ref(&self, args: &mut Vec<SqliteArgumentValue<'q>>) -> IsNull {
        args.push(SqliteArgumentValue::Int(self.0 as i32));
        IsNull::No
    }
}

impl<'r> Decode<'r, Sqlite> for WorkerCount {
    fn decode(value: SqliteValueRef<'r>) -> Result<Self, BoxDynError> {
        let value = <i32 as Decode<Sqlite>>::decode(value)?;
        Ok(Self(value as usize))
    }
}

pub struct SqliteStateWriter {
    rt: Runtime,
    conn: SqlitePool,
}

impl SqliteStateWriter {
    pub(crate) fn new(db_file: &Path) -> Result<Self, Box<dyn std::error::Error>> {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()?;

        let mut options = SqliteConnectOptions::new().filename(db_file);
        options = options.create_if_missing(true);
        options.disable_statement_logging();
        let future = SqlitePool::connect_with(options);
        tracing::debug!("Opening Sqlite connection to {db_file:?}");
        let conn = rt.block_on(future)?;

        tracing::debug!("Running any pending sqlite migrations");
        rt.block_on(sqlx::migrate!().run(&conn))
            .expect("Unable to run sqlite migrations");

        Ok(Self { rt, conn })
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
                let sql = "INSERT INTO state (step_id, state_key, epoch, snapshot) \
                           VALUES (?1, ?2, ?3, ?4) \
                           ON CONFLICT (step_id, state_key, epoch) DO UPDATE \
                           SET snapshot = EXCLUDED.snapshot";

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
                    .execute(&self.conn);
                self.rt.block_on(future).unwrap();
            }
            Change::Discard => {
                let sql = "DELETE FROM state \
                           WHERE step_id = ?1 AND state_key = ?2 AND epoch = ?3";
                let future = query(&sql)
                    .bind(step_id)
                    .bind(state_key)
                    .bind(
                        <u64 as TryInto<i64>>::try_into(epoch.0)
                            .expect("epoch can't fit into SQLite int"),
                    )
                    .execute(&self.conn);
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
    pub(crate) fn new(db_file: &Path) -> Result<Self, Box<dyn std::error::Error>> {
        // Bootstrap off writer to get table creation.
        let writer = SqliteStateWriter::new(db_file)?;
        let rt = writer.rt;
        let conn = writer.conn;

        let (tx, rx) = tokio::sync::mpsc::channel(1);

        rt.spawn(async move {
            let sql = "SELECT step_id, state_key, epoch, snapshot \
                       FROM state \
                       ORDER BY epoch ASC";
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
                .fetch(&conn)
                .map(|result| result.expect("Error selecting from SQLite"));

            while let Some(kchange) = stream.next().await {
                tracing::trace!("Reading state change {kchange:?}");
                tx.send(kchange).await.unwrap();
            }
        });

        Ok(Self { rt, rx })
    }
}

impl KReader<StoreKey, Change<StateBytes>> for SqliteStateReader {
    fn read(&mut self) -> Option<StoreChange> {
        self.rt.block_on(self.rx.recv())
    }
}

pub(crate) struct SqliteProgressWriter {
    rt: Runtime,
    conn: SqlitePool,
}

impl SqliteProgressWriter {
    pub(crate) fn new(db_file: &Path) -> Result<Self, Box<dyn std::error::Error>> {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()?;

        let mut options = SqliteConnectOptions::new().filename(db_file);
        options = options.create_if_missing(true);
        options.disable_statement_logging();
        // Use [`SqlitePool`] and not a direct [`SqliteConnection`] to
        // allow multiple async queries at once.
        let future = SqlitePool::connect_with(options);
        tracing::debug!("Opening Sqlite connection to {db_file:?}");
        let conn = rt.block_on(future)?;

        Ok(Self { rt, conn })
    }
}

impl KWriter<WorkerKey, ProgressMsg> for SqliteProgressWriter {
    fn write(&mut self, kchange: ProgressChange) {
        tracing::trace!("Writing progress change {kchange:?}");
        let KChange(key, change) = kchange;
        let WorkerKey(ex, index) = key;

        match change {
            Change::Upsert(msg) => match msg {
                ProgressMsg::Init(count, epoch) => {
                    let sql = "INSERT INTO execution \
                               (execution, worker_index, worker_count, resume_epoch) \
                               VALUES (?1, ?2, ?3, ?4) \
                               ON CONFLICT (execution, worker_index) DO UPDATE \
                               SET worker_count = EXCLUDED.worker_count, \
                               resume_epoch = EXCLUDED.resume_epoch";

                    let future = query(&sql)
                        .bind(
                            <u64 as TryInto<i64>>::try_into(ex.0)
                                .expect("execution can't fit into SQLite int"),
                        )
                        .bind(index)
                        .bind(count)
                        .bind(
                            <u64 as TryInto<i64>>::try_into(epoch.0)
                                .expect("epoch can't fit into SQLite int"),
                        )
                        .execute(&self.conn);
                    self.rt.block_on(future).unwrap();
                }
                ProgressMsg::Advance(epoch) => {
                    let sql = "INSERT INTO progress \
                               (execution, worker_index, frontier) \
                               VALUES (?1, ?2, ?3) \
                               ON CONFLICT (execution, worker_index) DO UPDATE \
                               SET frontier = EXCLUDED.frontier";
                    let future = query(&sql)
                        .bind(
                            <u64 as TryInto<i64>>::try_into(ex.0)
                                .expect("execution can't fit into SQLite int"),
                        )
                        .bind(index)
                        .bind(
                            <u64 as TryInto<i64>>::try_into(epoch.0)
                                .expect("epoch can't fit into SQLite int"),
                        )
                        .execute(&self.conn);
                    self.rt.block_on(future).unwrap();
                }
            },
            Change::Discard => {
                let sql = "DELETE FROM progress WHERE execution = ?1 AND worker_index = ?2";
                let future = query(&sql)
                    .bind(
                        <u64 as TryInto<i64>>::try_into(ex.0)
                            .expect("execution can't fit into SQLite int"),
                    )
                    .bind(index)
                    .execute(&self.conn);
                self.rt.block_on(future).unwrap();
                // TODO: Can we delete execution information? We'd
                // need to change the key concept.
            }
        }
    }
}

pub(crate) struct SqliteProgressReader {
    rt: Runtime,
    rx: tokio::sync::mpsc::Receiver<ProgressChange>,
}

impl SqliteProgressReader {
    pub(crate) fn new(db_file: &Path) -> Result<Self, Box<dyn std::error::Error>> {
        let writer = SqliteProgressWriter::new(db_file)?;
        let rt = writer.rt;
        let conn = writer.conn;

        let (tx, rx) = tokio::sync::mpsc::channel(1);

        rt.spawn(async move {
            let sql = "SELECT execution, worker_index, worker_count, resume_epoch \
                       FROM execution ORDER BY execution DESC LIMIT 1";
            let mut stream = query(&sql)
                .map(|row: SqliteRow| {
                    let ex = Execution(
                        row.get::<i64, _>(0)
                            .try_into()
                            .expect("SQLite int can't fit into execution; might be negative"),
                    );
                    let index: WorkerIndex = row.get(1);
                    let key = WorkerKey(ex, index);
                    let count: WorkerCount = row.get(2);
                    let epoch = ResumeEpoch(
                        row.get::<i64, _>(3)
                            .try_into()
                            .expect("SQLite int can't fit into epoch; might be negative"),
                    );
                    let msg = ProgressMsg::Init(count, epoch);
                    (key, msg)
                })
                .fetch(&conn)
                .map(|result| result.expect("Error selecting from SQLite"));

            let mut last_ex = None;

            while let Some((key, msg)) = stream.next().await {
                let WorkerKey(ex, _index) = key;
                last_ex = Some(ex);

                let kchange = KChange(key, Change::Upsert(msg));
                tracing::trace!("Reading progress change {kchange:?}");
                tx.send(kchange).await.unwrap();
            }

            if let Some(ex) = last_ex {
                let sql =
                    "SELECT execution, worker_index, frontier FROM progress WHERE execution = ?1 ORDER BY frontier ASC";

                let mut stream = query(&sql)
                    .bind(
                        <u64 as TryInto<i64>>::try_into(ex.0)
                            .expect("execution can't fit into SQLite int"),
                    )
                    .map(|row: SqliteRow| {
                        let ex = Execution(
                            row.get::<i64, _>(0)
                                .try_into()
                                .expect("SQLite int can't fit into execution; might be negative"),
                        );
                        let index: WorkerIndex = row.get(1);
                        let key = WorkerKey(ex, index);
                        let epoch = WorkerFrontier(
                            row.get::<i64, _>(2)
                                .try_into()
                                .expect("SQLite int can't fit into epoch; might be negative"),
                        );
                        let msg = ProgressMsg::Advance(epoch);
                        (key, msg)
                    })
                    .fetch(&conn)
                    .map(|result| result.expect("Error selecting from SQLite"));

                while let Some((key, msg)) = stream.next().await {
                    let kchange = KChange(key, Change::Upsert(msg));
                    tracing::trace!("Reading progress change {kchange:?}");
                    tx.send(kchange).await.unwrap();
                }
            }
        });

        Ok(Self { rt, rx })
    }
}

impl KReader<WorkerKey, ProgressMsg> for SqliteProgressReader {
    fn read(&mut self) -> Option<ProgressChange> {
        self.rt.block_on(self.rx.recv())
    }
}

impl StateWriter for SqliteStateWriter {}
impl StateReader for SqliteStateReader {}
impl ProgressWriter for SqliteProgressWriter {}
impl ProgressReader for SqliteProgressReader {}
