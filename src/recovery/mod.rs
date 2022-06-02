//! Internal code for writing to recovery stores.
//!
//! For a user-centric version of recovery, read the
//! `bytewax.recovery` Python module docstring. Read that first.
//!
//! Because the recovery system is complex and requires coordination
//! at many points in execution, not all code for recovery lives
//! here. There are some custom operators and execution and dataflow
//! building code which all help implement the recovery logic. An
//! overview of it is given here, though.
//!
//! The recovery system uses a few custom **utility operators**
//! ([`crate::operators::Recovery`],
//! [`crate::operators::DataflowFrontier`]) to implement
//! behavior. These operators do not represent user-facing dataflow
//! logic, but instead implement our recovery guarantees.
//!
//! [`crate::execution::build_dataflow()`] is where all the
//! coordination for recovery is strung together and you can see the
//! implementation of the things described here.
//!
//! The [`RecoveryStore`] trait provides an interface that the rest of
//! the components use to save data to the recovery store. To
//! implement a new backing type, create a new impl of that.
//!
//! ```mermaid
//! graph TD
//! DB{{Dataflow Builder}} -. Recovered State .-> S1
//! DB -. Recovered State .-> S2
//! DB -. Resume Epoch .-> I
//! I(Input) --> X1([... More Dataflow ...])
//! X1 --> S1(Stateful Operator)
//! S1 -. Updated State .-> R{{Recovery}}
//! S1 -- Logic Output --> X2([... More Dataflow ...]) --> C1(Capture)
//! X1 --> S2(Stateful Operator)
//! S2 -. Updated State .-> R
//! S2 -- Logic Output --> X3([... More Dataflow ...]) --> C2(Capture)
//! R -. Backup Frontier .-> DF{{Dataflow Frontier}}
//! C1 -. Frontier .-> DF
//! C2 -. Frontier .-> DF
//! DF -. Dataflow Frontier .-> R
//! ```
//!
//! Backup
//! ------
//!
//! Each stateful operator ([`crate::operators::Reduce`],
//! [`crate::operators::StatefulMap`]) only contains execution logic;
//! they do not load recovery data or backup themselves. Instead, they
//! emit a second **state update stream** of `(key, state)` updates to
//! feed into the rest of the recovery machinery. A
//! [`timely::operators::Map`] operator which adds the ID of that
//! step.
//!
//! All state updates flow into the [`crate::operators::Recovery`]
//! operator, which writes out these state updates to the recovery
//! store for backup. It keeps an in-memory summary of the keys and
//! epochs that are currently in the recovery store. The recovery
//! operators writes out a log of backups to the **backup
//! stream**. This contains which keys' state has been updated.
//!
//! The [`crate::operator::DataflowFrontier`] operator then looks at
//! the backup stream and all captures and calculates the **dataflow
//! frontier**: the oldest in-progress epoch (meaning all data has not
//! been backed up or output yet). It emits "heartbeat" `()` items
//! which can be used to listen to the dataflow frontier.
//!
//! The dataflow fronter is looped back into the
//! [`crate::operators::Recovery`] operator via a second input. It
//! uses the latest dataflow frontier to detect when some state is no
//! longer necessary for recovery at the dataflow frontier and garbage
//! collect it. It writes out epochs that have been GCd via a **GC
//! stream**.
//!
//! As one extra quirk, the dataflow frontier must be passed through
//! [`timely::dataflow::operators::broadcast::Broadcast`] so that
//! every worker has the true whole dataflow frontier, not just what
//! work happens to be complete on the local worker.
//!
//! Recovery
//! --------
//!
//! Recovery happens whenever a dataflow is built with a recovery
//! store that has data in it. Most of this logic is implemented in
//! [`crate::execution::build_dataflow()`].
//!
//! First the last dataflow frontier and is read from the recovery
//! store. This will be passed to the input builder to ensure that the
//! user's code starts from the correct location.
//!
//! Next, all of the state data is dumped out of the recovery store
//! into memory. It is divvied up into state for each stateful
//! operator using [`build_state_caches()`] and passed as the initial
//! `state_cache` argument to each operator. (See
//! e.g. [`crate::operators::StatefulMap`]).
//!
//! Finally, that state dump is summarized into "which keys and epochs
//! does the recovery store know about" and passed into the
//! [`crate::operators::Recovery`] so it can provide correct and
//! up-to-date GC requests to the state store.
//!
//! If the underlying data or bug has been fixed, then things should
//! start right up again!
//!
//! Hints
//! -----
//!
//! Note that backing up the fact that state was deleted
//! (`state_update: Option<D>` is `None`) is not the same as GCing the
//! state. We need to explicitly save the history of all deletions in
//! case we need to recover right after a deletion; that state value
//! should not be loaded. Separately, once we know some backup state
//! is no longer needed and we'll never need to recover there do we
//! actually delete the state from the data store.

use crate::dataflow::StepId;
use crate::pyo3_extensions::StateKey;
use pyo3::exceptions::PyValueError;
use rdkafka::admin::AdminClient;
use rdkafka::admin::AdminOptions;
use rdkafka::admin::NewTopic;
use rdkafka::admin::TopicReplication;
use rdkafka::consumer::Consumer;
use rdkafka::consumer::StreamConsumer;
use rdkafka::error::KafkaError;
use rdkafka::producer::FutureProducer;
use rdkafka::producer::FutureRecord;
use rdkafka::util::Timeout;
use rdkafka::ClientConfig;
use rdkafka::Message;
use retry::delay::Fixed;
use retry::retry;
use retry::OperationResult;
use send_wrapper::SendWrapper;
use serde::Deserialize;
use serde::Serialize;
use sqlx::query;
use sqlx::sqlite::SqliteConnectOptions;
use sqlx::sqlite::SqlitePoolOptions;
use sqlx::sqlite::SqliteRow;
use sqlx::Pool;
use sqlx::Row;
use sqlx::Sqlite;
use std::collections::HashMap;
use std::fmt::Debug;
use std::time::Duration;
use timely::progress::frontier::AntichainRef;
use timely::progress::Timestamp;
use tokio::runtime::Runtime;

use crate::pyo3_extensions::TdPyAny;
use log::debug;
use pyo3::prelude::*;

/// Base class for a recovery config.
///
/// This describes how to connect to a recovery store.
///
/// Use a specific subclass of this that matches the kind of storage
/// system you are going to use. See the subclasses in this module.
#[pyclass(module = "bytewax.recovery", subclass)]
#[pyo3(text_signature = "()")]
pub(crate) struct RecoveryConfig;

/// Use [SQLite](https://sqlite.org/index.html) as recovery storage.
///
/// Because it's not designed for high-concurrency, SQLite should only
/// be used for machine-local testing of dataflows. Multiple workers
/// will _not_ result in corrupted data, but there will be reduced
/// performance due to contention for the DB lock.
///
/// A `state` table in this DB will automatically be created and
/// queried.
///
/// Only one dataflow can be persisted per SQLite DB. Use a new file
/// for a new dataflow.
///
/// Args:
///
///     db_file_path: Local path to the DB file in Sqlite3
///         format. E.g. `./state.sqlite3`
///
///     create: If the DB file is missing, create it. Defaults to
///         `False`.
///
/// Returns:
///
///     Config object. Pass this as the `recovery_config` argument to
///     your execution entry point.
#[pyclass(module="bytewax.recovery", extends=RecoveryConfig)]
#[pyo3(text_signature = "(db_file_path, create)")]
struct SqliteRecoveryConfig {
    #[pyo3(get)]
    db_file_path: String,
    #[pyo3(get)]
    create: bool,
}

#[pymethods]
impl SqliteRecoveryConfig {
    #[new]
    #[args(db_file_path, create = "false")]
    fn new(db_file_path: String, create: bool) -> (Self, RecoveryConfig) {
        (
            Self {
                db_file_path,
                create,
            },
            RecoveryConfig {},
        )
    }

    /// Pickle as a tuple.
    fn __getstate__(&self) -> (&str, &str, bool) {
        ("SqliteRecoveryConfig", &self.db_file_path, self.create)
    }

    /// Egregious hack because pickling assumes the type has "empty"
    /// mutable objects.
    ///
    /// Pickle always calls `__new__(*__getnewargs__())` but notice we
    /// don't have access to the pickled `db_file_path` yet, so we
    /// have to pass in some dummy string value that will be
    /// overwritten by `__setstate__()` shortly.
    fn __getnewargs__(&self) -> (&str, bool) {
        ("UNINIT_PICKLED_STRING", false)
    }

    /// Unpickle from tuple of arguments.
    fn __setstate__(&mut self, state: &PyAny) -> PyResult<()> {
        if let Ok(("SqliteRecoveryConfig", db_file_path, create)) = state.extract() {
            self.db_file_path = db_file_path;
            self.create = create;
            Ok(())
        } else {
            Err(PyValueError::new_err(format!(
                "bad pickle contents for SqliteRecoveryConfig: {state:?}"
            )))
        }
    }
}

#[pyclass(module="bytewax.recovery", extends=RecoveryConfig)]
#[pyo3(text_signature = "(hosts, recovery_topic, create)")]
struct KafkaRecoveryConfig {
    #[pyo3(get)]
    hosts: Vec<String>,
    #[pyo3(get)]
    topic: String,
    #[pyo3(get)]
    create: bool,
}

#[pymethods]
impl KafkaRecoveryConfig {
    #[new]
    #[args(hosts, recovery_topic, create = "false")]
    fn new(hosts: Vec<String>, topic: String, create: bool) -> (Self, RecoveryConfig) {
        (
            Self {
                hosts,
                topic,
                create,
            },
            RecoveryConfig {},
        )
    }

    /// Pickle as a tuple.
    fn __getstate__(&self) -> (&str, Vec<String>, String, bool) {
        (
            "KafkaRecoveryConfig",
            self.hosts.clone(),
            self.topic.clone(),
            self.create,
        )
    }

    /// Egregious hack see [`SqliteRecoveryConfig::__getnewargs__`].
    fn __getnewargs__(&self) -> (Vec<String>, &str, bool) {
        (vec![], "UNINIT_PICKLED_STRING", false)
    }

    /// Unpickle from tuple of arguments.
    fn __setstate__(&mut self, state: &PyAny) -> PyResult<()> {
        if let Ok(("KafkaRecoveryConfig", hosts, topic, create)) = state.extract() {
            self.hosts = hosts;
            self.topic = topic;
            self.create = create;
            Ok(())
        } else {
            Err(PyValueError::new_err(format!(
                "bad pickle contents for KafkaRecoveryConfig: {state:?}"
            )))
        }
    }
}

/// During dataflow building in
/// [`crate::execution::build_dataflow()`], this creates the
/// [`RecoveryStore`] instance for this worker.
///
/// This is the Python -- Rust barrier; everything after this is pure
/// Rust and should obtain the GIL to function on Bytewax objects.
pub(crate) fn build_recovery_store(
    py: Python,
    recovery_config: Option<Py<RecoveryConfig>>,
) -> Result<Box<dyn RecoveryStore<u64, StateKey, TdPyAny>>, String> {
    match recovery_config {
        None => Ok(Box::new(NoOpRecoveryStore::new())),
        Some(recovery_config) => {
            let recovery_config = recovery_config.as_ref(py);
            if let Ok(sqlite_recovery_config) =
                recovery_config.downcast::<PyCell<SqliteRecoveryConfig>>()
            {
                Ok(Box::new(SqliteRecoveryStore::new(
                    sqlite_recovery_config.borrow(),
                )))
            } else if let Ok(kafka_recovery_config) =
                recovery_config.downcast::<PyCell<KafkaRecoveryConfig>>()
            {
                Ok(Box::new(KafkaRecoveryStore::new(
                    kafka_recovery_config.borrow(),
                )))
            } else {
                let pytype = recovery_config.get_type();
                Err(format!("Unknown recovery_config type: {pytype}"))
            }
        }
    }
}

/// Convert the unstructured log format of recovery data into a
/// structured HashMap that the stateful operators can use during
/// execution.
pub(crate) fn build_state_caches(
    recovery_data: Vec<(StepId, StateKey, u64, Option<TdPyAny>)>,
) -> HashMap<StepId, HashMap<StateKey, TdPyAny>> {
    let mut last_epochs: HashMap<(StepId, StateKey), u64> = HashMap::new();
    let mut state_caches: HashMap<StepId, HashMap<StateKey, TdPyAny>> = HashMap::new();
    for (step_id, key, epoch, state) in recovery_data {
        debug!("state_cache step_id={step_id:?} key={key:?} epoch={epoch:?} state={state:?}");
        // Let's double check that RecoveryStore.load() is loading
        // things in order and panic otherwise because the code below
        // is only correct if state is in epoch order.
        last_epochs
            .entry((step_id.clone(), key.clone()))
            .and_modify(|last_epoch| {
                assert!(
                    epoch > *last_epoch,
                    "Recovery store must return data in epoch order"
                );
                *last_epoch = epoch;
            })
            .or_insert(epoch);

        let state_cache = state_caches.entry(step_id).or_default();
        match state {
            Some(state) => state_cache.insert(key, state),
            None => state_cache.remove(&key),
        };
    }
    state_caches
}

/// Use this instead of [`Option`] so we don't have to actually retain
/// the value of the state in memory to track what the recovery store
/// knows. We only care about keys and if the value was a delete to
/// allow GCing now-unused keys.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) enum UpdateType {
    Upsert,
    Delete,
}

impl<T> From<Option<T>> for UpdateType {
    fn from(option: Option<T>) -> Self {
        match option {
            Some(_) => Self::Upsert,
            None => Self::Delete,
        }
    }
}

/// Rust-side trait which represents actions the dataflow execution
/// will need to delegate to a recovery store.
///
/// We'll implement this for each kind of recovery store we need to
/// talk to. This is not exposed Python-side.
pub(crate) trait RecoveryStore<T: Timestamp, K, D> {
    /// Load all state to recover the dataflow from the most recent
    /// dataflow frontier.
    ///
    /// This returns a vector of known state snapshots and the resume
    /// epoch.
    ///
    /// You must not hold the GIL when calling this function.
    // TODO: Somehow only load data relevant to this worker. Hash the
    // key?
    // TODO: What does it mean to recover to a non-fully ordered
    // timestamp?
    fn load(&self) -> (Vec<(StepId, K, T, Option<D>)>, T);

    /// Save some updated state for a key in a stateful operator in an
    /// epoch.
    ///
    /// If the state was deleted, pass in `None`.
    ///
    /// You should only call this once per epoch per key, when the
    /// epoch is complete to avoid saving unnecessary between-epoch
    /// state.
    ///
    /// You must not hold the GIL when calling this function.
    fn save_state(&mut self, step_id: &StepId, key: &K, epoch: &T, state: &Option<D>);

    /// Save the current dataflow frontier, as this is the resume
    /// epoch.
    ///
    /// This is what is returned as the resume epoch from [`load()`].
    ///
    /// You must not hold the GIL when calling this function.
    fn save_frontier(&mut self, dataflow_frontier: AntichainRef<T>);

    /// Called when recovery state is no longer needed and should be
    /// deleted.
    ///
    /// You must not hold the GIL when calling this function.
    fn delete_state(&mut self, step_id: &StepId, key: &K, epoch: &T);
}

/// A recovery store which does nothing.
///
/// Saves are logged but dropped and all recoveries result in no data.
pub(crate) struct NoOpRecoveryStore;

impl NoOpRecoveryStore {
    pub(crate) fn new() -> Self {
        NoOpRecoveryStore {}
    }
}

impl<T: Timestamp, K: Debug, D: Debug> RecoveryStore<T, K, D> for NoOpRecoveryStore {
    fn load(&self) -> (Vec<(StepId, K, T, Option<D>)>, T) {
        debug!("noop load");
        (Vec::new(), T::minimum())
    }

    fn save_state(&mut self, step_id: &StepId, key: &K, epoch: &T, state: &Option<D>) {
        debug!("noop save_state step_id={step_id:?} key={key:?} epoch={epoch:?} state={state:?}");
    }

    fn save_frontier(&mut self, dataflow_frontier: AntichainRef<T>) {
        debug!("noop save_frontier dataflow_frontier={dataflow_frontier:?}");
    }

    fn delete_state(&mut self, step_id: &StepId, key: &K, epoch: &T) {
        debug!("noop delete_state step_id={step_id:?} key={key:?} epoch={epoch:?}");
    }
}

/// A recovery store which save state to an SQLite DB.
///
/// State is stored in a `states` table with the epoch it was
/// generated at. Backup involves saving `(step_id, key, epoch,
/// state)` tuples and GC involves deleting them. We really just use
/// this as a key-value store.
///
/// The dataflow frontier is stored in a `frontiers` table.
struct SqliteRecoveryStore {
    // SAFETY: Avoid PyO3 Send overloading.
    rt: SendWrapper<Runtime>,
    pool: Pool<Sqlite>,
}

impl SqliteRecoveryStore {
    fn new(config: PyRef<SqliteRecoveryConfig>) -> Self {
        // Horrible news: we have to be very studious and release the
        // GIL any time we know we have it and we call into sqlx
        // because internally it might call log!() which because of
        // pyo3-log might re-acuqire the GIL and sqlx always has a
        // background thread. We don't need to do this in the
        // RecoveryStore methods below because we know they won't be
        // called from a GIL-holding point.

        // SAFETY: Avoid PyO3 Send overloading.
        let config = SendWrapper::new(config);
        config.py().allow_threads(|| {
            let rt = SendWrapper::new(
                tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .unwrap(),
            );

            // For some reason the busy_timeout setting doesn't affect the
            // initial connection. So manually retry here.
            let pool = retry(Fixed::from_millis(100), || {
                let mut options = SqliteConnectOptions::new()
                    .filename(&config.db_file_path)
                    .busy_timeout(Duration::from_secs(5))
                    .journal_mode(sqlx::sqlite::SqliteJournalMode::Wal)
                    .locking_mode(sqlx::sqlite::SqliteLockingMode::Normal)
                    .synchronous(sqlx::sqlite::SqliteSynchronous::Normal)
                    .thread_name(|i| format!("sqlx-sqlite-{i}"));
                if config.create {
                    options = options.create_if_missing(true);
                }
                let future = SqlitePoolOptions::new().connect_with(options);
                match rt.block_on(future) {
                    Ok(pool) => OperationResult::Ok(pool),
                    Err(err) => OperationResult::Err(err),
                }
            })
                .unwrap();
            debug!("Opened Sqlite connection pool to {}", config.db_file_path);

            if config.create {
                let future = query(
                    "CREATE TABLE IF NOT EXISTS states(step_id, key, epoch INTEGER, state, PRIMARY KEY (step_id, key, epoch));"
                ).execute(&pool);
                rt.block_on(future).unwrap();
                let future = query("CREATE TABLE IF NOT EXISTS frontiers(name PRIMARY KEY, epoch INTEGER);")
                    .execute(&pool);
                rt.block_on(future).unwrap();
                let future = query("INSERT INTO frontiers (name, epoch) VALUES (\"dataflow_frontier\", 0) ON CONFLICT (name) DO NOTHING")
                    .execute(&pool);
                rt.block_on(future).unwrap();
            }

            SqliteRecoveryStore { rt, pool }
        })
    }

    fn step_id_to_string(step_id: &StepId) -> String {
        step_id.clone().into()
    }

    fn key_to_string(key: &StateKey) -> String {
        key.clone().into()
    }

    fn epoch_to_i64(epoch: &u64) -> i64 {
        i64::try_from(*epoch).expect("Epoch too big to fit into SQLite int")
    }

    fn state_to_bytes(state: &Option<TdPyAny>) -> Option<Vec<u8>> {
        state.as_ref().map(|state| {
            Python::with_gil(|py| -> Result<Vec<u8>, PyErr> {
                let pickle = py.import("dill")?;
                Ok(pickle.call_method1("dumps", (state,))?.extract()?)
            })
            .expect("Error pickling state")
        })
    }
}

impl RecoveryStore<u64, StateKey, TdPyAny> for SqliteRecoveryStore {
    fn load(&self) -> (Vec<(StepId, StateKey, u64, Option<TdPyAny>)>, u64) {
        let future = query("SELECT epoch FROM frontiers WHERE name = \"dataflow_frontier\"")
            .map(|row: SqliteRow| {
                row.get::<i64, _>(0)
                    .try_into()
                    .expect("SQLite int can't fit into epoch; might be negative")
            })
            .fetch_one(&self.pool);
        let dataflow_frontier: u64 = self.rt.block_on(future).unwrap();

        let resume_epoch = dataflow_frontier;
        // Notice we only select < dataflow frontier. Not <=. Any
        // state written during the previous execution's dataflow
        // frontier itself was still "in progress" and so shouldn't be
        // re-introduced in this run.
        let future = query("SELECT step_id, key, epoch, state FROM states WHERE epoch < ?1 ORDER BY step_id, key, epoch ASC")
            .bind(Self::epoch_to_i64(&resume_epoch))
            .map(|row: SqliteRow| {
                // Because of the whole "PyO3 uses Sync to mark
                // GIL-bound lifetimes" thing, we can't move this GIL
                // block outside without jumping through hoops.
                Python::with_gil(|py| {
                    let pickle = py.import("dill").expect("Error importing dill");

                    let step_id: StepId = row.get::<String, _>(0).into();
                    let key: StateKey = StateKey::new(row.get(1));
                    let epoch: u64 = row.get::<i64, _>(2).try_into().expect("SQLite int can't fit into epoch; might be negative");
                    let state_pickled: Option<&[u8]> = row.get(3);
                    let state = state_pickled.map(|bytes| pickle.call_method1("loads", (bytes, )).expect("Error unpickling state").into());
                    (step_id, key, epoch, state)
                })
            })
            .fetch_all(&self.pool);
        let recovery_store_log = self.rt.block_on(future).unwrap();

        debug!(
            "sqlite load resume_epoch={resume_epoch:?} recovery_store_log.len()={}",
            recovery_store_log.len()
        );
        (recovery_store_log, resume_epoch)
    }

    fn save_state(
        &mut self,
        step_id: &StepId,
        key: &StateKey,
        epoch: &u64,
        state: &Option<TdPyAny>,
    ) {
        let future = query("INSERT INTO states (step_id, key, epoch, state) VALUES (?1, ?2, ?3, ?4) ON CONFLICT (step_id, key, epoch) DO UPDATE SET state = EXCLUDED.state")
            .bind(Self::step_id_to_string(step_id))
            .bind(Self::key_to_string(key))
            .bind(Self::epoch_to_i64(epoch))
            // Remember, deleted state is stored as NULL in the DB.
            .bind(Self::state_to_bytes(state))
            .execute(&self.pool);
        self.rt.block_on(future).unwrap();

        debug!("sqlite save_state step_id={step_id:?} key={key:?} epoch={epoch:?} state={state:?}");
        // TODO: Warn on state overwriting?
    }

    fn save_frontier(&mut self, dataflow_frontier: AntichainRef<u64>) {
        if dataflow_frontier.len() > 0 {
            let future = query("INSERT INTO frontiers (name, epoch) VALUES (?1, ?2) ON CONFLICT (name) DO UPDATE SET epoch = EXCLUDED.epoch")
                .bind("dataflow_frontier")
                // TODO: Will we ever want to save the whole final
                // frontier? We're using fully ordered epochs in
                // Bytewax so there should never be more than one
                // element.
                .bind(Self::epoch_to_i64(dataflow_frontier.first().unwrap()))
                .execute(&self.pool);
            self.rt.block_on(future).unwrap();

            // TODO: Warn on final frontier going backwards? Might stay
            // the same if recovering.
        } else {
            // TODO: More gracefully handle if the dataflow completes
            // sucessfully. Currently if it does, the recovery store
            // ends up empty. So if you attempt to recover from it,
            // the whole dataflow starts again. Delete the state DB?
            let future = query("DELETE FROM frontiers WHERE name = \"dataflow_frontier\"")
                .execute(&self.pool);
            self.rt.block_on(future).unwrap();
        }

        debug!("sqlite save_frontier dataflow_frontier={dataflow_frontier:?}");
    }

    fn delete_state(&mut self, step_id: &StepId, key: &StateKey, epoch: &u64) {
        let future = query("DELETE FROM states WHERE step_id = ?1 AND key = ?2 AND epoch = ?3")
            .bind(Self::step_id_to_string(step_id))
            .bind(Self::key_to_string(key))
            .bind(Self::epoch_to_i64(epoch))
            .execute(&self.pool);
        self.rt.block_on(future).unwrap();

        debug!("sqlite delete_state step_id={step_id:?} key={key:?} epoch={epoch:?}");
    }
}

/// A recovery store which save state to a Kafka topic.
///
/// We use the topic as a log of changes to a key-value store. We read
/// back in the change log to re-build that state.
///
/// See [`KafkaKey`] for the kinds of things we want to store.
struct KafkaRecoveryStore {
    rt: SendWrapper<Runtime>,
    hosts: Vec<String>,
    producer: FutureProducer,
    topic: String,
}

/// Keys in our Kafka-based key-value store.
///
/// Again use an enum to get easy serialization to / from bytes (which
/// is all Kafka can actually read / write).
#[derive(Debug, Serialize, Deserialize)]
enum KafkaKey {
    /// Store a state update for a stateful operator.
    State {
        step_id: StepId,
        key: StateKey,
        epoch: u64,
    },
    /// Save that the dataflow frontier has progressed.
    DataflowFrontier,
}

impl KafkaKey {
    fn to_bytes(&self) -> Vec<u8> {
        // TODO: Figure out if there's a more robust-to-evolution way
        // to serialize this key. If the serialization changes between
        // versions, then recovery doesn't work. Or if we use an
        // encoding that isn't deterministic.
        bincode::serialize(self).expect("Error serializing state key")
    }

    fn from_bytes(bytes: &[u8]) -> Self {
        bincode::deserialize(bytes).expect("Error deserializing state key")
    }
}

/// Values our or Kafka-based key-value store.
///
/// Again use an enum to get easy serialization to / from bytes (which
/// is all Kafka can actually read / write).
#[derive(Debug, Serialize, Deserialize)]
enum KafkaPayload {
    /// The actual state update. If inside is `None` then the state
    /// was deleted at this epoch.
    State(Option<TdPyAny>),
    /// A dataflow frontier value update.
    DataflowFrontier(u64),
}

impl KafkaPayload {
    fn to_bytes(&self) -> Vec<u8> {
        bincode::serialize(self).expect("Error serializing state payload")
    }

    fn from_bytes(bytes: &[u8]) -> Self {
        bincode::deserialize(bytes).expect("Error deserializing state payload")
    }
}

impl KafkaRecoveryStore {
    fn new(config: PyRef<KafkaRecoveryConfig>) -> Self {
        use rdkafka::types::RDKafkaErrorCode;

        let rt = SendWrapper::new(
            tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap(),
        );

        let hosts = config.hosts.clone();
        let topic = config.topic.clone();

        debug!("Creating Kafka topic with hosts={hosts:?}");
        let admin: AdminClient<_> = rt.block_on(async {
            ClientConfig::new()
                .set("bootstrap.servers", hosts.join(","))
                .create()
                .expect("Error building Kafka admin")
        });
        let admin_options = AdminOptions::new();

        // TODO: Really we want num_partitions: worker_count, but
        // we'll figure that out once we do multi-worker recovery
        // loading.
        let new_topic = NewTopic {
            name: topic.as_str(),
            num_partitions: 1,
            replication: TopicReplication::Fixed(1),
            config: vec![],
        };
        let future = admin.create_topics(vec![&new_topic], &admin_options);
        let result = rt
            .block_on(future)
            .expect("Error calling create Kafka topic on admin")
            .pop()
            .unwrap();
        match result {
            Ok(topic) => debug!("Created recovery Kafka topic={topic:?}"),
            Err((topic, RDKafkaErrorCode::TopicAlreadyExists)) => {
                debug!("Kafka topic={topic:?} already exists; continuing")
            }
            Err((topic, err_code)) => panic!("Error creating Kafka topic={topic:?}: {err_code:?}"),
        }

        debug!("Creating Kafka producer with hosts={hosts:?}");
        let producer: FutureProducer = ClientConfig::new()
            .set("bootstrap.servers", hosts.join(","))
            .create()
            .expect("Error building Kafka producer");

        Self {
            rt,
            hosts,
            producer,
            topic,
        }
    }
}

impl RecoveryStore<u64, StateKey, TdPyAny> for KafkaRecoveryStore {
    fn load(&self) -> (Vec<(StepId, StateKey, u64, Option<TdPyAny>)>, u64) {
        debug!(
            "Loading recovery data from hosts={:?} topic={}",
            self.hosts, self.topic
        );
        let consumer: StreamConsumer = self.rt.block_on(async {
            ClientConfig::new()
                // TODO: Do away with a consumer group here and
                // instead do per-worker recovery loading.
                .set("group.id", "bytewax-loader")
                .set("bootstrap.servers", self.hosts.join(","))
                .set("auto.offset.reset", "earliest")
                .set("enable.partition.eof", "true")
                .create()
                .expect("Error building Kafka consumer")
        });

        consumer
            .subscribe(&vec![self.topic.as_str()])
            .expect("Error subscribing to Kafka recovery topic");

        let mut resume_epoch = 0;
        // Build up an in-memory representation of the key-value store
        // the Kafka topic is representing.
        let mut db = HashMap::new();

        loop {
            let future = consumer.recv();
            let msg_result = self.rt.block_on(future);
            match msg_result {
                Ok(msg) => {
                    let msg_key =
                        KafkaKey::from_bytes(msg.key().expect("Kafka message with no key"));
                    let msg_payload = msg.payload().map(KafkaPayload::from_bytes);
                    match msg_key {
                        KafkaKey::DataflowFrontier => match msg_payload {
                            Some(KafkaPayload::DataflowFrontier(dataflow_frontier)) => resume_epoch = dataflow_frontier,
                            // TODO: Figure out better semantics when
                            // resuming a completed dataflow.
                            None => resume_epoch = 0,
                            unexpected_payload => panic!("Unexpected dataflow frontier Kafka message payload: {unexpected_payload:?}"),
                        },
                        KafkaKey::State { step_id, key, epoch } => {
                            let db_key = (step_id, key, epoch);
                            match msg_payload {
                                Some(KafkaPayload::State(state)) => {
                                    db.insert(db_key, state);
                                },
                                None => {
                                    db.remove(&db_key);
                                },
                                unexpected_payload => panic!("Unexpected state update Kafka message payload: {unexpected_payload:?}"),
                            }
                        },
                    }
                }
                Err(KafkaError::PartitionEOF(_)) => {
                    break;
                }
                Err(err) => panic!("Error reading from Kafka topic: {err:?}"),
            }
        }

        let mut recovery_store_log = db
            .into_iter()
            .map(|((step_id, key, epoch), state)| (step_id, key, epoch, state))
            .filter(|(_step_id, _key, epoch, _state)| epoch <= &resume_epoch)
            .collect::<Vec<_>>();
        recovery_store_log.sort_by_key(|(_step_id, _key, epoch, _state)| *epoch);

        debug!(
            "kafka load resume_epoch={resume_epoch:?} recovery_store_log.len()={}",
            recovery_store_log.len()
        );
        (recovery_store_log, resume_epoch)
    }

    fn save_state(
        &mut self,
        step_id: &StepId,
        key: &StateKey,
        epoch: &u64,
        state: &Option<TdPyAny>,
    ) {
        let msg_key = KafkaKey::State {
            step_id: step_id.clone(),
            key: key.clone(),
            epoch: *epoch,
        }
        .to_bytes();
        // Remember, deleted state is stored as `None` in the
        // encoding.
        let msg_payload = KafkaPayload::State(state.clone()).to_bytes();
        let record = FutureRecord::to(&self.topic)
            .key(&msg_key)
            .payload(&msg_payload);

        let future = self.producer.send(record, Timeout::Never);
        self.rt
            .block_on(future)
            .expect("Error saving recovery state");

        debug!("kafka save_state step_id={step_id:?} key={key:?} epoch={epoch:?} state={state:?}");
    }

    fn save_frontier(&mut self, dataflow_frontier: AntichainRef<u64>) {
        if dataflow_frontier.len() > 0 {
            let msg_key = KafkaKey::DataflowFrontier.to_bytes();
            let msg_payload =
                KafkaPayload::DataflowFrontier(*dataflow_frontier.first().unwrap()).to_bytes();
            let record = FutureRecord::to(&self.topic)
                .key(&msg_key)
                .payload(&msg_payload);

            let future = self.producer.send(record, Timeout::Never);
            self.rt
                .block_on(future)
                .expect("Error saving recovery state");
        } else {
            let msg_key = KafkaKey::DataflowFrontier.to_bytes();
            let record: FutureRecord<_, Vec<u8>> = FutureRecord::to(&self.topic).key(&msg_key);

            let future = self.producer.send(record, Timeout::Never);
            self.rt
                .block_on(future)
                .expect("Error saving recovery state");
        }

        debug!("kafka save_frontier dataflow_frontier={dataflow_frontier:?}");
    }

    fn delete_state(&mut self, step_id: &StepId, key: &StateKey, epoch: &u64) {
        let msg_key = KafkaKey::State {
            step_id: step_id.clone(),
            key: key.clone(),
            epoch: *epoch,
        }
        .to_bytes();
        let record: FutureRecord<_, Vec<u8>> = FutureRecord::to(&self.topic).key(&msg_key);

        let future = self.producer.send(record, Timeout::Never);
        self.rt
            .block_on(future)
            .expect("Error saving recovery state");

        debug!("kafka delete_state step_id={step_id:?} key={key:?} epoch={epoch:?}");
    }
}

pub(crate) fn register(_py: Python, m: &PyModule) -> PyResult<()> {
    m.add_class::<RecoveryConfig>()?;
    m.add_class::<SqliteRecoveryConfig>()?;
    m.add_class::<KafkaRecoveryConfig>()?;
    Ok(())
}
