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
//! Architecture
//! ------------
//!
//! Recovery is based around storing each worker's state updates and
//! progress information. Each worker's recovery data is stored
//! separately so that we can discern the exact state of each worker
//! at the point of failure.
//!
//! The **worker frontier** represents the oldest epoch for which
//! items are still in the process of being output or state updates
//! are being backed up.
//!
//! We model the recovery store for each worker as a key-value
//! database with two "tables" for each worker: a **state table** and
//! a **progress table**. The state table backs up state changes,
//! while the progress table backs up changes to the worker frontier.
//!
//! Note that backing up the fact that state was deleted
//! (`StateUpdate::Reset`) is not the same as GCing the state. We need
//! to explicitly save the history of all deletions in case we need to
//! recover right after a deletion; that state value should not be
//! recovered. Separately, once we know some backup state is no longer
//! needed and we'll never need to recover there do we actually delete
//! the state from the recovery store.
//!
//! The data model for state table is represented in the structs
//! [`RecoveryKey`] and [`StateBackup`] for the key and value
//! respectively. For the progress table, there is only a single key
//! representing "worker frontier" and the value is
//! [`FrontierUpdate`].
//!
//! All state data is serialized into bytes right after the stateful
//! operator and the rest of the downstream machinery works on the
//! state being an opaque blob of bytes. We do this so we can
//! interleave multiple concrete state types from different stateful
//! operators together.
//!
//! There are 5 traits which provides an interface to the abstract
//! idea of a KVDB that the dataflow operators use to save data to the
//! recovery store: [`ProgressReader`], [`ProgressWriter`],
//! [`StateReader`], [`StateWriter`], and [`StateCollector`]. To
//! implement a new backing recovery store, create a new impl of
//! that. Because each worker's data needs to be kept separate, they
//! are parameterized on creation by worker index and count to ensure
//! data is routed appropriately for each worker.
//!
//! The recovery system uses a few custom utility operators
//! ([`crate::operators::WriteState`],
//! [`crate::operators::WriteProgress`],
//! [`crate::operators::CollectGarbage`],
//! [`crate::operators::state_source`],
//! [`crate::operators::progress_source`]) to implement
//! behavior. These operators do not represent user-facing dataflow
//! logic, but instead implement our recovery behavior.
//!
//! A technique we use to broadcast progress information without
//! needing to serialize the associated items is to map a stream into
//! **heartbeats** `()` since they'll have minimal overhead of
//! transport.
//!
//! ```mermaid
//! graph TD
//! subgraph "Resume Calc Dataflow"
//! LI{{Progress Input}} -- frontier update --> LWM{{Accumulate}} -- worker frontier --> LB{{Broadcast}} -- worker frontier --> LCM{{Accumulate}} -- cluster frontier --> LS{{Channel}}
//! LI -. probe .-> LS
//! end
//!
//! LS == resume epoch ==> I & SI
//!
//! subgraph "Production Dataflow"
//! SI{{State Input}} -. probe .-> GC
//! SI -- "(step id, key, epoch): state bytes" --> SFX{{Filter}} -- "(step id, key, epoch): state bytes" --> SOXD{{Map}} -- "('step x', key, epoch): state" --> SOX
//! SI -- "(step id, key, epoch): state bytes" --> SFY{{Filter}} -- "(step id, key, epoch): state bytes" --> SOYD{{Map}} -- "('step y', key, epoch): state" --> SOY
//! I(Input) -- items --> XX1([...]) -- "(key, value)" --> SOX(Stateful Operator X) & SOY(Stateful Operator Y)
//! SOX & SOY -- "(key, value)" --> XX2([...]) -- items --> O1(Output 1) & O2(Output 2)
//! O1 & O2 -- items --> OM{{Map}}
//! SOX -- "('step x', key, epoch): state" --> SOXS{{Map}} -- "('step x', key, epoch): state bytes" --> SOC
//! SOY -- "('step y', key, epoch): state" --> SOYS{{Map}} -- "('step x', key, epoch): state bytes" --> SOC
//! SOC{{Concat}} -- "(step id, key, epoch): state" --> SB{{State Backup}}
//! SB -- "(step id, key, epoch): state bytes" --> BM{{Map}}
//! OM & BM -- heartbeats --> DFC{{Concat}} -- heartbeats / worker frontier --> FB{{Progress Backup}} -- heartbeats / worker frontier --> DFB{{Broadcast}} -- heartbeats / dataflow frontier --> GC
//! SB & SI -- "(step id, key, epoch): state bytes" --> GCC{{Concat}} -- "(step id, key, epoch): state bytes" --> GC{{Garbage Collector}}
//! I -. probe .-> GC
//! end
//! ```
//!
//! On Backup
//! ---------
//!
//! [`crate::execution::build_production_dataflow`] builds the parts
//! of the dataflow for backup. Look there for what is described
//! below.
//!
//! We currently have two user-facing stateful operators
//! [`crate::operators::Reduce`],
//! [`crate::operators::StatefulMap`]). But they are both implemented
//! on top of a general underlying one:
//! [`crate::operators::StatefulUnary`]. This means all in-operator
//! recovery-related code is only written once.
//!
//! Stateful unary does not load recovery data or backup
//! itself. Instead, it interfaces with a second **state loading
//! stream** input and generates a second **state backup stream**
//! output. These are then connected to the rest of the recovery
//! componenets, after serializing / deserializing the state so that
//! the recovery streams are all backups of bytes.
//!
//! All state backups from all stateful operators are concatenated
//! into the [`crate::operators::WriteState`] operator, which actually
//! performs the writes via the [`StateWriter`]. It emits the backups
//! after writing downstream so progress can be monitored.
//!
//! The [`crate::operator::WriteProgress`] operator then looks at the
//! **worker frontier**, the combined stream of written backups and
//! all captures. This will be writen via the [`ProgressWriter`]. It
//! emits heartbeats.
//!
//! These worker frontier heartbeats are then broadcast so operators
//! listening to this stream will see progress of the entire dataflow
//! cluster, the **dataflow frontier**.
//!
//! The dataflow frontier heartbeat stream and completed state backups
//! are then fed into the [`crate::operators::GarbageCollector`]
//! operator. It uses the dataflow frontier to detect when some state
//! is no longer necessary for recovery and issues deletes via the
//! [`StateCollector`]. GC keeps an in-memory summary of the keys and
//! epochs that are currently in the recovery store so reads are not
//! necessary. It writes out heartbeats of progress as well.
//!
//! The progress of GC is what is probed to rate-limit execution, not
//! just captures, so we don't queue up too much recovery work.
//!
//! On Resume
//! ---------
//!
//! [`crate::execution::worker_main`] is where loading starts. It's
//! broken into two dataflows, built in
//! [`crate::execution::build_and_run_resume_epoch_calc_dataflow`] and
//! [`crate::execution::build_and_run_production_dataflow`].
//!
//! First, the resume epoch must be calculated from the progress data
//! actually written to the recovery store. This can't be
//! pre-calculated during backup because that write might fail.
//!
//! This is done in a separate dataflow first because it needs a
//! unique epoch definition from the production dataflow: we need to
//! know when we're done reading all recovery data, which would be
//! impossible if we re-used the epoch definition from the backed up
//! dataflow (because that would mean it completed).
//!
//! Each resume cluster worker is assigned to read the entire progress
//! data from some failed cluster worker.
//!
//! 1. Find the oldest frontier for the worker since we want to know
//! how far it got. (That's the first accumulate).
//!
//! 2. Broadcast all our oldest per-worker frontiers.
//!
//! 3. Find the earliest worker frontier since we want to resume from
//! the last epoch fully completed by all workers in the
//! cluster. (That's the second accumulate).
//!
//! 4. Cough and turn the frontier back into a singular resume epoch.
//!
//! 5. Send the resume epoch out of the dataflow via a channel.
//!
//! Once we have the resume epoch, we can start the production
//! dataflow.
//!
//! The main input handle of the production dataflow is advanced to
//! that epoch (so the input system does not accidentally send old
//! data).
//!
//! The production dataflow also has a second **state loading input**
//! [`crate::operators::state_source`] in which each resume worker is
//! assigned to read all state _before_ the resume epoch. This state
//! is routed to the correct stateful operators (filtering on step ID)
//! so that state is loaded and ready for when normal execution
//! resumes. This state data is also sent to the
//! [`crate::operators::GarbageCollector`] operator so that it has a
//! correct cache of all state keys.
//!
//! Once the state loading input has caught up to the resume epoch and
//! completed, the probe of the production dataflow will show a
//! frontier of the resume epoch and so the input machinery will start
//! introducing new data again.
//!
//! If the underlying data or bug has been fixed, then things should
//! start right up again!

use crate::dataflow::StepId;
use crate::pyo3_extensions::StateKey;
use futures::stream::StreamExt;
use log::debug;
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use rdkafka::admin::AdminClient;
use rdkafka::admin::AdminOptions;
use rdkafka::admin::NewTopic;
use rdkafka::admin::TopicReplication;
use rdkafka::consumer::BaseConsumer;
use rdkafka::consumer::Consumer;
use rdkafka::error::KafkaError;
use rdkafka::producer::BaseProducer;
use rdkafka::producer::BaseRecord;
use rdkafka::util::Timeout;
use rdkafka::ClientConfig;
use rdkafka::Message;
use rdkafka::Offset;
use rdkafka::TopicPartitionList;
use serde::de::DeserializeOwned;
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
use std::any::type_name;
use std::borrow::Cow;
use std::fmt::Debug;
use std::marker::PhantomData;
use std::path::Path;
use std::path::PathBuf;
use timely::dataflow::operators::Map;
use timely::dataflow::Scope;
use timely::dataflow::Stream;
use timely::progress::Antichain;
use timely::Data;
use tokio::runtime::Runtime;

/// Base class for a recovery config.
///
/// This describes how each worker in a dataflow cluster should store
/// its recovery data.
///
/// Use a specific subclass of this that matches the kind of storage
/// system you are going to use. See the subclasses in this module.
#[pyclass(module = "bytewax.recovery", subclass)]
#[pyo3(text_signature = "()")]
pub(crate) struct RecoveryConfig;

/// Do not store any recovery data.
///
/// This is the default if no `recovery_config` is passed to your
/// execution entry point, so you shouldn't need to build this
/// explicitly.
#[pyclass(module="bytewax.recovery", extends=RecoveryConfig)]
struct NoopRecoveryConfig;

#[pymethods]
impl NoopRecoveryConfig {
    #[new]
    fn new() -> (Self, RecoveryConfig) {
        (Self {}, RecoveryConfig {})
    }

    /// Pickle as a tuple.
    fn __getstate__(&self) -> (&str,) {
        ("NoopRecoveryConfig",)
    }

    /// Unpickle from tuple of arguments.
    fn __setstate__(&mut self, state: &PyAny) -> PyResult<()> {
        if let Ok(("NoopRecoveryConfig",)) = state.extract() {
            Ok(())
        } else {
            Err(PyValueError::new_err(format!(
                "bad pickle contents for NoopRecoveryConfig: {state:?}"
            )))
        }
    }
}

pub(crate) fn default_recovery_config() -> Py<RecoveryConfig> {
    Python::with_gil(|py| {
        PyCell::new(py, NoopRecoveryConfig::new())
            .unwrap()
            .extract()
            .unwrap()
    })
}

/// Use [SQLite](https://sqlite.org/index.html) to store recovery
/// data.
///
/// Creates a SQLite DB per-worker in a given directory. Multiple DBs
/// are used to allow workers to write without contention.
///
/// Use a distinct directory per dataflow so recovery data is not
/// mixed.
///
/// >>> flow = Dataflow()
/// >>> flow.capture()
/// >>> def input_builder(worker_index, worker_count, resume_epoch):
/// ...     for epoch, item in enumerate(range(resume_epoch, 3)):
/// ...         yield AdvanceTo(epoch)
/// ...         yield Emit(item)
/// >>> def output_builder(worker_index, worker_count):
/// ...     return print
/// >>> tmp_dir = TemporaryDirectory()  # We'll store this somewhere temporary for this test.
/// >>> recovery_config = SqliteRecoveryConfig(tmp_dir)
/// >>> run_main(
/// ...     flow,
/// ...     ManualConfig(input_builder),
/// ...     output_builder,
/// ...     recovery_config=recovery_config,
/// ... )  # doctest: +ELLIPSIS
/// (...)
///
/// DB files and tables will automatically be created if there's no
/// previous recovery data.
///
/// Args:
///
///     db_dir: Existing directory to store per-worker DBs in. Must be
///         distinct per-dataflow. DB files will have names like
///         `"worker0.sqlite3"`. You can use `"."` for the current
///         directory.
///
/// Returns:
///
///     Config object. Pass this as the `recovery_config` argument to
///     your execution entry point.
#[pyclass(module="bytewax.recovery", extends=RecoveryConfig)]
#[pyo3(text_signature = "(db_dir)")]
struct SqliteRecoveryConfig {
    #[pyo3(get)]
    db_dir: PathBuf,
}

#[pymethods]
impl SqliteRecoveryConfig {
    #[new]
    #[args(db_dir)]
    fn new(db_dir: PathBuf) -> (Self, RecoveryConfig) {
        (Self { db_dir }, RecoveryConfig {})
    }

    /// Pickle as a tuple.
    fn __getstate__(&self) -> (&str, &Path) {
        ("SqliteRecoveryConfig", &self.db_dir)
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
    fn db_file(&self, worker_index: usize) -> PathBuf {
        self.db_dir.join(format!("worker{worker_index}.sqlite3"))
    }
}

/// Use [Kafka](https://kafka.apache.org/) to store recovery data.
///
/// Uses a "progress" topic and a "state" topic with a number of
/// partitions equal to the number of workers. Will take advantage of
/// log compaction so that topic size is proportional to state size,
/// not epoch count.
///
/// Use a distinct topic prefix per dataflow so recovery data is not
/// mixed.
//
/// >>> flow = Dataflow()
/// >>> flow.capture()
/// >>> def input_builder(worker_index, worker_count, resume_epoch):
/// ...     for epoch, item in enumerate(range(resume_epoch, 3)):
/// ...         yield AdvanceTo(epoch)
/// ...         yield Emit(item)
/// >>> def output_builder(worker_index, worker_count):
/// ...     return print
/// >>> recovery_config = KafkaRecoveryConfig(
/// ...     ["localhost:9092"],
/// ...     "sample-dataflow",
/// ... )
/// >>> run_main(
/// ...     flow,
/// ...     ManualConfig(input_builder),
/// ...     output_builder,
/// ...     recovery_config=recovery_config,
/// ... )  # doctest: +ELLIPSIS
/// (...)
///
/// If there's no previous recovery data, topics will automatically be
/// created with the correct number of partitions and log compaction
/// enabled
///
/// Args:
///
///     hosts: List of `host:port` strings of Kafka brokers.
///
///     topic_prefix: Prefix used for naming topics. Must be distinct
///         per-dataflow. Two topics will be created using this prefix
///         `"topic_prefix-progress"` and `"topic_prefix-state"`.
///
/// Returns:
///
///     Config object. Pass this as the `recovery_config` argument to
///     your execution entry point.
#[pyclass(module="bytewax.recovery", extends=RecoveryConfig)]
#[pyo3(text_signature = "(hosts, topic_prefix, create)")]
struct KafkaRecoveryConfig {
    #[pyo3(get)]
    hosts: Vec<String>,
    #[pyo3(get)]
    topic_prefix: String,
}

#[pymethods]
impl KafkaRecoveryConfig {
    #[new]
    #[args(hosts, topic_prefix)]
    fn new(hosts: Vec<String>, topic_prefix: String) -> (Self, RecoveryConfig) {
        (
            Self {
                hosts,
                topic_prefix,
            },
            RecoveryConfig {},
        )
    }

    /// Pickle as a tuple.
    fn __getstate__(&self) -> (&str, Vec<String>, &str) {
        (
            "KafkaRecoveryConfig",
            self.hosts.clone(),
            &self.topic_prefix,
        )
    }

    /// Egregious hack see [`SqliteRecoveryConfig::__getnewargs__`].
    fn __getnewargs__(&self) -> (Vec<String>, &str) {
        (vec![], "UNINIT_PICKLED_STRING")
    }

    /// Unpickle from tuple of arguments.
    fn __setstate__(&mut self, state: &PyAny) -> PyResult<()> {
        if let Ok(("KafkaRecoveryConfig", hosts, topic_prefix)) = state.extract() {
            self.hosts = hosts;
            self.topic_prefix = topic_prefix;
            Ok(())
        } else {
            Err(PyValueError::new_err(format!(
                "bad pickle contents for KafkaRecoveryConfig: {state:?}"
            )))
        }
    }
}

impl KafkaRecoveryConfig {
    fn progress_topic(&self) -> String {
        format!("{}-progress", self.topic_prefix)
    }

    fn state_topic(&self) -> String {
        format!("{}-state", self.topic_prefix)
    }
}

fn to_bytes<T: Serialize>(obj: &T) -> Vec<u8> {
    // TODO: Figure out if there's a more robust-to-evolution way
    // to serialize this key. If the serialization changes between
    // versions, then recovery doesn't work. Or if we use an
    // encoding that isn't deterministic.
    bincode::serialize(obj).expect("Error serializing recovery data")
}

fn from_bytes<'a, T: Deserialize<'a>>(bytes: &'a [u8]) -> T {
    let t_name = type_name::<T>();
    bincode::deserialize(bytes).expect(&format!("Error deserializing recovery {t_name})"))
}

// Here's our recovery data model.

/// A message noting that the frontier at an input changed.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct FrontierUpdate<T>(pub(crate) Antichain<T>);

/// A message noting the state for a key in a stateful operator
/// changed.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct StateBackup<T, D>(pub(crate) RecoveryKey<T>, pub(crate) StateUpdate<D>);

/// Key used to address data within a recovery store.
///
/// Remember, this isn't the same as [`StateKey`], as the "address
/// space" of that key is just within a single operator. This type
/// includes the operator name and epoch too, so we can address state
/// in an entire dataflow and across time.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct RecoveryKey<T>(pub(crate) StepId, pub(crate) StateKey, pub(crate) T);

/// The two kinds of actions that logic in a stateful operator can do
/// to each [`StateKey`].
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) enum StateUpdate<D> {
    /// Save an updated state.
    Upsert(D),
    /// Note that the state for this key was reset to empty.
    Reset,
}

impl<D> StateUpdate<D> {
    fn map<R>(self, f: impl Fn(D) -> R) -> StateUpdate<R> {
        match self {
            Self::Upsert(state) => StateUpdate::Upsert(f(state)),
            Self::Reset => StateUpdate::Reset,
        }
    }
}

// Here's our core traits for recovery that allow us to swap out
// underlying storage.

pub(crate) trait ProgressWriter<T> {
    fn write(&mut self, frontier_update: &FrontierUpdate<T>);
}

pub(crate) trait ProgressReader<T> {
    /// Has the same semantics as [`std::iter::Iterator::next`]:
    /// return [`None`] to signal EOF.
    fn read(&mut self) -> Option<FrontierUpdate<T>>;
}

pub(crate) trait StateWriter<T, D> {
    fn write(&mut self, state_backup: &StateBackup<T, D>);
}

pub(crate) trait StateCollector<T> {
    fn delete(&mut self, recovery_key: &RecoveryKey<T>);
}

pub(crate) trait StateReader<T, D> {
    /// Has the same semantics as [`std::iter::Iterator::next`]:
    /// return [`None`] to signal EOF.
    fn read(&mut self) -> Option<StateBackup<T, D>>;
}

/// Extension trait for [`Stream`].
pub(crate) trait SerializeBackup<S: Scope> {
    /// Serialize the state data in a backup stream.
    ///
    /// We do this after each stateful operator so the types
    /// downstream are all compatible.
    fn serialize_backup(&self) -> Stream<S, StateBackup<S::Timestamp, Vec<u8>>>;
}

impl<S: Scope, D: Data + Serialize> SerializeBackup<S> for Stream<S, StateBackup<S::Timestamp, D>> {
    fn serialize_backup(&self) -> Stream<S, StateBackup<S::Timestamp, Vec<u8>>> {
        self.map(|StateBackup(recovery_key, state_update)| {
            StateBackup(recovery_key, state_update.map(|s| to_bytes(&s)))
        })
    }
}

/// Extension trait for [`Stream`].
pub(crate) trait DeserializeBackup<S: Scope, D: Data + DeserializeOwned> {
    /// Deserialize the state data in a backup stream.
    ///
    /// We do this after each stateful operator so the types
    /// downstream are all compatible.
    fn deserialize_backup(&self) -> Stream<S, StateBackup<S::Timestamp, D>>;
}

impl<S: Scope, D: Data + DeserializeOwned> DeserializeBackup<S, D>
    for Stream<S, StateBackup<S::Timestamp, Vec<u8>>>
{
    fn deserialize_backup(&self) -> Stream<S, StateBackup<S::Timestamp, D>> {
        self.map(|StateBackup(recovery_key, state_update)| {
            StateBackup(recovery_key, state_update.map(|s| from_bytes(&s)))
        })
    }
}

/// Use a recovery config and the current worker's identity to build
/// out the specific recovery writer instances that this worker will
/// need to backup recovery data.
///
/// This function is also the Python-Rust barrier for recovery; we
/// don't have any Python types in the recovery machinery after this.
pub(crate) fn build_recovery_writers(
    py: Python,
    worker_index: usize,
    worker_count: usize,
    recovery_config: Py<RecoveryConfig>,
) -> Result<
    (
        Box<dyn ProgressWriter<u64>>,
        Box<dyn StateWriter<u64, Vec<u8>>>,
        Box<dyn StateCollector<u64>>,
    ),
    String,
> {
    // Horrible news: we have to be very studious and release the GIL
    // any time we know we have it and we call into complex Rust
    // libraries because internally it might call log!() on a
    // background thread, which because of `pyo3-log` might try to
    // re-acquire the GIL and then you have deadlock. E.g. `sqlx` and
    // `rdkafka` always spawn background threads.
    let recovery_config = recovery_config.as_ref(py);

    if let Ok(_noop_recovery_config) = recovery_config.downcast::<PyCell<NoopRecoveryConfig>>() {
        let (state_writer, progress_writer, state_collector) = py.allow_threads(|| {
            (
                NoopRecovery::new(),
                NoopRecovery::new(),
                NoopRecovery::new(),
            )
        });
        Ok((
            Box::new(state_writer),
            Box::new(progress_writer),
            Box::new(state_collector),
        ))
    } else if let Ok(sqlite_recovery_config) =
        recovery_config.downcast::<PyCell<SqliteRecoveryConfig>>()
    {
        let sqlite_recovery_config = sqlite_recovery_config.borrow();

        let db_file = sqlite_recovery_config.db_file(worker_index);

        let (progress_writer, state_writer, state_collector) = py.allow_threads(|| {
            (
                SqliteProgressWriter::new(&db_file),
                SqliteStateWriter::new(&db_file),
                SqliteStateWriter::new(&db_file),
            )
        });

        Ok((
            Box::new(progress_writer),
            Box::new(state_writer),
            Box::new(state_collector),
        ))
    } else if let Ok(kafka_recovery_config) =
        recovery_config.downcast::<PyCell<KafkaRecoveryConfig>>()
    {
        let kafka_recovery_config = kafka_recovery_config.borrow();

        let hosts = &kafka_recovery_config.hosts;
        let state_topic = kafka_recovery_config.state_topic();
        let progress_topic = kafka_recovery_config.progress_topic();
        let partition = worker_index.try_into().unwrap();
        let create_partitions = worker_count.try_into().unwrap();

        let (progress_writer, state_writer, state_collector): (
            KafkaWriter<String, FrontierUpdate<u64>>,
            KafkaWriter<RecoveryKey<u64>, StateUpdate<Vec<u8>>>,
            KafkaWriter<RecoveryKey<u64>, StateUpdate<Vec<u8>>>,
        ) = py.allow_threads(|| {
            create_kafka_topic(hosts, &progress_topic, create_partitions);
            create_kafka_topic(hosts, &state_topic, create_partitions);

            (
                KafkaWriter::new(hosts, progress_topic, partition),
                KafkaWriter::new(hosts, state_topic.clone(), partition),
                KafkaWriter::new(hosts, state_topic, partition),
            )
        });

        Ok((
            Box::new(progress_writer),
            Box::new(state_writer),
            Box::new(state_collector),
        ))
    } else {
        Err(format!(
            "Unknown recovery_config type: {}",
            recovery_config.get_type(),
        ))
    }
}

/// Use a recovery config and the current worker's identity to build
/// out the specific recovery reader instances that this worker will
/// need to load recovery data.
///
/// This function is also the Python-Rust barrier for recovery; we
/// don't have any Python types in the recovery machinery after this.
///
/// We need to know worker count and index here because each worker
/// needs to read distinct loading data from a worker in the previous
/// dataflow execution.
///
/// Note that as of now, this code assumes that the number of workers
/// _has not changed between executions_. Things will silently not
/// fully load if worker count is changed.
pub(crate) fn build_recovery_readers(
    py: Python,
    worker_index: usize,
    worker_count: usize,
    recovery_config: Py<RecoveryConfig>,
) -> Result<
    (
        Box<dyn ProgressReader<u64>>,
        Box<dyn StateReader<u64, Vec<u8>>>,
    ),
    String,
> {
    // See comment about the GIL in
    // [`build_recovery_writers`].
    let recovery_config = recovery_config.as_ref(py);

    if let Ok(_noop_recovery_config) = recovery_config.downcast::<PyCell<NoopRecoveryConfig>>() {
        let (state_reader, progress_reader) =
            py.allow_threads(|| (NoopRecovery::new(), NoopRecovery::new()));
        Ok((Box::new(state_reader), Box::new(progress_reader)))
    } else if let Ok(sqlite_recovery_config) =
        recovery_config.downcast::<PyCell<SqliteRecoveryConfig>>()
    {
        let sqlite_recovery_config = sqlite_recovery_config.borrow();

        let db_file = sqlite_recovery_config.db_file(worker_index);

        let (progress_reader, state_reader) = py.allow_threads(|| {
            (
                SqliteProgressReader::new(&db_file),
                SqliteStateReader::new(&db_file),
            )
        });

        Ok((Box::new(progress_reader), Box::new(state_reader)))
    } else if let Ok(kafka_recovery_config) =
        recovery_config.downcast::<PyCell<KafkaRecoveryConfig>>()
    {
        let kafka_recovery_config = kafka_recovery_config.borrow();

        let hosts = &kafka_recovery_config.hosts;
        let state_topic = kafka_recovery_config.state_topic();
        let progress_topic = kafka_recovery_config.progress_topic();
        let partition = worker_index.try_into().unwrap();
        let create_partitions = worker_count.try_into().unwrap();

        let (progress_reader, state_reader) = py.allow_threads(|| {
            create_kafka_topic(hosts, &progress_topic, create_partitions);
            create_kafka_topic(hosts, &state_topic, create_partitions);

            (
                KafkaReader::new(hosts, &progress_topic, partition),
                KafkaReader::new(hosts, &state_topic, partition),
            )
        });

        Ok((Box::new(progress_reader), Box::new(state_reader)))
    } else {
        Err(format!(
            "Unknown recovery_config type: {}",
            recovery_config.get_type(),
        ))
    }
}

/// Implements all the recovery traits Bytewax needs, but does not
/// load or backup any data.
pub struct NoopRecovery;

impl NoopRecovery {
    pub fn new() -> Self {
        NoopRecovery {}
    }
}

impl<T: Debug, D: Debug> StateWriter<T, D> for NoopRecovery {
    fn write(&mut self, backup: &StateBackup<T, D>) {
        debug!("noop state write backup={backup:?}");
    }
}

impl<T: Debug> StateCollector<T> for NoopRecovery {
    fn delete(&mut self, recovery_key: &RecoveryKey<T>) {
        debug!("noop state delete recovery_key={recovery_key:?}");
    }
}

impl<T, D> StateReader<T, D> for NoopRecovery {
    fn read(&mut self) -> Option<StateBackup<T, D>> {
        debug!("noop state read");
        None
    }
}

impl<T: Debug> ProgressWriter<T> for NoopRecovery {
    fn write(&mut self, frontier_update: &FrontierUpdate<T>) {
        debug!("noop frontier write frontier_update={frontier_update:?}");
    }
}

impl<T> ProgressReader<T> for NoopRecovery {
    fn read(&mut self) -> Option<FrontierUpdate<T>> {
        debug!("noop frontier read");
        None
    }
}

struct SqliteStateWriter {
    rt: Runtime,
    conn: SqliteConnection,
    table_name: String,
}

impl SqliteStateWriter {
    fn new(db_file: &Path) -> Self {
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

impl<D> Type<Sqlite> for StateUpdate<D> {
    fn type_info() -> SqliteTypeInfo {
        <&[u8] as Type<Sqlite>>::type_info()
    }
}

impl<'q, D: Serialize> Encode<'q, Sqlite> for StateUpdate<D> {
    fn encode_by_ref(&self, args: &mut Vec<SqliteArgumentValue<'q>>) -> IsNull {
        let bytes = to_bytes(self);
        args.push(SqliteArgumentValue::Blob(Cow::Owned(bytes)));
        IsNull::No
    }
}

impl<'r, D: Deserialize<'r>> Decode<'r, Sqlite> for StateUpdate<D> {
    fn decode(value: SqliteValueRef<'r>) -> Result<Self, BoxDynError> {
        let value = <&[u8] as Decode<Sqlite>>::decode(value)?;
        Ok(from_bytes(value))
    }
}

impl<D> Type<Sqlite> for FrontierUpdate<D> {
    fn type_info() -> SqliteTypeInfo {
        <&[u8] as Type<Sqlite>>::type_info()
    }
}

impl<'q, T: Serialize> Encode<'q, Sqlite> for FrontierUpdate<T> {
    fn encode_by_ref(&self, args: &mut Vec<SqliteArgumentValue<'q>>) -> IsNull {
        let bytes = to_bytes(self);
        args.push(SqliteArgumentValue::Blob(Cow::Owned(bytes)));
        IsNull::No
    }
}

impl<'r, T: Deserialize<'r>> Decode<'r, Sqlite> for FrontierUpdate<T> {
    fn decode(value: SqliteValueRef<'r>) -> Result<Self, BoxDynError> {
        let value = <&[u8] as Decode<Sqlite>>::decode(value)?;
        Ok(from_bytes(value))
    }
}

impl<D: Send + Sync + Serialize + Debug> StateWriter<u64, D> for SqliteStateWriter {
    fn write(&mut self, backup: &StateBackup<u64, D>) {
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

struct SqliteStateReader<D> {
    rt: Runtime,
    rx: tokio::sync::mpsc::Receiver<StateBackup<u64, D>>,
}

impl<D: Unpin + Send + Sync + DeserializeOwned + Debug + 'static> SqliteStateReader<D> {
    fn new(db_file: &Path) -> Self {
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

impl<D: Debug> StateReader<u64, D> for SqliteStateReader<D> {
    fn read(&mut self) -> Option<StateBackup<u64, D>> {
        self.rt.block_on(self.rx.recv())
    }
}

struct SqliteProgressWriter {
    rt: Runtime,
    conn: SqliteConnection,
    table_name: String,
}

impl SqliteProgressWriter {
    fn new(db_file: &Path) -> Self {
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
    fn write(&mut self, frontier_update: &FrontierUpdate<u64>) {
        let sql = format!("INSERT INTO {} (name, antichain) VALUES (?1, ?2) ON CONFLICT (name) DO UPDATE SET antichain = EXCLUDED.antichain", self.table_name);
        let future = query(&sql)
            .bind("worker_frontier")
            .bind(frontier_update)
            .execute(&mut self.conn);
        self.rt.block_on(future).unwrap();

        debug!("sqlite frontier write frontier_update={frontier_update:?}");
    }
}

struct SqliteProgressReader {
    rt: Runtime,
    rx: tokio::sync::mpsc::Receiver<FrontierUpdate<u64>>,
}

impl SqliteProgressReader {
    fn new(db_file: &Path) -> Self {
        let table_name = "progress";

        let writer = SqliteProgressWriter::new(db_file);
        let rt = writer.rt;
        let mut conn = writer.conn;

        let (tx, rx) = tokio::sync::mpsc::channel(1);

        rt.spawn(async move {
            let sql =
                format!("SELECT antichain FROM {table_name} WHERE name = \"worker_frontier\"");
            let mut stream = query(&sql)
                .map(|row: SqliteRow| row.get::<FrontierUpdate<u64>, _>(0))
                .fetch(&mut conn)
                .map(|result| result.expect("Error selecting from SQLite"));

            while let Some(backup) = stream.next().await {
                tx.send(backup).await.unwrap();
            }
        });

        Self { rt, rx }
    }
}

impl ProgressReader<u64> for SqliteProgressReader {
    fn read(&mut self) -> Option<FrontierUpdate<u64>> {
        self.rt.block_on(self.rx.recv())
    }
}

fn create_kafka_topic(hosts: &[String], topic: &str, partitions: i32) {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();

    let admin: AdminClient<_> = rt.block_on(async {
        ClientConfig::new()
            .set("bootstrap.servers", hosts.join(","))
            .create()
            .expect("Error building Kafka admin")
    });
    let admin_options = AdminOptions::new();

    let new_topic = NewTopic {
        name: topic,
        num_partitions: partitions,
        // I believe this chooses the default replication factor.
        replication: TopicReplication::Fixed(-1),
        config: vec![("cleanup.policy", "compact")],
    };
    let future = admin.create_topics(vec![&new_topic], &admin_options);
    let result = rt
        .block_on(future)
        .expect("Error calling create Kafka topic on admin")
        .pop()
        .unwrap();
    match result {
        Ok(topic) => {
            debug!("Created Kafka topic={topic:?}");
        }
        Err((topic, rdkafka::types::RDKafkaErrorCode::TopicAlreadyExists)) => {
            debug!("Kafka topic={topic:?} already exists; continuing");
        }
        Err((topic, err_code)) => {
            panic!("Error creating Kafka topic={topic:?}: {err_code:?}")
        }
    }
}

/// This is a generic wrapper around [`BaseProducer`] which adds
/// serde and only writes to a single topic and partition.
struct KafkaWriter<K, P> {
    producer: BaseProducer,
    topic: String,
    partition: i32,
    key_type: PhantomData<K>,
    payload_type: PhantomData<P>,
}

impl<K: Serialize, P: Serialize> KafkaWriter<K, P> {
    fn new(hosts: &[String], topic: String, partition: i32) -> Self {
        debug!("Creating Kafka producer with hosts={hosts:?} topic={topic:?}");
        let producer: BaseProducer = ClientConfig::new()
            .set("bootstrap.servers", hosts.join(","))
            .create()
            .expect("Error building Kafka producer");

        Self {
            producer,
            topic,
            partition,
            key_type: PhantomData,
            payload_type: PhantomData,
        }
    }

    fn write(&self, key: &K, payload: &P) {
        let key_bytes = to_bytes(key);
        let payload_bytes = to_bytes(payload);
        let record = BaseRecord::to(&self.topic)
            .key(&key_bytes)
            .payload(&payload_bytes)
            .partition(self.partition);

        self.producer.send(record).expect("Error writing state");
        self.producer.poll(Timeout::Never);
    }

    fn delete(&self, key: &K) {
        let key_bytes = to_bytes(key);
        let record = BaseRecord::<Vec<u8>, Vec<u8>>::to(&self.topic)
            .key(&key_bytes)
            .partition(self.partition);
        // Explicitly no payload to mark as delete for key.

        self.producer.send(record).expect("Error deleting state");
        self.producer.poll(Timeout::Never);
    }
}

impl<T: Serialize + Debug, D: Serialize + Debug> StateWriter<T, D>
    for KafkaWriter<RecoveryKey<T>, StateUpdate<D>>
{
    fn write(&mut self, backup: &StateBackup<T, D>) {
        let StateBackup(recovery_key, state_update) = backup;
        KafkaWriter::write(self, recovery_key, state_update);
        debug!("kafka state write backup={backup:?}");
    }
}

impl<T: Serialize + Debug, D: Serialize> StateCollector<T>
    for KafkaWriter<RecoveryKey<T>, StateUpdate<D>>
{
    fn delete(&mut self, recovery_key: &RecoveryKey<T>) {
        KafkaWriter::delete(self, recovery_key);
        debug!("kafka state delete recovery_key={recovery_key:?}");
    }
}

/// This is a generic wrapper around [`BaseConsumer`] which adds
/// serde and reads from only a single topic and partition.
struct KafkaReader<K, P> {
    consumer: BaseConsumer,
    key_type: PhantomData<K>,
    payload_type: PhantomData<P>,
}

impl<K: DeserializeOwned, P: DeserializeOwned> KafkaReader<K, P> {
    fn new(hosts: &[String], topic: &str, partition: i32) -> Self {
        debug!("Loading recovery data from hosts={hosts:?} topic={topic:?}");
        let consumer: BaseConsumer = ClientConfig::new()
            .set("bootstrap.servers", hosts.join(","))
            // We don't want to use consumer groups because
            // re-balancing makes no sense in the recovery
            // context. librdkafka requires you to set a consumer
            // group, though, but they say it is never used if you
            // don't call
            // `subscribe`. https://github.com/edenhill/librdkafka/issues/593#issuecomment-278954990
            .set("group.id", "BYTEWAX_IGNORED")
            .set("enable.auto.commit", "false")
            .set("enable.partition.eof", "true")
            .create()
            .expect("Error building Kafka consumer");

        let mut partitions = TopicPartitionList::new();
        partitions
            .add_partition_offset(topic, partition, Offset::Beginning)
            .unwrap();
        consumer
            .assign(&partitions)
            .expect("Error assigning Kafka recovery topic");

        Self {
            consumer,
            key_type: PhantomData,
            payload_type: PhantomData,
        }
    }

    fn read(&mut self) -> Option<(Option<K>, Option<P>)> {
        let msg_result = self.consumer.poll(Timeout::Never);
        match msg_result {
            Some(Ok(msg)) => Some((msg.key().map(from_bytes), msg.payload().map(from_bytes))),
            Some(Err(KafkaError::PartitionEOF(_))) => None,
            Some(Err(err)) => panic!("Error reading from Kafka topic: {err:?}"),
            None => None,
        }
    }
}

impl<T: DeserializeOwned, D: DeserializeOwned> StateReader<T, D>
    for KafkaReader<RecoveryKey<T>, StateUpdate<D>>
{
    fn read(&mut self) -> Option<StateBackup<T, D>> {
        loop {
            match KafkaReader::read(self) {
                // Skip deletions if they haven't been compacted.
                Some((_, None)) => continue,
                Some((Some(recovery_key), Some(state_update))) => {
                    return Some(StateBackup(recovery_key, state_update));
                }
                Some((None, _)) => panic!("Missing key in reading state Kafka topic"),
                None => return None,
            }
        }
    }
}

impl<T: Serialize + Debug> ProgressWriter<T> for KafkaWriter<String, FrontierUpdate<T>> {
    fn write(&mut self, frontier_update: &FrontierUpdate<T>) {
        KafkaWriter::write(self, &String::from("worker_frontier"), &frontier_update);
        debug!("kafka frontier write frontier_backup={frontier_update:?}");
    }
}

impl<T: DeserializeOwned + Debug> ProgressReader<T> for KafkaReader<String, FrontierUpdate<T>> {
    fn read(&mut self) -> Option<FrontierUpdate<T>> {
        match KafkaReader::read(self) {
            Some((Some(_), Some(frontier_update))) => {
                debug!("kafka frontier read frontier_update={frontier_update:?}");
                Some(frontier_update)
            }
            None => None,
            _ => panic!("Missing payload in reading frontier Kafka topic"),
        }
    }
}

pub(crate) fn register(_py: Python, m: &PyModule) -> PyResult<()> {
    m.add_class::<RecoveryConfig>()?;
    m.add_class::<SqliteRecoveryConfig>()?;
    m.add_class::<KafkaRecoveryConfig>()?;
    Ok(())
}
