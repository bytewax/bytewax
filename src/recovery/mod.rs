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
//! Stateful Logic
//! --------------
//!
//! To create a new stateful operator, create a new
//! [`StatefulLogic`] impl and pass it to the
//! [`StatefulUnary`] Timely operator. If you
//! fullfil the API of [`StatefulLogic`], you will
//! get proper recovery behavior.
//!
//! The general idea is that you pass a **logic builder** which takes
//! any previous state snapshots from the last execution and builds an
//! instance of your logic. Then your logic is **snapshotted** at the
//! end of each epoch, and that state durably saved in the recovery
//! store.
//!
//! Stateful Inputs
//! ---------------
//!
//! Input components use a different system, since they need to
//! interplay with the `source` epoch generating system. In general,
//! create a new **input reader** [`crate::inputs::InputReader`] and
//! make sure to snapshot all relevant state.
//!
//! Architecture
//! ------------
//!
//! Recovery is based around snapshotting the state of each stateful
//! operator in worker at the end of each epoch and recording that
//! worker's progress information. The history of state changes across
//! epochs is stored so that we can recover to a consistent point in
//! time (the beginning of an epoch). Each worker's recovery data is
//! stored separately so that we can discern the exact state of each
//! worker at the point of failure.
//!
//! The **worker frontier** represents the oldest epoch on a given
//! worker for which items are still in the process of being output or
//! recovery updates are being written.
//!
//! We model the recovery store for each worker as a key-value
//! database with two "tables" for each worker: a **state table** and
//! a **progress table**. Stateful operators emit CDC-like change
//! updates downstream and the other recovery operators actually
//! perform those changes to the recovery store's tables.
//!
//! The state table backs up all state related to each user-determined
//! [`StateKey`] in each operator. A change to this table is
//! represented by [`StateUpdate`], with [`StateRecoveryKey`] and
//! [`StateOp`] representing the key and operation performed on the
//! value (update, delete) respectively.
//!
//! The progress table backs up changes to the worker frontier. Each
//! modification to this table is represented by [`ProgressUpdate`],
//! with [`ProgressRecoveryKey`] and [`ProgressOp`] representing the
//! key and operation performed on the value repectively.
//!
//! The state within each logic is snapshotted as generic
//! [`StateBytes`], not typed data. We do this so we can interleave
//! multiple different concrete kinds of state information from
//! different stateful operators together through the same parts of
//! the recovery dataflow without Rust type gymnastics.
//!
//! Note that backing up the fact that the logic for a [`StateKey`]
//! was deleted ([`StateOp::Discard`]) is not the same as GCing
//! unneeded recovery state. We need to explicitly save the history of
//! all deletions over epochs in case we need to recover right after a
//! deletion; that logic should not be recovered. Separately, once we
//! know and we'll never need to recover before a certain epoch do we
//! actually GC those old writes from the recovery store.
//!
//! There are 5 traits which provides an interface to the abstract
//! idea of a KVDB that is the recovery store: [`ProgressReader`],
//! [`ProgressWriter`], [`StateReader`], [`StateWriter`], and
//! [`StateCollector`]. To implement a new backing recovery store,
//! create a new impl of those. Because each worker's data needs to be
//! kept separate, they are parameterized on creation by worker index
//! and count to ensure data is routed appropriately for each worker.
//!
//! The recovery system uses a few custom utility operators
//! ([`WriteState`],
//! [`WriteProgress`],
//! [`CollectGarbage`],
//! [`state_source`],
//! [`progress_source`]) to implement
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
//! subgraph "Resume Epoch Calc Dataflow"
//! EI{{progress_source}} -- ProgressUpdate --> EAG{{Aggregate}} -- worker frontier --> LB{{Broadcast}} -- Progress --> EAC{{Accumulate}} -- cluster frontier --> EM{{Map}}
//! EI -. probe .-> EM
//! end
//!
//! EM == resume epoch ==> I & SI
//!
//! subgraph "State Loading Dataflow"
//! SI{{state_source}} -. probe .-> SSUF & SCUF
//! SI -- StateUpdate --> SSUF{{Unary Frontier}}
//! SI -- StateUpdate --> SCUF{{Unary Frontier}}
//! end
//!
//! SSUF == RecoveryStoreSummary ==> GC
//! SCUF == recovery state ==> SOX & SOY
//!
//! subgraph "Production Dataflow"
//! I(Input) -- items --> XX1([...]) -- "(key, value)" --> SOX(StatefulUnary X) & SOY(StatefulUnary Y)
//! SOX & SOY -- "(key, value)" --> XX2([...]) -- items --> O1(Output 1) & O2(Output 2)
//!
//! O1 & O2 -- items --> OM{{Map}}
//! I & SOX & SOY -- StateUpdate --> SOC
//! SOC{{Concat}} -- StateUpdate --> SB{{WriteState}}
//! SB -- StateUpdate --> BM{{Map}}
//! OM & BM -- heartbeats --> DFC{{Concat}} -- heartbeats / worker frontier --> FB{{WriteProgress}} -- ProgressUpdate / dataflow frontier --> DFB{{Broadcast}} -- ProgressUpdate / dataflow frontier --> GC
//! SB -- StateUpdate --> GC{{CollectGarbage}}
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
//! We currently have a few user-facing stateful operators
//! (e.g. [`crate::dataflow::Dataflow::reduce`],
//! [`crate::dataflow::Dataflow::stateful_map`]). But they are all
//! implemented on top of a general underlying one:
//! [`crate::recovery::StatefulUnary`]. This means all in-operator
//! recovery-related code is only written once.
//!
//! Stateful unary does not modify the recovery store itself. Instead,
//! each stateful operator generates a second **state update stream**
//! of state changes over time as an output of [`StateUpdate`]s. These
//! are then connected to the rest of the recovery components.
//!
//! Stateful inputs (as opposed to stateful operators) don't use
//! [`StatefulUnary`] because they need to generate their own
//! epochs. Instead the
//! [`crate::execution::periodic_epoch::periodic_epoch_source`]
//! operator asks [`crate::input::InputReader`] to snapshot its state
//! and similarly produces a stream of [`StateUpdates`]s.
//!
//! All state updates from all stateful operators are routed to the
//! [`WriteState`] operator, which actually performs the writes to the
//! recovery store via the [`StateWriter`]. It emits the updates after
//! writing downstream so progress can be monitored.
//!
//! The [`WriteProgress`] operator then looks at the **worker
//! frontier**, the combined stream of written updates and all
//! captures. This will be written via the [`ProgressWriter`] and
//! emits these per-worker [`ProgressUpdate`]s downstream.
//!
//! These worker frontier [`ProgressUpdate`]s are then broadcast so
//! operators listening to this stream will see progress of the entire
//! dataflow cluster, the **dataflow frontier**.
//!
//! The dataflow frontier and completed state backups are then fed
//! into the [`crate::recovery::CollectGarbage`] operator. It uses the
//! dataflow frontier to detect when some state is no longer necessary
//! for recovery and issues deletes via the [`StateCollector`]. GC
//! keeps an in-memory summary of the keys and epochs that are
//! currently in the recovery store so reads are not necessary. It
//! writes out heartbeats of progress so the worker can probe
//! execution progress.
//!
//! The progress of GC is what is probed to rate-limit execution, not
//! just captures, so we don't queue up too much recovery work.
//!
//! On Resume
//! ---------
//!
//! The function [`crate::execution::worker_main`] is where loading
//! starts. It's broken into three dataflows, built in
//! [`build_resume_epoch_calc_dataflow`],
//! [`build_state_loading_dataflow`], and
//! [`build_production_dataflow`].
//!
//! First, the resume epoch must be calculated from the
//! [`ProgressUpdate`]s actually written to the recovery store. This
//! can't be pre-calculated during backup because that write might
//! fail.
//!
//! This is done in a separate dataflow because it needs a unique
//! epoch definition: we need to know when we're done reading all
//! recovery data, which would be impossible if we re-used the epoch
//! definition from the backed up dataflow (because that would mean it
//! completed).
//!
//! Once we have the resume epoch, we know what previously backed up
//! data is relevant (and not too new) and can start loading that onto
//! each worker.
//!
//! A second separate dataflow does this loading. Each resume worker
//! is assigned to read all state _before_ the resume epoch. We read
//! all [`StateUpdate`]s for each worker and the final update is
//! loaded into maps by [`StepId`] and [`StateKey`]. This state data
//! also is used to produce a [`RecoveryStoreSummary`] so that the
//! [`CollectGarbage`] operator has a correct cache of all
//! previously-written [`RecoveryStateKey`]s.
//!
//! Once the state loading is complete, the resulting **resume state**
//! is handed off to the production dataflow. It is routed to the
//! correct stateful operators by [`StepId`], then deserialized to
//! produce the final relevant state with [`StatefulUnary`] for each
//! [`StateKey`].
//!
//! If the underlying data or bug has been fixed, then the production
//! dataflow should resume with the state from the end of the epoch
//! just before failure, with the input resuming from beginning of the
//! next epoch.

use chrono::DateTime;
use chrono::Utc;
use log::trace;
use pyo3::exceptions::PyTypeError;
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3::types::*;
use serde::de::DeserializeOwned;
use serde::Deserialize;
use serde::Serialize;
use std::any::type_name;
use std::collections::hash_map::DefaultHasher;
use std::collections::hash_map::Entry;
use std::collections::BTreeSet;
use std::collections::HashMap;
use std::collections::HashSet;
use std::fmt::Debug;
use std::fmt::Display;
use std::hash::Hash;
use std::hash::Hasher;
use std::task::Poll;
use std::time::Duration;
use timely::communication::Allocate;
use timely::dataflow::channels::pact;
use timely::dataflow::channels::pact::ParallelizationContract;
use timely::dataflow::operators::aggregation::Aggregate;
use timely::dataflow::operators::flow_controlled::iterator_source;
use timely::dataflow::operators::flow_controlled::IteratorSourceInput;
use timely::dataflow::operators::generic::builder_rc::OperatorBuilder;
use timely::dataflow::operators::generic::FrontieredInputHandle;
use timely::dataflow::operators::*;
use timely::dataflow::ProbeHandle;
use timely::dataflow::Scope;
use timely::dataflow::ScopeParent;
use timely::dataflow::Stream;
use timely::order::TotalOrder;
use timely::progress::Antichain;
use timely::progress::Timestamp;
use timely::worker::Worker;
use timely::Data;
use timely::ExchangeData;

use crate::execution::WorkerIndex;
use crate::StringResult;

use self::kafka::create_kafka_topic;
use self::kafka::KafkaReader;
use self::kafka::KafkaRecoveryConfig;
use self::kafka::KafkaWriter;
use self::sqlite::SqliteProgressReader;
use self::sqlite::SqliteProgressWriter;
use self::sqlite::SqliteRecoveryConfig;
use self::sqlite::SqliteStateReader;
use self::sqlite::SqliteStateWriter;

pub(crate) mod kafka;
pub(crate) mod sqlite;

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

impl RecoveryConfig {
    /// Create an "empty" [`Self`] just for use in `__getnewargs__`.
    #[allow(dead_code)]
    pub(crate) fn pickle_new(py: Python) -> Py<Self> {
        PyCell::new(py, RecoveryConfig {}).unwrap().into()
    }
}

#[pymethods]
impl RecoveryConfig {
    #[new]
    fn new() -> Self {
        Self {}
    }

    /// Pickle as a tuple.
    fn __getstate__(&self) -> (&str,) {
        ("RecoveryConfig",)
    }

    /// Unpickle from tuple of arguments.
    fn __setstate__(&mut self, state: &PyAny) -> PyResult<()> {
        if let Ok(("RecoveryConfig",)) = state.extract() {
            Ok(())
        } else {
            Err(PyValueError::new_err(format!(
                "bad pickle contents for RecoveryConfig: {state:?}"
            )))
        }
    }
}

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

// Here's our public facing recovery operator data model and trait.

/// Unique name for a step in a dataflow.
///
/// Used as a key for looking up relevant state for different steps
/// surrounding recovery.
#[derive(
    Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize, FromPyObject,
)]
pub(crate) struct StepId(String);

impl Display for StepId {
    fn fmt(&self, fmt: &mut std::fmt::Formatter) -> Result<(), std::fmt::Error> {
        fmt.write_str(&self.0)
    }
}

impl IntoPy<PyObject> for StepId {
    fn into_py(self, py: Python) -> Py<PyAny> {
        self.0.into_py(py)
    }
}

/// Routing key for stateful operators.
///
/// We place restraints on this, rather than allowing any Python type
/// to be routeable because the routing key interfaces with a lot of
/// Bytewax and Timely code which puts requirements on it: it has to
/// be hashable, have equality, debug printable, and is serde-able and
/// we can't guarantee those things are correct on any arbitrary
/// Python type.
///
/// Yes, we lose a little bit of flexibility, but it makes usage more
/// convenient.
///
/// You'll mostly interface with this via [`extract_state_pair`] and
/// [`wrap_state_pair`].
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub(crate) enum StateKey {
    /// An arbitrary string key.
    Hash(String),
    /// Route to a specific worker.
    Worker(WorkerIndex),
}

impl StateKey {
    /// Hash this key for Timely.
    ///
    /// Timely uses the result here to decide which worker to send
    /// this data.
    pub(crate) fn route(&self) -> u64 {
        match self {
            Self::Hash(key) => {
                let mut hasher = DefaultHasher::new();
                key.hash(&mut hasher);
                hasher.finish()
            }
            Self::Worker(index) => index.route_to(),
        }
    }
}

impl<'source> FromPyObject<'source> for StateKey {
    fn extract(ob: &'source PyAny) -> PyResult<Self> {
        if let Ok(py_string) = ob.cast_as::<PyString>() {
            Ok(Self::Hash(py_string.to_str()?.into()))
        } else if let Ok(py_int) = ob.cast_as::<PyLong>() {
            Ok(Self::Worker(WorkerIndex(py_int.extract()?)))
        } else {
            Err(PyTypeError::new_err("Can only make StateKey out of either str (route to worker by hash) or int (route to worker by index)"))
        }
    }
}

impl IntoPy<PyObject> for StateKey {
    fn into_py(self, py: Python) -> Py<PyAny> {
        match self {
            Self::Hash(key) => key.into_py(py),
            Self::Worker(index) => index.0.into_py(py),
        }
    }
}

/// Dataflow streams that go into stateful operators are routed by
/// state key.
pub(crate) type StatefulStream<S, V> = Stream<S, (StateKey, V)>;

/// A serialized snapshot of operator state.
///
/// The recovery system only deals in bytes so each operator can store
/// custom types.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct StateBytes(Vec<u8>);

impl StateBytes {
    /// Serialize this state object from an operator into bytes the
    /// recovery system can store.
    pub(crate) fn ser<T: Serialize>(obj: &T) -> Self {
        // TODO: Figure out if there's a more robust-to-evolution way
        // to serialize this key. If the serialization changes between
        // versions, then recovery doesn't work. Or if we use an
        // encoding that isn't deterministic.
        let t_name = type_name::<T>();
        Self(
            bincode::serialize(obj)
                .unwrap_or_else(|_| panic!("Error serializing recovery state type {t_name})")),
        )
    }

    /// Deserialize these bytes from the recovery system into a state
    /// object that an operator can use.
    pub(crate) fn de<T: DeserializeOwned>(self) -> T {
        let t_name = type_name::<T>();
        bincode::deserialize(&self.0)
            .unwrap_or_else(|_| panic!("Error deserializing recovery state type {t_name})"))
    }
}

// Here's our recovery data model.

/// Key used to address progress data within a recovery store.
///
/// Progress data is key'd per-worker so we can combine together all
/// [`ProgressUpdate`]s for a cluster and determine each worker's
/// position.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub(crate) struct ProgressRecoveryKey {
    worker_index: WorkerIndex,
}

impl ProgressRecoveryKey {
    /// Route all progress updates for a given worker to the same
    /// location.
    fn route_by_worker(&self) -> u64 {
        self.worker_index.route_to()
    }
}

/// A snapshot of progress data for a worker.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct Progress<T> {
    /// Worker's frontier.
    frontier: T,
}

/// Possible modification operations that can be performed for each
/// [`ProgressRecoveryKey`].
///
/// Since progress can't be "deleted", we only allow upserts.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) enum ProgressOp<T> {
    /// Snapshot of new progress data.
    Upsert(Progress<T>),
}

/// An update to the progress table of the recovery store.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct ProgressUpdate<T>(pub(crate) ProgressRecoveryKey, pub(crate) ProgressOp<T>);

/// Timely stream containing progress changes for the recovery system.
pub(crate) type ProgressUpdateStream<S> = Stream<S, ProgressUpdate<<S as ScopeParent>::Timestamp>>;

/// Key used to address state data within a recovery store.
///
/// Remember, this isn't the same as [`StateKey`], as the "address
/// space" of that key is just within a single operator. This type
/// includes the operator name and epoch too, so we can address state
/// in an entire dataflow and across time.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct StateRecoveryKey<T> {
    pub(crate) step_id: StepId,
    pub(crate) state_key: StateKey,
    pub(crate) epoch: T,
}

/// Snapshot of state data.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct State {
    pub(crate) state_bytes: StateBytes,
    pub(crate) next_awake: Option<DateTime<Utc>>,
}

/// Possible modification operations that can be performed for each
/// [`StateKey`], and thus each [`StateRecoveryKey`].
///
/// This is related to [`LogicFate`], as that is the final result of
/// what should happen to the info for each [`StateKey`].
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) enum StateOp {
    /// Snapshot of new state data.
    Upsert(State),
    /// This [`StateKey`] was deleted. Mark that the data is deleted.
    Discard,
}

/// An update to the state table of the recovery store.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct StateUpdate<T>(pub(crate) StateRecoveryKey<T>, pub(crate) StateOp);

impl<T: Timestamp> StateUpdate<T> {
    /// Route in Timely just by the state key to ensure that all
    /// relevant data ends up on the correct worker.
    fn pact_for_state_key() -> impl ParallelizationContract<T, StateUpdate<T>> {
        pact::Exchange::new(|StateUpdate(StateRecoveryKey { state_key, .. }, ..)| state_key.route())
    }
}

/// Timely stream containing state changes for the recovery system.
pub(crate) type StateUpdateStream<S> = Stream<S, StateUpdate<<S as ScopeParent>::Timestamp>>;

// Here's our core traits for recovery that allow us to swap out
// underlying storage.

pub(crate) trait ProgressWriter<T> {
    fn write(&mut self, update: &ProgressUpdate<T>);
}

pub(crate) trait ProgressReader<T> {
    /// Has the same semantics as [`std::iter::Iterator::next`]:
    /// return [`None`] to signal EOF.
    fn read(&mut self) -> Option<ProgressUpdate<T>>;
}

pub(crate) trait StateWriter<T> {
    fn write(&mut self, update: &StateUpdate<T>);
}

pub(crate) trait StateCollector<T> {
    fn delete(&mut self, key: &StateRecoveryKey<T>);
}

pub(crate) trait StateReader<T> {
    /// Has the same semantics as [`std::iter::Iterator::next`]:
    /// return [`None`] to signal EOF.
    fn read(&mut self) -> Option<StateUpdate<T>>;
}

// Here are our loading dataflows.

/// Compile a dataflow which reads the progress data from the previous
/// execution and calculates the resume epoch.
///
/// Each resume cluster worker is assigned to read the entire progress
/// data from some failed cluster worker.
///
/// 1. Find the oldest frontier for the worker since we want to know
/// how far it got. (That's the first accumulate).
///
/// 2. Broadcast all our oldest per-worker frontiers.
///
/// 3. Find the earliest worker frontier since we want to resume from
/// the last epoch fully completed by all workers in the
/// cluster. (That's the second accumulate).
///
/// 4. Send the resume epoch out of the dataflow via a channel.
///
/// Once the progress input is done, this dataflow will send the
/// resume epoch through a channel. The main function should consume
/// from that channel to pass on to the other loading and production
/// dataflows.
pub(crate) fn build_resume_epoch_calc_dataflow<A, T>(
    timely_worker: &mut Worker<A>,
    // TODO: Allow multiple (or none) FrontierReaders so you can recover a
    // different-sized cluster.
    progress_reader: Box<dyn ProgressReader<T>>,
    resume_epoch_tx: std::sync::mpsc::Sender<T>,
) -> StringResult<ProbeHandle<()>>
where
    A: Allocate,
    T: Timestamp,
{
    timely_worker.dataflow(|scope| {
        let mut probe = ProbeHandle::new();

        progress_source(scope, progress_reader, &probe)
            .map(|ProgressUpdate(key, op)| (key, op))
            .aggregate(
                |_key, op, worker_frontier_acc: &mut Option<T>| {
                    let worker_frontier = worker_frontier_acc.get_or_insert(T::minimum());
                    // For each worker in the failed cluster, find the
                    // latest frontier.
                    match op {
                        ProgressOp::Upsert(progress) => {
                            if &progress.frontier > worker_frontier {
                                *worker_frontier = progress.frontier;
                            }
                        }
                    }
                },
                |key, worker_frontier_acc| {
                    (
                        key.worker_index,
                        worker_frontier_acc
                            .expect("Did not update worker_frontier_acc in aggregation"),
                    )
                },
                ProgressRecoveryKey::route_by_worker,
            )
            // Each worker in the recovery cluster reads only some of
            // the progress data of workers in the failed
            // cluster. Broadcast to ensure all recovery cluster
            // workers can calculate the dataflow frontier.
            .broadcast()
            .accumulate(None, |dataflow_frontier_acc, worker_frontiers| {
                for (_worker_index, worker_frontier) in worker_frontiers.iter() {
                    let dataflow_frontier =
                        dataflow_frontier_acc.get_or_insert(worker_frontier.clone());
                    // The slowest of the workers in the failed
                    // cluster is the resume epoch.
                    if worker_frontier < dataflow_frontier {
                        *dataflow_frontier = worker_frontier.clone();
                    }
                }
            })
            .map(move |resume_epoch| {
                resume_epoch_tx
                    .send(resume_epoch.expect("Did not update dataflow_frontier_acc in accumulate"))
                    .unwrap()
            })
            .probe_with(&mut probe);

        Ok(probe)
    })
}

/// Compile a dataflow which reads state data from the previous
/// execution and loads state into hash maps per step ID and key, and
/// prepares the recovery store summary.
///
/// Once the state input is done, this dataflow will send the
/// collected recovery data through these channels. The main function
/// should consume from the channels to pass on to the production
/// dataflow.
pub(crate) fn build_state_loading_dataflow<A>(
    timely_worker: &mut Worker<A>,
    state_reader: Box<dyn StateReader<u64>>,
    resume_epoch: u64,
    resume_state_tx: std::sync::mpsc::Sender<(StepId, HashMap<StateKey, State>)>,
    recovery_store_summary_tx: std::sync::mpsc::Sender<RecoveryStoreSummary<u64>>,
) -> StringResult<ProbeHandle<u64>>
where
    A: Allocate,
{
    timely_worker.dataflow(|scope| {
        let mut probe = ProbeHandle::new();

        let source = state_source(scope, state_reader, resume_epoch, &probe);

        source
            .unary_frontier(
                StateUpdate::pact_for_state_key(),
                "RecoveryStoreSummaryCalc",
                |_init_cap, _info| {
                    let mut fncater = FrontierNotificator::new();

                    let mut tmp_incoming = Vec::new();
                    let mut resume_state_buffer = HashMap::new();
                    let mut recovery_store_summary = Some(RecoveryStoreSummary::new());

                    move |input, output| {
                        input.for_each(|cap, incoming| {
                            let epoch = cap.time();
                            assert!(tmp_incoming.is_empty());
                            incoming.swap(&mut tmp_incoming);

                            resume_state_buffer
                                .entry(*epoch)
                                .or_insert_with(Vec::new)
                                .append(&mut tmp_incoming);

                            fncater.notify_at(cap.retain());
                        });

                        fncater.for_each(&[input.frontier()], |cap, _ncater| {
                            let epoch = cap.time();
                            if let Some(updates) = resume_state_buffer.remove(epoch) {
                                let recovery_store_summary = recovery_store_summary
                                    .as_mut()
                                    .expect(
                                    "More input after recovery store calc input frontier was empty",
                                );
                                for update in &updates {
                                    recovery_store_summary.insert(update);
                                }
                            }

                            // Emit heartbeats so we can monitor progress
                            // at the probe.
                            output.session(&cap).give(());
                        });

                        if input.frontier().is_empty() {
                            if let Some(recovery_store_summary) = recovery_store_summary.take() {
                                recovery_store_summary_tx
                                    .send(recovery_store_summary)
                                    .unwrap();
                            }
                        }
                    }
                },
            )
            .probe_with(&mut probe);

        source
            .unary_frontier(StateUpdate::pact_for_state_key(), "StateCacheCalc", |_init_cap, _info| {
                let mut fncater = FrontierNotificator::new();

                let mut tmp_incoming = Vec::new();
                let mut resume_state_buffer = HashMap::new();
                let mut resume_state: Option<HashMap<StepId, HashMap<StateKey, State>>> =
                    Some(HashMap::new());

                move |input, output| {
                    input.for_each(|cap, incoming| {
                        let epoch = cap.time();
                        assert!(tmp_incoming.is_empty());
                        incoming.swap(&mut tmp_incoming);

                        resume_state_buffer
                            .entry(*epoch)
                            .or_insert_with(Vec::new)
                            .append(&mut tmp_incoming);

                        fncater.notify_at(cap.retain());
                    });

                    fncater.for_each(&[input.frontier()], |cap, _ncater| {
                        let epoch = cap.time();
                        if let Some(updates) = resume_state_buffer.remove(epoch) {
                            for StateUpdate(recovery_key, op) in updates {
                                let StateRecoveryKey { step_id, state_key, epoch: found_epoch } = recovery_key;
                                assert!(&found_epoch == epoch);

                                let resume_state =
                                    resume_state
                                    .as_mut()
                                    .expect("More input after resume state calc input frontier was empty")
                                    .entry(step_id)
                                    .or_default();

                                match op {
                                    StateOp::Upsert(state) => {
                                        resume_state.insert(state_key.clone(), state);
                                    },
                                    StateOp::Discard => {
                                        resume_state.remove(&state_key);
                                    },
                                };
                            }
                        }

                        // Emit heartbeats so we can monitor progress
                        // at the probe.
                        output.session(&cap).give(());
                    });

                    if input.frontier().is_empty() {
                        for resume_state in resume_state.take().expect("More input after resume state calc input frontier was empty") {
                            resume_state_tx.send(resume_state).unwrap();
                        }
                    }
                }
            })
            .probe_with(&mut probe);

        Ok(probe)
    })
}

// Here's operators related to recovery.

/// Build a source which loads previously backed up progress data as
/// separate items.
///
/// Note that [`T`] is the epoch of the previous, failed dataflow that
/// is being read. [`S::Timestamp`] is the timestamp used in the
/// recovery epoch calculation dataflow.
///
/// The resulting stream only has the zeroth epoch.
///
/// Note that this pretty meta! This new _loading_ dataflow will only
/// have the zeroth epoch, but you can observe what progress was made
/// on the _previous_ dataflow.
pub(crate) fn progress_source<S, T>(
    scope: &S,
    mut reader: Box<dyn ProgressReader<T>>,
    probe: &ProbeHandle<S::Timestamp>,
) -> Stream<S, ProgressUpdate<T>>
where
    S: Scope,
    T: Timestamp + Data,
    S::Timestamp: TotalOrder,
{
    iterator_source(
        scope,
        "ProgressSource",
        move |last_cap| {
            reader.read().map(|update| IteratorSourceInput {
                lower_bound: S::Timestamp::minimum(),
                // An iterator of (timestamp, iterator of
                // items). Nested [`IntoIterator`]s.
                data: Some((S::Timestamp::minimum(), Some(update))),
                target: last_cap.clone(),
            })
        },
        probe.clone(),
    )
}

/// Build a source which loads previously backed up state data.
///
/// The resulting stream has each state update in its original epoch.
///
/// State must be stored in epoch order. [`WriteState`] does that.
pub(crate) fn state_source<S>(
    scope: &S,
    mut reader: Box<dyn StateReader<S::Timestamp>>,
    stop_at: S::Timestamp,
    probe: &ProbeHandle<S::Timestamp>,
) -> StateUpdateStream<S>
where
    S: Scope,
    S::Timestamp: TotalOrder,
{
    iterator_source(
        scope,
        "StateSource",
        move |last_cap| match reader.read() {
            Some(update) => {
                let StateUpdate(key, _op) = &update;
                let StateRecoveryKey { epoch, .. } = key;
                let epoch = epoch.clone();

                if epoch < stop_at {
                    Some(IteratorSourceInput {
                        lower_bound: S::Timestamp::minimum(),
                        // An iterator of (timestamp, iterator of
                        // items). Nested [`IntoIterators`].
                        data: Some((epoch, Some(update))),
                        target: last_cap.clone(),
                    })
                } else {
                    None
                }
            }
            None => None,
        },
        probe.clone(),
    )
}

/// Extension trait for [`Stream`].
pub(crate) trait WriteState<S>
where
    S: Scope,
{
    /// Writes state backups in timestamp order.
    fn write_state_with(&self, writer: Box<dyn StateWriter<S::Timestamp>>) -> StateUpdateStream<S>;
}

impl<S> WriteState<S> for StateUpdateStream<S>
where
    S: Scope,
{
    fn write_state_with(
        &self,
        mut writer: Box<dyn StateWriter<S::Timestamp>>,
    ) -> StateUpdateStream<S> {
        self.unary_notify(pact::Pipeline, "WriteState", None, {
            let mut tmp_incoming = Vec::new();
            let mut incoming_buffer = HashMap::new();

            move |input, output, ncater| {
                input.for_each(|cap, incoming| {
                    let epoch = cap.time();
                    assert!(tmp_incoming.is_empty());
                    incoming.swap(&mut tmp_incoming);

                    incoming_buffer
                        .entry(epoch.clone())
                        .or_insert_with(Vec::new)
                        .append(&mut tmp_incoming);

                    ncater.notify_at(cap.retain());
                });

                // Use the notificator to ensure state is written in
                // epoch order.
                ncater.for_each(|cap, _count, _ncater| {
                    let epoch = cap.time();
                    if let Some(updates) = incoming_buffer.remove(epoch) {
                        for update in updates {
                            writer.write(&update);
                            output.session(&cap).give(update);
                        }
                    }
                });
            }
        })
    }
}

/// Extension trait for [`Stream`].
pub(crate) trait WriteProgress<S, D>
where
    S: Scope,
    D: Data,
{
    /// Write out the current frontier of the output this is connected
    /// to whenever it changes.
    fn write_progress_with(
        &self,
        writer: Box<dyn ProgressWriter<S::Timestamp>>,
    ) -> ProgressUpdateStream<S>;
}

impl<S, D> WriteProgress<S, D> for Stream<S, D>
where
    S: Scope,
    D: Data,
{
    fn write_progress_with(
        &self,
        mut writer: Box<dyn ProgressWriter<S::Timestamp>>,
    ) -> ProgressUpdateStream<S> {
        self.unary_notify(pact::Pipeline, "WriteProgress", None, {
            let worker_index = WorkerIndex(self.scope().index());

            let mut tmp_incoming = Vec::new();

            move |input, output, ncater| {
                input.for_each(|cap, incoming| {
                    // We drop the old data in tmp_incoming since we
                    // don't care about the items.
                    incoming.swap(&mut tmp_incoming);

                    ncater.notify_at(cap.retain());
                });

                ncater.for_each(|cap, _count, ncater| {
                    // 0 is our singular input.
                    let frontier = ncater.frontier(0).to_owned();

                    // Don't write out the last "empty" frontier to
                    // allow restarting from the end of the dataflow.
                    if !frontier.elements().is_empty() {
                        // TODO: Is this the right way to transform a frontier
                        // back into a recovery epoch?
                        let frontier = frontier
                            .elements()
                            .iter()
                            .cloned()
                            .min()
                            .unwrap_or_else(S::Timestamp::minimum);
                        let progress = Progress { frontier };
                        let key = ProgressRecoveryKey { worker_index };
                        let op = ProgressOp::Upsert(progress);
                        let update = ProgressUpdate(key, op);
                        writer.write(&update);
                        output.session(&cap).give(update);
                    }
                });
            }
        })
    }
}

/// Extension trait for [`Stream`].
pub(crate) trait CollectGarbage<S>
where
    S: Scope,
{
    /// Run the recovery garbage collector.
    ///
    /// This will be instantiated on stream of already written state
    /// and will observe the dataflow's frontier and then decide what
    /// is garbage and delete it.
    ///
    /// It needs a separate handle to write to the state store so that
    /// there's not contention between it and [`WriteState`].
    fn collect_garbage(
        &self,
        recovery_store_summary: RecoveryStoreSummary<S::Timestamp>,
        state_collector: Box<dyn StateCollector<S::Timestamp>>,
        dataflow_frontier: ProgressUpdateStream<S>,
    ) -> Stream<S, ()>;
}

impl<S> CollectGarbage<S> for StateUpdateStream<S>
where
    S: Scope,
{
    fn collect_garbage(
        &self,
        mut recovery_store_summary: RecoveryStoreSummary<S::Timestamp>,
        mut state_collector: Box<dyn StateCollector<S::Timestamp>>,
        dataflow_frontier: ProgressUpdateStream<S>,
    ) -> Stream<S, ()> {
        let mut op_builder = OperatorBuilder::new("CollectGarbage".to_string(), self.scope());

        let mut state_input = op_builder.new_input(self, pact::Pipeline);
        let mut dataflow_frontier_input = op_builder.new_input(&dataflow_frontier, pact::Pipeline);

        let (mut output_wrapper, stream) = op_builder.new_output();

        let mut fncater = FrontierNotificator::new();
        op_builder.build(move |_init_capabilities| {
            let mut tmp_incoming_state = Vec::new();
            let mut tmp_incoming_frontier = Vec::new();

            move |input_frontiers| {
                let mut state_input =
                    FrontieredInputHandle::new(&mut state_input, &input_frontiers[0]);
                let mut dataflow_frontier_input =
                    FrontieredInputHandle::new(&mut dataflow_frontier_input, &input_frontiers[1]);

                let mut output_handle = output_wrapper.activate();

                // Update our internal cache of the state store's
                // keys.
                state_input.for_each(|_cap, incoming| {
                    assert!(tmp_incoming_state.is_empty());
                    incoming.swap(&mut tmp_incoming_state);

                    // Drain items so we don't have to re-allocate.
                    for state_backup in tmp_incoming_state.drain(..) {
                        recovery_store_summary.insert(&state_backup);
                    }
                });

                // Tell the notificator to trigger on dataflow
                // frontier advance.
                dataflow_frontier_input.for_each(|cap, incomfing| {
                    assert!(tmp_incoming_frontier.is_empty());
                    // Drain the dataflow frontier input so the
                    // frontier advances.
                    incomfing.swap(&mut tmp_incoming_frontier);
                    tmp_incoming_frontier.drain(..);

                    fncater.notify_at(cap.retain());
                });

                // Collect garbage.
                fncater.for_each(
                    &[state_input.frontier(), dataflow_frontier_input.frontier()],
                    |cap, _ncater| {
                        // If the dataflow frontier has passed a
                        // notificator-retained epoch, it means it is
                        // fully output and backed up.
                        let finalized_epoch = cap.time();

                        // Now remove all dead items from the state
                        // store and the local cache.
                        for recovery_key in recovery_store_summary.remove_garbage(finalized_epoch) {
                            state_collector.delete(&recovery_key);
                        }

                        // Note progress on the output stream.
                        output_handle.session(&cap);
                    },
                );

                // NOTE: We won't call this GC code when the dataflow
                // frontier closes / input is complete. This makes
                // sense to me: It's not correct to say last_epoch+1
                // has been "finalized" as it never happened. And it
                // supports the use case of "continuing" a completed
                // dataflow by starting back up at that epoch.
            }
        });

        stream
    }
}

// Here's the main recovery operator.

/// If a [`StatefulLogic`] for a key should be retained by
/// [`StatefulUnary::stateful_unary`].
///
/// See [`StatefulLogic::fate`].
pub(crate) enum LogicFate {
    /// This logic for this key should be retained and used again
    /// whenever new data for this key comes in.
    Retain,
    /// The logic for this key is "complete" and should be
    /// discarded. It will be built again if the key is encountered
    /// again.
    Discard,
}

/// Impl this trait to create an operator which maintains recoverable
/// state.
///
/// Pass a builder of this to [`StatefulUnary::stateful_unary`] to
/// create the Timely operator. A separate instance of this will be
/// created for each key in the input stream. There is no way to
/// interact across keys.
pub(crate) trait StatefulLogic<V, R, I>
where
    V: Data,
    I: IntoIterator<Item = R>,
{
    /// Logic to run when this operator is awoken.
    ///
    /// `next_value` has the same semantics as
    /// [`std::async_iter::AsyncIterator::poll_next`]:
    ///
    /// - [`Poll::Pending`]: no new values ready yet. We were probably
    ///   awoken because of a timeout.
    ///
    /// - [`Poll::Ready`] with a [`Some`]: a new value has arrived.
    ///
    /// - [`Poll::Ready`] with a [`None`]: the stream has ended and
    ///   logic will not be called again.
    ///
    /// This must return values to be emitted downstream.
    fn on_awake(&mut self, next_value: Poll<Option<V>>) -> I;

    /// Called when [`StatefulUnary::stateful_unary`] is deciding if
    /// the logic for this key is still relevant.
    ///
    /// Since [`StatefulUnary::stateful_unary`] owns this logic, we
    /// need a way to communicate back up wheither it should be
    /// retained.
    ///
    /// This will be called at the end of each epoch.
    fn fate(&self) -> LogicFate;

    /// Return the next system time this operator should be awoken at.
    fn next_awake(&self) -> Option<DateTime<Utc>>;

    /// Snapshot the internal state of this operator.
    ///
    /// Serialize any and all state necessary to re-construct the
    /// operator exactly how it currently is in the
    /// [`StatefulUnary::stateful_unary`]'s `logic_builder`.
    ///
    /// This will be called at the end of each epoch.
    fn snapshot(&self) -> StateBytes;
}

/// Extension trait for [`Stream`].
// Based on the good work in
// https://github.com/TimelyDataflow/timely-dataflow/blob/0d0d84885672d6369a78cd9aff7beb2048390d3b/timely/src/dataflow/operators/aggregation/state_machine.rs#L57
pub(crate) trait StatefulUnary<S, V>
where
    S: Scope,
    V: ExchangeData,
{
    /// Create a new generic stateful operator.
    ///
    /// This is the core Timely operator that all Bytewax stateful
    /// operators are implemented in terms of. It is awkwardly generic
    /// because of that. We do this so we only have to implement the
    /// very tricky recovery system interop once.
    ///
    /// # Input
    ///
    /// The input must be a stream of `(key, value)` 2-tuples. They
    /// will automatically be routed to the same worker and logic
    /// instance by key.
    ///
    /// # Logic Builder
    ///
    /// This is a closure which should build a new instance of your
    /// logic for a key, given the last snapshot of its state for that
    /// key. You should implement the deserialization from
    /// [`StateBytes`] in this builder; it should be the reverse of
    /// your [`StatefulLogic::snapshot`].
    ///
    /// See [`StatefulLogic`] for the semantics of the logic.
    ///
    /// This will be called periodically as new keys are encountered
    /// and the first time a key is seen during a resume.
    ///
    /// # Output
    ///
    /// The output will be a stream of `(key, value)` 2-tuples. Values
    /// emitted by [`StatefulLogic::awake_with`] will be automatically
    /// paired with the key in the output stream.
    fn stateful_unary<R, I, L, LB>(
        &self,
        step_id: StepId,
        logic_builder: LB,
        key_to_resume_state: HashMap<StateKey, State>,
    ) -> (StatefulStream<S, R>, StateUpdateStream<S>)
    where
        R: Data,                                   // Output value type
        I: IntoIterator<Item = R>,                 // Iterator of output values
        L: StatefulLogic<V, R, I> + 'static,       // Logic
        LB: Fn(Option<StateBytes>) -> L + 'static  // Logic builder
    ;
}

impl<S, V> StatefulUnary<S, V> for StatefulStream<S, V>
where
    S: Scope<Timestamp = u64>,
    V: ExchangeData, // Input value type
{
    fn stateful_unary<R, I, L, LB>(
        &self,
        step_id: StepId,
        logic_builder: LB,
        resume_state: HashMap<StateKey, State>,
    ) -> (StatefulStream<S, R>, StateUpdateStream<S>)
    where
        R: Data,                                   // Output value type
        I: IntoIterator<Item = R>,                 // Iterator of output values
        L: StatefulLogic<V, R, I> + 'static,       // Logic
        LB: Fn(Option<StateBytes>) -> L + 'static, // Logic builder
    {
        let mut op_builder = OperatorBuilder::new(format!("{step_id}"), self.scope());

        let (mut output_wrapper, output_stream) = op_builder.new_output();
        let (mut state_backup_wrapper, state_update_stream) = op_builder.new_output();

        let mut input_handle = op_builder.new_input_connection(
            self,
            pact::Exchange::new(move |&(ref key, ref _value)| StateKey::route(key)),
            // This is saying this input results in items on either
            // output.
            vec![Antichain::from_elem(0), Antichain::from_elem(0)],
            // TODO: Figure out the magic incantation of
            // S::Timestamp::minimum() and S::Timestamp::Summary that
            // lets you do this without the trait bound S:
            // Scope<Timestamp = u64> above.
        );

        let info = op_builder.operator_info();
        let activator = self.scope().activator_for(&info.address[..]);

        op_builder.build(move |mut init_caps| {
            // Logic struct for each key. There is only a single logic
            // for each key representing the state at the frontier
            // epoch; we only modify state carefully in epoch order
            // once we know we won't be getting any input on closed
            // epochs.
            let mut current_state: HashMap<StateKey, L> = HashMap::new();
            // Next awaken timestamp for each key. There is only a
            // single awake time for each key, representing the next
            // awake time.
            let mut current_next_awake: HashMap<StateKey, DateTime<Utc>> = HashMap::new();

            for (
                key,
                State {
                    state_bytes: key_resume_state,
                    next_awake,
                },
            ) in resume_state
            {
                current_state.insert(key.clone(), logic_builder(Some(key_resume_state)));
                match next_awake {
                    Some(next_awake) => {
                        current_next_awake.insert(key.clone(), next_awake);
                    }
                    None => {
                        current_next_awake.remove(&key);
                    }
                }
            }

            // We have to retain separate capabilities
            // per-output. This seems to be only documented in
            // https://github.com/TimelyDataflow/timely-dataflow/pull/187
            // In reverse order because of how [`Vec::pop`] removes
            // from back.
            let mut state_update_cap = init_caps.pop();
            let mut output_cap = init_caps.pop();

            // Here we have "buffers" that store items across
            // activations.

            // Persistent across activations buffer keeping track of
            // out-of-order inputs. Push in here when Timely says we
            // have new data; pull out of here in epoch order to
            // process. This spans activations and will have epochs
            // removed from it as the input frontier progresses.
            let mut incoming_buffer: HashMap<S::Timestamp, Vec<(StateKey, V)>> = HashMap::new();
            // Persistent across activations buffer of what keys were
            // awoken during the most recent epoch. This is used to
            // only snapshot state of keys that could have resulted in
            // state modifications. This is drained after each epoch
            // is processed.
            let mut awoken_keys_buffer: HashSet<StateKey> = HashSet::new();

            // Here are some temporary working sets that we allocate
            // once, then drain and re-use each activation of this
            // operator.

            // Timely requires us to swap incoming data into a buffer
            // we own. This is drained and re-used each activation.
            let mut tmp_incoming: Vec<(StateKey, V)> = Vec::new();
            // Temp ordered set of epochs that can be processed
            // because all their input has been finalized or it's the
            // frontier epoch. This is filled from buffered data and
            // drained and re-used each activation of this operator.
            let mut tmp_closed_epochs: BTreeSet<S::Timestamp> = BTreeSet::new();
            // Temp list of `(StateKey, Poll<Option<V>>)` to awake the
            // operator logic within each epoch. This is drained and
            // re-used each activation of this operator.
            let mut tmp_awake_logic_with: Vec<(StateKey, Poll<Option<V>>)> = Vec::new();

            move |input_frontiers| {
                // Will there be no more data?
                let eof = input_frontiers.iter().all(|f| f.is_empty());
                let is_closed = |e: &S::Timestamp| input_frontiers.iter().all(|f| !f.less_equal(e));

                if let (Some(output_cap), Some(state_update_cap)) =
                    (output_cap.as_mut(), state_update_cap.as_mut())
                {
                    assert!(output_cap.time() == state_update_cap.time());
                    assert!(tmp_closed_epochs.is_empty());
                    assert!(tmp_awake_logic_with.is_empty());

                    let now = chrono::offset::Utc::now();

                    // Buffer the inputs so we can apply them to the
                    // state cache in epoch order.
                    input_handle.for_each(|cap, incoming| {
                        let epoch = cap.time();
                        assert!(tmp_incoming.is_empty());
                        incoming.swap(&mut tmp_incoming);

                        incoming_buffer
                            .entry(*epoch)
                            .or_insert_with(Vec::new)
                            .append(&mut tmp_incoming);
                    });

                    // TODO: Is this the right way to get the epoch
                    // from a frontier? I haven't seen any examples
                    // with non-comparable timestamps to understand
                    // this. Is the current capability reasonable for
                    // when the frontiers are closed?
                    let frontier_epoch = input_frontiers
                        .iter()
                        .flat_map(|mf| mf.frontier().iter().min().cloned())
                        .min()
                        .unwrap_or_else(|| *output_cap.time());

                    // Now let's find out which epochs we should wake
                    // up the logic for.

                    // On the last activation, we eagerly executed the
                    // frontier at that time (which may or may not
                    // still be the frontier), even though it wasn't
                    // closed. Thus, we haven't run the "epoch closed"
                    // code yet. Make sure that close code is run if
                    // that epoch is now closed on this activation.
                    tmp_closed_epochs.extend(Some(output_cap.time().clone()).filter(is_closed));
                    // Try to process all the epochs we have input
                    // for. Filter out epochs that are not closed; the
                    // state at the beginning of those epochs are not
                    // truly known yet, so we can't apply input in
                    // those epochs yet.
                    tmp_closed_epochs.extend(incoming_buffer.keys().cloned().filter(is_closed));
                    // Eagerly execute the current frontier (even
                    // though it's not closed).
                    tmp_closed_epochs.insert(frontier_epoch);

                    // For each epoch in order:
                    for epoch in tmp_closed_epochs.iter() {
                        // Since the frontier has advanced to at least
                        // this epoch (because we're going through
                        // them in order), say that we'll not be
                        // sending output at any older epochs. This
                        // also asserts "apply changes in epoch order"
                        // to the state cache.
                        output_cap.downgrade(&epoch);
                        state_update_cap.downgrade(&epoch);

                        let incoming_state_key_values = incoming_buffer.remove(&epoch);

                        // Now let's find all the key-value pairs to
                        // awaken logic with.

                        // Include all the incoming data.
                        tmp_awake_logic_with.extend(
                            incoming_state_key_values
                                .unwrap_or_default()
                                .into_iter()
                                .map(|(state_key, value)| (state_key, Poll::Ready(Some(value)))),
                        );

                        // Then extend the values with any "awake"
                        // activations after the input.
                        if eof {
                            // If this is the last activation, signal
                            // that all keys have
                            // terminated. Repurpose
                            // [`awoken_keys_buffer`] because it
                            // contains outstanding keys from the last
                            // activation. It's ok that we drain it
                            // because those keys will be re-inserted
                            // due to the EOF items.

                            // First all "new" keys in this input.
                            awoken_keys_buffer.extend(
                                tmp_awake_logic_with
                                    .iter()
                                    .map(|(state_key, _value)| state_key)
                                    .cloned(),
                            );
                            // Then all keys that are still waiting on
                            // awakening. Keys that do not have a
                            // pending awakening will not see EOF
                            // messages (otherwise we'd have to retain
                            // data for all keys ever seen).
                            awoken_keys_buffer.extend(current_next_awake.keys().cloned());
                            // Since this is EOF, we will never
                            // activate this operator again.
                            tmp_awake_logic_with.extend(
                                awoken_keys_buffer
                                    .drain()
                                    .map(|state_key| (state_key, Poll::Ready(None))),
                            );
                        } else {
                            // Otherwise, wake up any keys that are
                            // past their requested awakening time.
                            tmp_awake_logic_with.extend(
                                current_next_awake
                                    .iter()
                                    .filter(|(_state_key, next_awake)| next_awake <= &&now)
                                    .map(|(state_key, _next_awake)| {
                                        (state_key.clone(), Poll::Pending)
                                    }),
                            );
                        }

                        let mut output_handle = output_wrapper.activate();
                        let mut state_update_handle = state_backup_wrapper.activate();
                        let mut output_session = output_handle.session(&output_cap);
                        let mut state_update_session =
                            state_update_handle.session(&state_update_cap);

                        // Drain to re-use allocation.
                        for (key, next_value) in tmp_awake_logic_with.drain(..) {
                            // Remove any scheduled awake times this
                            // current one will satisfy.
                            if let Entry::Occupied(next_awake_at_entry) =
                                current_next_awake.entry(key.clone())
                            {
                                if next_awake_at_entry.get() <= &now {
                                    next_awake_at_entry.remove();
                                }
                            }

                            let logic = current_state
                                .entry(key.clone())
                                .or_insert_with(|| logic_builder(None));
                            let output = logic.on_awake(next_value);
                            output_session
                                .give_iterator(output.into_iter().map(|item| (key.clone(), item)));

                            awoken_keys_buffer.insert(key);
                        }

                        // Determine the fate of each key's logic at
                        // the end of each epoch. If a key wasn't
                        // awoken, then there's no state change so
                        // ignore it here. Snapshot and output state
                        // changes. Remove will ensure we slowly drain
                        // the buffer.
                        if is_closed(&epoch) {
                            for state_key in awoken_keys_buffer.drain() {
                                let logic = current_state
                                    .remove(&state_key)
                                    .expect("No logic for activated key");

                                let op = match logic.fate() {
                                    LogicFate::Discard => {
                                        // Do not re-insert the
                                        // logic. It'll be dropped.
                                        current_next_awake.remove(&state_key);

                                        StateOp::Discard
                                    }
                                    LogicFate::Retain => {
                                        let state_bytes = logic.snapshot();
                                        let next_awake = logic.next_awake();

                                        current_state.insert(state_key.clone(), logic);
                                        match next_awake {
                                            Some(next_awake) => {
                                                current_next_awake
                                                    .insert(state_key.clone(), next_awake);
                                            }
                                            None => {
                                                current_next_awake.remove(&state_key);
                                            }
                                        }

                                        StateOp::Upsert(State {
                                            state_bytes,
                                            next_awake,
                                        })
                                    }
                                };

                                let recovery_key = StateRecoveryKey {
                                    step_id: step_id.clone(),
                                    state_key,
                                    epoch: epoch.clone(),
                                };
                                let update = StateUpdate(recovery_key, op);
                                state_update_session.give(update);
                            }
                        }
                    }
                    // Clear to re-use buffer.
                    // TODO: One day I hope BTreeSet has drain.
                    tmp_closed_epochs.clear();

                    // Schedule operator activation at the soonest
                    // requested logic awake time for any key.
                    if let Some(soonest_next_awake) = current_next_awake
                        .values()
                        .map(|next_awake| next_awake.clone() - now)
                        .min()
                    {
                        activator
                            .activate_after(soonest_next_awake.to_std().unwrap_or(Duration::ZERO));
                    }
                }

                if eof {
                    output_cap = None;
                    state_update_cap = None;
                }
            }
        });

        (output_stream, state_update_stream)
    }
}

// Here's our recovery core trait implementers and utility functions.

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
) -> StringResult<(
    Box<dyn ProgressWriter<u64>>,
    Box<dyn StateWriter<u64>>,
    Box<dyn StateCollector<u64>>,
)> {
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

        let hosts = &kafka_recovery_config.brokers;
        let state_topic = kafka_recovery_config.state_topic();
        let progress_topic = kafka_recovery_config.progress_topic();
        let partition = worker_index.try_into().unwrap();
        let create_partitions = worker_count.try_into().unwrap();

        let (progress_writer, state_writer, state_collector): (
            KafkaWriter<ProgressRecoveryKey, ProgressOp<u64>>,
            KafkaWriter<StateRecoveryKey<u64>, StateOp>,
            KafkaWriter<StateRecoveryKey<u64>, StateOp>,
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
) -> StringResult<(Box<dyn ProgressReader<u64>>, Box<dyn StateReader<u64>>)> {
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

        let brokers = &kafka_recovery_config.brokers;
        let state_topic = kafka_recovery_config.state_topic();
        let progress_topic = kafka_recovery_config.progress_topic();
        let partition = worker_index.try_into().unwrap();
        let create_partitions = worker_count.try_into().unwrap();

        let (progress_reader, state_reader) = py.allow_threads(|| {
            create_kafka_topic(brokers, &progress_topic, create_partitions);
            create_kafka_topic(brokers, &state_topic, create_partitions);

            (
                KafkaReader::new(brokers, &progress_topic, partition),
                KafkaReader::new(brokers, &state_topic, partition),
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

impl<T> StateWriter<T> for NoopRecovery
where
    T: Debug,
{
    fn write(&mut self, update: &StateUpdate<T>) {
        trace!("Noop wrote state update {update:?}");
    }
}

impl<T> StateCollector<T> for NoopRecovery
where
    T: Debug,
{
    fn delete(&mut self, key: &StateRecoveryKey<T>) {
        trace!("Noop deleted state for {key:?}");
    }
}

impl<T> StateReader<T> for NoopRecovery {
    fn read(&mut self) -> Option<StateUpdate<T>> {
        None
    }
}

impl<T> ProgressWriter<T> for NoopRecovery
where
    T: Debug,
{
    fn write(&mut self, update: &ProgressUpdate<T>) {
        trace!("Noop wrote progress update {update:?}");
    }
}

impl<T> ProgressReader<T> for NoopRecovery {
    fn read(&mut self) -> Option<ProgressUpdate<T>> {
        None
    }
}

// The recovery store summary.

/// The [`RecoveryStoreSummary`] doesn't need to retain full copies of
/// state to determine what is garbage (just that there was a reset or
/// an update), so have a little enum here to represent that.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
enum OpType {
    Upsert,
    Discard,
}

impl From<&StateOp> for OpType {
    fn from(op: &StateOp) -> Self {
        match op {
            StateOp::Upsert { .. } => Self::Upsert,
            StateOp::Discard => Self::Discard,
        }
    }
}

/// In-memory summary of all keys this worker's recovery store knows
/// about.
///
/// This is used to quickly find garbage state without needing to
/// query the recovery store itself.
pub(crate) struct RecoveryStoreSummary<T> {
    db: HashMap<(StepId, StateKey), HashMap<T, OpType>>,
}

impl<T> RecoveryStoreSummary<T>
where
    T: Timestamp,
{
    pub(crate) fn new() -> Self {
        Self { db: HashMap::new() }
    }

    /// Mark that state for this step ID, key, and epoch was backed
    /// up.
    pub(crate) fn insert(&mut self, update: &StateUpdate<T>) {
        let StateUpdate(recovery_key, op) = update;
        let StateRecoveryKey {
            step_id,
            state_key,
            epoch,
        } = recovery_key;
        let op_type = op.into();
        self.db
            .entry((step_id.clone(), state_key.clone()))
            .or_default()
            .insert(epoch.clone(), op_type);
    }

    /// Find and remove all garbage given a finalized epoch.
    ///
    /// Garbage is any state data before or during a finalized epoch,
    /// other than the last upsert for a key (since that's still
    /// relevant since it hasn't been overwritten yet).
    pub(crate) fn remove_garbage(&mut self, finalized_epoch: &T) -> Vec<StateRecoveryKey<T>> {
        let mut garbage = Vec::new();

        let mut empty_map_keys = Vec::new();
        for (map_key, epoch_ops) in self.db.iter_mut() {
            let (step_id, state_key) = map_key;

            // TODO: The following becomes way cleaner once
            // [`std::collections::BTreeMap::drain_filter`] and
            // [`std::collections::BTreeMap::first_entry`] hits
            // stable.

            let (mut map_key_garbage, mut map_key_non_garbage): (Vec<_>, Vec<_>) = epoch_ops
                .drain()
                .partition(|(epoch, _update_type)| epoch <= finalized_epoch);
            map_key_garbage.sort();

            // If the final bit of "garbage" is an upsert, keep it,
            // since it's the state we'd use to recover.
            if let Some(epoch_op) = map_key_garbage.pop() {
                let (_epoch, op_type) = &epoch_op;
                if op_type == &OpType::Upsert {
                    map_key_non_garbage.push(epoch_op);
                } else {
                    map_key_garbage.push(epoch_op);
                }
            }

            for (epoch, _op_type) in map_key_garbage {
                garbage.push(StateRecoveryKey {
                    step_id: step_id.clone(),
                    state_key: state_key.clone(),
                    epoch,
                });
            }

            // Non-garbage should remain in the in-mem DB.
            *epoch_ops = map_key_non_garbage.into_iter().collect::<HashMap<_, _>>();

            if epoch_ops.is_empty() {
                empty_map_keys.push(map_key.clone());
            }
        }

        // Clean up any keys that aren't seen again.
        for map_key in empty_map_keys {
            self.db.remove(&map_key);
        }

        garbage
    }
}

pub(crate) fn register(_py: Python, m: &PyModule) -> PyResult<()> {
    m.add_class::<RecoveryConfig>()?;
    m.add_class::<SqliteRecoveryConfig>()?;
    m.add_class::<KafkaRecoveryConfig>()?;
    Ok(())
}
