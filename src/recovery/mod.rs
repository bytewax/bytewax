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
//! Backup Architecture
//! -------------------
//!
//! Recovery is based around snapshotting the state of each stateful
//! operator in worker at the end of each epoch and recording that
//! worker's progress information. Each worker's recovery data is
//! stored separately so that we can discern the exact state of each
//! worker at the point of failure.
//!
//! The **worker frontier** represents the oldest epoch on a given
//! worker for which items are still in the process of being output or
//! state updates are being backed up.
//!
//! We model the recovery store for each worker as a key-value
//! database with two "tables" for each worker: a **state table** and
//! a **progress table**. The state table backs up state snapshots,
//! while the progress table backs up changes to the worker frontier.
//!
//! The data model for state table is represented in the structs
//! [`RecoveryKey`] and [`StateBackup`] for the key and value
//! respectively.
//!
//! The recovery machinery works only on [`StateBytes`], not on actual
//! live objects. We do this so we can interleave multiple concrete
//! state types from different stateful operators together through the
//! same parts of the recovery dataflow.
//!
//! Note that backing up the fact that state was deleted
//! ([`StateUpdate::Reset`]) is not the same as GCing the state. We
//! need to explicitly save the history of all deletions in case we
//! need to recover right after a deletion; that state value should
//! not be recovered. Separately, once we know some backup state is no
//! longer needed and we'll never need to recover there do we actually
//! delete the state from the recovery store.
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
//! The (private) function `build_production_dataflow` in [`crate::execution`]
//! builds the parts of the dataflow for backup.
//! Look there for what is described below.
//!
//! We currently have a few user-facing stateful operators
//! (e.g. [`crate::dataflow::Dataflow::reduce`],
//! [`crate::dataflow::Dataflow::stateful_map`]). But they are both implemented
//! on top of a general underlying one:
//! [`crate::recovery::StatefulUnary`]. This means all in-operator
//! recovery-related code is only written once.
//!
//! Stateful unary does not backup itself. Instead, each stateful
//! operator generates a second **state backup stream** output. These
//! are then connected to the rest of the recovery components, after
//! serializing / deserializing the state so that the recovery streams
//! are all backups of bytes.
//!
//! All state backups from all stateful operators are concatenated
//! into the [`WriteState`] operator, which actually
//! performs the writes via the [`StateWriter`]. It emits the backups
//! after writing downstream so progress can be monitored.
//!
//! The [`WriteProgress`] operator then looks at the
//! **worker frontier**, the combined stream of written backups and
//! all captures. This will be written via the [`ProgressWriter`]. It
//! emits heartbeats.
//!
//! These worker frontier heartbeats are then broadcast so operators
//! listening to this stream will see progress of the entire dataflow
//! cluster, the **dataflow frontier**.
//!
//! The dataflow frontier heartbeat stream and completed state backups
//! are then fed into the [`crate::recovery::CollectGarbage`]
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
//! The (private) function `worker_main` in [`crate::execution`]
//! is where loading starts.
//! It's broken into two dataflows, built in
//! [`build_resume_epoch_calc_dataflow`],
//! [`build_state_loading_dataflow`].
//!
//! First, the resume epoch must be calculated from the progress data
//! actually written to the recovery store. This can't be
//! pre-calculated during backup because that write might fail.
//!
//! This is done in a first separate dataflow because it needs a unique epoch
//! definition: we need to know when we're done reading all recovery
//! data, which would be impossible if we re-used the epoch definition
//! from the backed up dataflow (because that would mean it
//! completed).
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
//! Once we have the resume epoch, we know what previously backed up
//! data is relevant (and not too new) and can start loading that onto
//! each worker.
//!
//! A second separate dataflow does this loading. Each resume worker
//! is assigned to read all state _before_ the resume epoch. This
//! state is loaded into maps by [`StepId`] and [`StateKey`] so that
//! the production dataflow's operators can deserialize the relevant
//! state when running. This state data also is used to produce a
//! [`RecoveryStoreSummary`] so that the [`CollectGarbage`] operator
//! has a correct cache of all previously-written state keys.
//!
//! Once the state loading is complete, the resulting **resume state**
//! is handed off to the production dataflow. It is routed to the
//! correct stateful operators by [`StepId`], then kept around until a
//! key is encountered and deserialized to build the relevant logic.
//!
//! If the underlying data or bug has been fixed, then things should
//! resume with the state from the end of the epoch just before
//! failure, with the input resuming from beginning of the next epoch.

use log::debug;
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
use std::time::Instant;
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
use timely::dataflow::Stream;
use timely::order::TotalOrder;
use timely::progress::Antichain;
use timely::progress::Timestamp;
use timely::worker::Worker;
use timely::Data;
use timely::ExchangeData;

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

/// Common data type used across the codebase
pub(crate) type StreamStateKey<T> = (StateKey, T);
pub(crate) type EpochData = StreamStateKey<(StepId, StateUpdate)>;

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
    Worker(usize),
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
            // My read of
            // https://github.com/TimelyDataflow/timely-dataflow/blob/v0.12.0/timely/src/dataflow/channels/pushers/exchange.rs#L61-L90
            // says that if you return the worker index, it'll be
            // routed to that worker.
            Self::Worker(index) => *index as u64,
        }
    }
}

impl<'source> FromPyObject<'source> for StateKey {
    fn extract(ob: &'source PyAny) -> PyResult<Self> {
        if let Ok(py_string) = ob.cast_as::<PyString>() {
            Ok(Self::Hash(py_string.to_str()?.into()))
        } else if let Ok(py_int) = ob.cast_as::<PyLong>() {
            Ok(Self::Worker(py_int.extract()?))
        } else {
            Err(PyTypeError::new_err("Can only make StateKey out of either str (route to worker by hash) or int (route to worker by index)"))
        }
    }
}

impl IntoPy<PyObject> for StateKey {
    fn into_py(self, py: Python) -> Py<PyAny> {
        match self {
            Self::Hash(key) => key.into_py(py),
            Self::Worker(index) => index.into_py(py),
        }
    }
}

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
                .unwrap_or_else(|_| panic!("Error serializing recovery {t_name})")),
        )
    }

    /// Deserialize these bytes from the recovery system into a state
    /// object that an operator can use.
    pub(crate) fn de<T: DeserializeOwned>(self) -> T {
        let t_name = type_name::<T>();
        bincode::deserialize(&self.0)
            .unwrap_or_else(|_| panic!("Error deserializing recovery state {t_name})"))
    }
}

/// The two kinds of actions that logic in a stateful operator can do
/// to each [`StateKey`].
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) enum StateUpdate {
    /// Save an updated state snapshot.
    Upsert(StateBytes),
    /// Note that the state for this key was reset to empty.
    Reset,
}

impl<T: Serialize> From<Option<T>> for StateUpdate {
    fn from(updated_state: Option<T>) -> Self {
        match updated_state {
            Some(state) => Self::Upsert(StateBytes::ser(&state)),
            None => Self::Reset,
        }
    }
}

/// Impl this trait to create an operator which maintains recoverable
/// state.
///
/// Pass a builder of this to [`StatefulUnary::stateful_unary`] to
/// create the Timely operator. A separate instance of this will be
/// created for each key in the input stream. There is no way to
/// interact across keys.
pub(crate) trait StatefulLogic<V, R, I: IntoIterator<Item = R>> {
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
    /// This must return a 2-tuple of:
    ///
    /// - Values to be emitted downstream.
    ///
    /// - Timeout delay to be awoken after with [`Poll::Pending`] as
    /// the value. It's possible the logic will be awoken for this key
    /// earlier if new data comes in. Timeouts are not buffered across
    /// calls to logic, so you should always specify the delay to the
    /// next time you should be woken up every time.
    fn exec(&mut self, next_value: Poll<Option<V>>) -> (I, Option<Duration>);

    /// Snapshot the internal state of this operator.
    ///
    /// Serialize any and all state necessary to re-construct the
    /// operator exactly how it currently is in the
    /// [`StatefulUnary::stateful_unary`]'s `logic_builder`.
    ///
    /// Return [`StateUpdate::Reset`] whenever this logic for this key
    /// is "complete" and should be discarded. It will be built again
    /// if the key is encountered again.
    ///
    /// This will be called at the end of each epoch.
    fn snapshot(&self) -> StateUpdate;
}

// Here's our recovery data model.

/// A message noting that the frontier at an input changed.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct FrontierBackup<T>(
    /// Worker index.
    pub(crate) usize,
    /// Worker frontier.
    pub(crate) Antichain<T>,
);

/// A message noting the state for a key in a stateful operator
/// changed.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct StateBackup<T>(pub(crate) RecoveryKey<T>, pub(crate) StateUpdate);

impl<T: Timestamp> StateBackup<T> {
    /// Route in Timely just by the state key to ensure that all
    /// relevant data ends up on the correct worker.
    fn pact() -> impl ParallelizationContract<T, StateBackup<T>> {
        pact::Exchange::new(|StateBackup(RecoveryKey(_step_id, key, _epoch), _update)| key.route())
    }
}

/// Key used to address data within a recovery store.
///
/// Remember, this isn't the same as [`StateKey`], as the "address
/// space" of that key is just within a single operator. This type
/// includes the operator name and epoch too, so we can address state
/// in an entire dataflow and across time.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct RecoveryKey<T>(pub(crate) StepId, pub(crate) StateKey, pub(crate) T);

// Here's our core traits for recovery that allow us to swap out
// underlying storage.

pub(crate) trait ProgressWriter<T> {
    fn write(&mut self, frontier_backup: &FrontierBackup<T>);
}

pub(crate) trait ProgressReader<T> {
    /// Has the same semantics as [`std::iter::Iterator::next`]:
    /// return [`None`] to signal EOF.
    fn read(&mut self) -> Option<FrontierBackup<T>>;
}

pub(crate) trait StateWriter<T> {
    fn write(&mut self, state_backup: &StateBackup<T>);
}

pub(crate) trait StateCollector<T> {
    fn delete(&mut self, recovery_key: &RecoveryKey<T>);
}

pub(crate) trait StateReader<T> {
    /// Has the same semantics as [`std::iter::Iterator::next`]:
    /// return [`None`] to signal EOF.
    fn read(&mut self) -> Option<StateBackup<T>>;
}

// Here are our loading dataflows.

/// Compile a dataflow which reads the progress data from the previous
/// execution and calculates the resume epoch.
///
/// Once the progress input is done, this dataflow will send the
/// resume epoch through a channel. The main function should consume
/// from that channel to pass on to the other loading and production
/// dataflows.
pub(crate) fn build_resume_epoch_calc_dataflow<A: Allocate, T: Timestamp>(
    timely_worker: &mut Worker<A>,
    // TODO: Allow multiple (or none) FrontierReaders so you can recover a
    // different-sized cluster.
    progress_reader: Box<dyn ProgressReader<T>>,
    resume_epoch_tx: std::sync::mpsc::Sender<T>,
) -> StringResult<ProbeHandle<()>> {
    timely_worker.dataflow(|scope| {
        let mut probe = ProbeHandle::new();

        progress_source(scope, progress_reader, &probe)
            .map(|FrontierBackup(worker_index, antichain)| (worker_index, antichain))
            .aggregate(
                |_worker_index, antichain, worker_frontier_acc: &mut Option<Antichain<T>>| {
                    let worker_frontier =
                    // A frontier of [0], not the empty frontier, is
                    // the "earliest". (Empty is "complete" or "last",
                    // which is used below).
                        worker_frontier_acc.get_or_insert(Antichain::from_elem(T::minimum()));
                    // For each worker in the failed cluster, find the
                    // latest frontier.
                    if timely::PartialOrder::less_than(worker_frontier, &antichain) {
                        *worker_frontier = antichain;
                    }
                },
                |_worker_index, worker_frontier_acc| {
                    // Drop worker index because next step looks at
                    // all workers.
                    worker_frontier_acc.expect("Did not update worker_frontier_acc in aggregation")
                },
                |worker_index| *worker_index as u64,
            )
            // Each worker in the recovery cluster reads only some of
            // the frontier data of workers in the failed cluster.
            .broadcast()
            .accumulate(Antichain::new(), |dataflow_frontier, worker_frontiers| {
                for worker_frontier in worker_frontiers.iter() {
                    // The slowest of the workers in the failed
                    // cluster is the resume epoch.
                    if timely::PartialOrder::less_than(worker_frontier, dataflow_frontier) {
                        *dataflow_frontier = worker_frontier.clone();
                    }
                }
            })
            .map(|dataflow_frontier| {
                // TODO: Is this the right way to transform a frontier
                // back into a recovery epoch?
                dataflow_frontier
                    .elements()
                    .iter()
                    .cloned()
                    .min()
                    .unwrap_or_else(T::minimum)
            })
            .map(move |resume_epoch| resume_epoch_tx.send(resume_epoch).unwrap())
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
pub(crate) fn build_state_loading_dataflow<A: Allocate>(
    timely_worker: &mut Worker<A>,
    state_reader: Box<dyn StateReader<u64>>,
    resume_epoch: u64,
    step_to_key_to_resume_state_bytes_tx: std::sync::mpsc::Sender<(
        StepId,
        HashMap<StateKey, StateBytes>,
    )>,
    recovery_store_summary_tx: std::sync::mpsc::Sender<RecoveryStoreSummary<u64>>,
) -> StringResult<ProbeHandle<u64>> {
    timely_worker.dataflow(|scope| {
        let mut probe = ProbeHandle::new();

        let source = state_source(scope, state_reader, resume_epoch, &probe);

        source
            .unary_frontier(
                StateBackup::pact(),
                "RecoveryStoreSummaryCalc",
                |_init_cap, _info| {
                    let mut fncater = FrontierNotificator::new();

                    let mut tmp_backups = Vec::new();
                    let mut epoch_to_backups_buffer = HashMap::new();
                    let mut recovery_store_summary = Some(RecoveryStoreSummary::new());

                    move |input, output| {
                        input.for_each(|cap, backups| {
                            let epoch = cap.time();
                            backups.swap(&mut tmp_backups);

                            epoch_to_backups_buffer

                                .entry(*epoch)
                                .or_insert_with(Vec::new)
                                .append(&mut tmp_backups);

                            fncater.notify_at(cap.retain());
                        });

                        fncater.for_each(&[input.frontier()], |cap, _ncater| {
                            let epoch = cap.time();
                            if let Some(backups) = epoch_to_backups_buffer.remove(epoch) {
                                let recovery_store_summary = recovery_store_summary
                                    .as_mut()
                                    .expect(
                                    "More input after recovery store calc input frontier was empty",
                                );
                                for backup in backups {
                                    recovery_store_summary.insert(backup);
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
            .unary_frontier(StateBackup::pact(), "StateCacheCalc", |_init_cap, _info| {
                let mut fncater = FrontierNotificator::new();

                let mut tmp_backups = Vec::new();
                let mut epoch_to_backups_buffer = HashMap::new();
                let mut step_to_key_to_resume_state_bytes: Option<HashMap<StepId, HashMap<StateKey, StateBytes>>> =
                    Some(HashMap::new());

                move |input, output| {
                    input.for_each(|cap, backups| {
                        let epoch = cap.time();
                        backups.swap(&mut tmp_backups);

                        epoch_to_backups_buffer
                            .entry(*epoch)
                            .or_insert_with(Vec::new)
                            .append(&mut tmp_backups);

                        fncater.notify_at(cap.retain());
                    });

                    fncater.for_each(&[input.frontier()], |cap, _ncater| {
                        let epoch = cap.time();
                        if let Some(backups) = epoch_to_backups_buffer.remove(epoch) {
                            for StateBackup(RecoveryKey(step_id, key, _epoch), update) in backups {
                                let resume_state =
                                    step_to_key_to_resume_state_bytes
                                    .as_mut()
                                    .expect("More input after resume state calc input frontier was empty")
                                    .entry(step_id)
                                    .or_default();

                                match update {
                                    StateUpdate::Upsert(state) => resume_state.insert(key, state),
                                    StateUpdate::Reset => resume_state.remove(&key),
                                };
                            }
                        }

                        // Emit heartbeats so we can monitor progress
                        // at the probe.
                        output.session(&cap).give(());
                    });

                    if input.frontier().is_empty() {
                        if let Some(step_to_key_to_resume_state_bytes) = step_to_key_to_resume_state_bytes.take() {
                            for step_key_resume_state_bytes in step_to_key_to_resume_state_bytes {
                                step_to_key_to_resume_state_bytes_tx.send(step_key_resume_state_bytes).unwrap();
                            }
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
/// The resulting stream only has the zeroth epoch.
///
/// Note that this pretty meta! This new _loading_ dataflow will only
/// have the zeroth epoch, but you can observe what progress was made
/// on the _previous_ dataflow.
pub(crate) fn progress_source<S: Scope, T: Data>(
    scope: &S,
    mut reader: Box<dyn ProgressReader<T>>,
    probe: &ProbeHandle<S::Timestamp>,
) -> Stream<S, FrontierBackup<T>>
where
    S::Timestamp: TotalOrder,
{
    iterator_source(
        scope,
        "ProgressSource",
        move |last_cap| {
            reader.read().map(|backup| IteratorSourceInput {
                lower_bound: S::Timestamp::minimum(),
                // An iterator of (timestamp, iterator of
                // items). Nested [`IntoIterator`]s.
                data: Some((S::Timestamp::minimum(), Some(backup))),
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
pub(crate) fn state_source<S: Scope>(
    scope: &S,
    mut reader: Box<dyn StateReader<S::Timestamp>>,
    stop_at: S::Timestamp,
    probe: &ProbeHandle<S::Timestamp>,
) -> Stream<S, StateBackup<S::Timestamp>>
where
    S::Timestamp: TotalOrder,
{
    iterator_source(
        scope,
        "StateSource",
        move |last_cap| match reader.read() {
            Some(backup) => {
                let StateBackup(RecoveryKey(_step_id, _key, epoch), _update) = &backup;
                let epoch = epoch.clone();

                if epoch < stop_at {
                    Some(IteratorSourceInput {
                        lower_bound: S::Timestamp::minimum(),
                        // An iterator of (timestamp, iterator of
                        // items). Nested [`IntoIterators`].
                        data: Some((epoch, Some(backup))),
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
pub(crate) trait WriteState<S: Scope> {
    /// Writes state backups in timestamp order.
    fn write_state_with(
        &self,
        state_writer: Box<dyn StateWriter<S::Timestamp>>,
    ) -> Stream<S, StateBackup<S::Timestamp>>;
}

impl<S: Scope> WriteState<S> for Stream<S, (StateKey, (StepId, StateUpdate))> {
    fn write_state_with(
        &self,
        mut state_writer: Box<dyn StateWriter<S::Timestamp>>,
    ) -> Stream<S, StateBackup<S::Timestamp>> {
        self.unary_notify(pact::Pipeline, "WriteState", None, {
            // TODO: Store worker_index in the backup so we know if we
            // crossed the worker backup streams?

            // let worker_index = self.scope().index();

            let mut tmp_backups = Vec::new();
            let mut epoch_to_backups_buffer = HashMap::new();

            move |input, output, ncater| {
                input.for_each(|cap, backups| {
                    let epoch = cap.time();
                    backups.swap(&mut tmp_backups);

                    epoch_to_backups_buffer
                        .entry(epoch.clone())
                        .or_insert_with(Vec::new)
                        .append(&mut tmp_backups);

                    ncater.notify_at(cap.retain());
                });

                // Use the notificator to ensure state is written in
                // epoch order.
                ncater.for_each(|cap, _count, _ncater| {
                    let epoch = cap.time();
                    if let Some(updates) = epoch_to_backups_buffer.remove(epoch) {
                        for (key, (step_id, update)) in updates {
                            let backup =
                                StateBackup(RecoveryKey(step_id, key, epoch.clone()), update);

                            state_writer.write(&backup);

                            output.session(&cap).give(backup);
                        }
                    }
                });
            }
        })
    }
}

/// Extension trait for [`Stream`].
pub(crate) trait WriteProgress<S: Scope, D: Data> {
    /// Write out the current frontier of the output this is connected
    /// to whenever it changes.
    fn write_progress_with(
        &self,
        frontier_writer: Box<dyn ProgressWriter<S::Timestamp>>,
    ) -> Stream<S, FrontierBackup<S::Timestamp>>;
}

impl<S: Scope, D: Data> WriteProgress<S, D> for Stream<S, D> {
    fn write_progress_with(
        &self,
        mut frontier_writer: Box<dyn ProgressWriter<S::Timestamp>>,
    ) -> Stream<S, FrontierBackup<S::Timestamp>> {
        self.unary_notify(pact::Pipeline, "WriteProgress", None, {
            let worker_index = self.scope().index();

            let mut tmp_data = Vec::new();

            move |input, output, ncater| {
                input.for_each(|cap, data| {
                    data.swap(&mut tmp_data);

                    ncater.notify_at(cap.retain());
                });

                ncater.for_each(|cap, _count, ncater| {
                    // 0 is our singular input.
                    let frontier = ncater.frontier(0).to_owned();

                    // Don't write out the last "empty" frontier to
                    // allow restarting from the end of the dataflow.
                    if !frontier.elements().is_empty() {
                        let backup = FrontierBackup(worker_index, frontier);

                        frontier_writer.write(&backup);

                        output.session(&cap).give(backup);
                    }
                });
            }
        })
    }
}

/// Extension trait for [`Stream`].
pub(crate) trait CollectGarbage<S: Scope> {
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
        dataflow_frontier: Stream<S, FrontierBackup<S::Timestamp>>,
    ) -> Stream<S, ()>;
}

impl<S: Scope> CollectGarbage<S> for Stream<S, StateBackup<S::Timestamp>> {
    fn collect_garbage(
        &self,
        mut recovery_store_summary: RecoveryStoreSummary<S::Timestamp>,
        mut state_collector: Box<dyn StateCollector<S::Timestamp>>,
        dataflow_frontier: Stream<S, FrontierBackup<S::Timestamp>>,
    ) -> Stream<S, ()> {
        let mut op_builder = OperatorBuilder::new("CollectGarbage".to_string(), self.scope());

        let mut state_input = op_builder.new_input(self, pact::Pipeline);
        let mut dataflow_frontier_input = op_builder.new_input(&dataflow_frontier, pact::Pipeline);

        let (mut output_wrapper, stream) = op_builder.new_output();

        let mut fncater = FrontierNotificator::new();
        op_builder.build(move |_init_capabilities| {
            let mut tmp_state_backups = Vec::new();
            let mut tmp_frontier_backups = Vec::new();

            move |input_frontiers| {
                let mut state_input =
                    FrontieredInputHandle::new(&mut state_input, &input_frontiers[0]);
                let mut dataflow_frontier_input =
                    FrontieredInputHandle::new(&mut dataflow_frontier_input, &input_frontiers[1]);

                let mut output_handle = output_wrapper.activate();

                // Update our internal cache of the state store's
                // keys.
                state_input.for_each(|_cap, state_backups| {
                    state_backups.swap(&mut tmp_state_backups);

                    // Drain items so we don't have to re-allocate.
                    for state_backup in tmp_state_backups.drain(..) {
                        recovery_store_summary.insert(state_backup);
                    }
                });

                // Tell the notificator to trigger on dataflow
                // frontier advance.
                dataflow_frontier_input.for_each(|cap, data| {
                    // Drain the dataflow frontier input so the
                    // frontier advances.
                    data.swap(&mut tmp_frontier_backups);
                    tmp_frontier_backups.drain(..);

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

/// Extension trait for [`Stream`].
// Based on the good work in
// https://github.com/TimelyDataflow/timely-dataflow/blob/0d0d84885672d6369a78cd9aff7beb2048390d3b/timely/src/dataflow/operators/aggregation/state_machine.rs#L57
pub(crate) trait StatefulUnary<S: Scope, V: ExchangeData> {
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
    /// emitted by [`StatefulLogic::exec`] will be automatically
    /// paired with the key in the output stream.
    fn stateful_unary<
        R: Data,                                   // Output item type
        I: IntoIterator<Item = R>,                 // Iterator of output items
        L: StatefulLogic<V, R, I> + 'static,       // Logic
        LB: Fn(Option<StateBytes>) -> L + 'static, // Logic builder
    >(
        &self,
        step_id: StepId,
        logic_builder: LB,
        resume_state: HashMap<StateKey, StateBytes>,
    ) -> (Stream<S, (StateKey, R)>, Stream<S, EpochData>)
    where
        S::Timestamp: Hash + Eq;
}

impl<S: Scope, V: ExchangeData> StatefulUnary<S, V> for Stream<S, (StateKey, V)>
where
    S: Scope<Timestamp = u64>,
{
    fn stateful_unary<
        R: Data,                                   // Output item type
        I: IntoIterator<Item = R>,                 // Iterator of output items
        L: StatefulLogic<V, R, I> + 'static,       // Logic
        LB: Fn(Option<StateBytes>) -> L + 'static, // Logic builder
    >(
        &self,
        step_id: StepId,
        logic_builder: LB,
        mut key_to_resume_state_bytes: HashMap<StateKey, StateBytes>,
    ) -> (
        Stream<S, (StateKey, R)>,
        Stream<S, (StateKey, (StepId, StateUpdate))>,
    ) {
        let mut op_builder = OperatorBuilder::new(format!("{step_id}"), self.scope());

        let (mut output_wrapper, output_stream) = op_builder.new_output();
        let (mut state_update_wrapper, state_update_stream) = op_builder.new_output();

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
            let mut logic_cache = HashMap::new();

            // We have to retain separate capabilities
            // per-output. This seems to be only documented in
            // https://github.com/TimelyDataflow/timely-dataflow/pull/187
            // In reverse order because of how [`Vec::pop`] removes
            // from back.
            let mut state_update_cap = init_caps.pop();
            let mut output_cap = init_caps.pop();

            // Timely requires us to swap incoming data into a buffer
            // we own. This is drained and re-used each activation.
            let mut tmp_key_values = Vec::new();
            // Persistent across activations buffer keeping track of
            // out-of-order inputs. Push in here when Timely says we
            // have new data; pull out of here in epoch order to
            // process. This spans activations and will have epochs
            // removed from it as the input frontier progresses.
            let mut incoming_epoch_to_key_values_buffer: HashMap<S::Timestamp, Vec<(StateKey, V)>> =
                HashMap::new();

            // Temp ordered set of epochs that need processing. This
            // is filled from buffered data and drained and re-used
            // each activation of this operator.
            let mut activate_epochs_buffer: BTreeSet<S::Timestamp> = BTreeSet::new();
            // Temp list of `(StateKey, Poll<Option<V>>)` to pass to
            // the operator logic within each epoch. This is drained
            // and re-used.
            let mut key_values_buffer: Vec<(StateKey, Poll<Option<V>>)> = Vec::new();
            // Temp set of all the keys we know about when we've
            // reached EOF to signal that. This is drained and
            // re-used.
            let mut keys_buffer = HashSet::new();

            // Persistent across activations buffer of when each key
            // needs to be awoken next. As activations occur due to
            // data or timeout, we'll remove pending activations here.
            let mut key_to_next_activate_at_buffer: HashMap<StateKey, Instant> = HashMap::new();
            // Persistent across activations buffer of what keys were
            // activated during each epoch. This is used to only
            // snapshot state of keys that could have resulted in
            // state modifications. Epochs will be removed from this
            // as the frontier progresses.
            let mut activated_epoch_to_keys_buffer: HashMap<S::Timestamp, HashSet<StateKey>> =
                HashMap::new();

            move |input_frontiers| {
                // Will there be no more data?
                let eof = input_frontiers.iter().all(|f| f.is_empty());
                let is_closed = |e: &S::Timestamp| input_frontiers.iter().all(|f| !f.less_equal(e));

                if let (Some(output_cap), Some(state_update_cap)) =
                    (output_cap.as_mut(), state_update_cap.as_mut())
                {
                    assert!(output_cap.time() == state_update_cap.time());
                    let mut output_handle = output_wrapper.activate();
                    let mut state_update_handle = state_update_wrapper.activate();

                    // Buffer the inputs so we can apply them to the
                    // state cache in epoch order.
                    input_handle.for_each(|cap, key_values| {
                        let epoch = cap.time();
                        assert!(tmp_key_values.is_empty());
                        key_values.swap(&mut tmp_key_values);

                        incoming_epoch_to_key_values_buffer
                            .entry(*epoch)
                            .or_insert_with(Vec::new)
                            .append(&mut tmp_key_values);
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
                    assert!(activate_epochs_buffer.is_empty());

                    // Try to process all the epochs we have data for.
                    // Filter out epochs that are ahead of the
                    // frontier. The state at the beginning of those
                    // epochs are not truly known yet, so we can't
                    // apply input in those epochs yet.
                    activate_epochs_buffer.extend(
                        incoming_epoch_to_key_values_buffer
                            .keys()
                            .cloned()
                            .filter(is_closed),
                    );
                    // Even if we don't have input data from an epoch,
                    // we might have some output data from the
                    // previous activation we need to output and the
                    // frontier might have already progressed and it
                    // would be stranded.
                    activate_epochs_buffer.extend(
                        activated_epoch_to_keys_buffer
                            .keys()
                            .cloned()
                            .filter(is_closed),
                    );
                    // Eagerly execute the frontier.
                    activate_epochs_buffer.insert(frontier_epoch);

                    // Drain to re-use buffer. For each epoch in
                    // order:
                    for epoch in activate_epochs_buffer.iter() {
                        // Since the frontier has advanced to at least
                        // this epoch (because we're going through
                        // them in order), say that we'll not be
                        // sending output at any older epochs. This
                        // also asserts "apply changes in epoch order"
                        // to the state cache.
                        output_cap.downgrade(&epoch);
                        state_update_cap.downgrade(&epoch);

                        let incoming_key_values =
                            incoming_epoch_to_key_values_buffer.remove(&epoch);

                        // Now let's find all the key-value pairs to
                        // wake up the logic with.
                        assert!(key_values_buffer.is_empty());

                        // Include all the incoming data.
                        key_values_buffer.extend(
                            incoming_key_values
                                .unwrap_or_default()
                                .into_iter()
                                .map(|(k, v)| (k, Poll::Ready(Some(v)))),
                        );

                        // Then extend the values with any "awake"
                        // activations after the input.
                        if eof {
                            // If this is the last activation,
                            // signal that all keys have
                            // terminated.
                            assert!(keys_buffer.is_empty());
                            // First all "new" keys in this input.
                            keys_buffer.extend(key_values_buffer.iter().map(|(k, _v)| k).cloned());
                            // Then all keys that are still waiting on
                            // wakeup. Keys that do not have a pending
                            // activation will not see EOF messages
                            // (otherwise we'd have to retain data for
                            // all keys ever seen).
                            keys_buffer.extend(key_to_next_activate_at_buffer.keys().cloned());
                            // Drain to re-use allocation.
                            key_values_buffer
                                .extend(keys_buffer.drain().map(|k| (k, Poll::Ready(None))));
                        } else {
                            // Otherwise, wake up any keys
                            // that are past their requested
                            // activation time.
                            key_values_buffer.extend(
                                key_to_next_activate_at_buffer
                                    .iter()
                                    .filter(|(_k, a)| a.elapsed() >= Duration::ZERO)
                                    .map(|(k, _a)| (k.clone(), Poll::Pending)),
                            );
                        }

                        let activated_keys = activated_epoch_to_keys_buffer
                            .entry(epoch.clone())
                            .or_default();

                        let mut output_session = output_handle.session(&output_cap);
                        let mut state_update_session =
                            state_update_handle.session(&state_update_cap);

                        // Drain to re-use allocation.
                        for (key, next_value) in key_values_buffer.drain(..) {
                            // Remove any activation times
                            // this current one will satisfy.
                            if let Entry::Occupied(next_activate_at_entry) =
                                key_to_next_activate_at_buffer.entry(key.clone())
                            {
                                if next_activate_at_entry.get().elapsed() >= Duration::ZERO {
                                    next_activate_at_entry.remove();
                                }
                            }

                            let logic = logic_cache.entry(key.clone()).or_insert_with_key(|key| {
                                // Remove so we only use the resume
                                // state once.
                                let resume_state_bytes = key_to_resume_state_bytes.remove(key);
                                logic_builder(resume_state_bytes)
                            });
                            let (output, activate_after) = logic.exec(next_value);

                            output_session
                                .give_iterator(output.into_iter().map(|item| (key.clone(), item)));

                            if let Some(activate_after) = activate_after {
                                let activate_at = Instant::now() + activate_after;
                                key_to_next_activate_at_buffer
                                    .entry(key.clone())
                                    .and_modify(|next_activate_at: &mut Instant| {
                                        // Only keep the soonest
                                        // activation.
                                        if activate_at < *next_activate_at {
                                            *next_activate_at = activate_at;
                                        }
                                    })
                                    .or_insert(activate_at);
                            }

                            activated_keys.insert(key);
                        }

                        // Snapshot and output state at the end of the
                        // epoch. Remove will ensure we slowly drain
                        // the buffer.
                        if is_closed(&epoch) {
                            if let Some(keys) = activated_epoch_to_keys_buffer.remove(&epoch) {
                                for key in keys {
                                    if let Some(logic) = logic_cache.remove(&key) {
                                        let state_bytes = logic.snapshot();
                                        // Retain logic if not a
                                        // reset.
                                        if let StateUpdate::Upsert(_) = state_bytes {
                                            logic_cache.insert(key.clone(), logic);
                                        }
                                        let update = (key, (step_id.clone(), state_bytes));
                                        state_update_session.give(update);
                                    }
                                }
                            }
                        }
                    }
                    // TODO: One day I hope BTreeSet has drain.
                    activate_epochs_buffer.clear();

                    // Schedule an activation at the next requested
                    // wake up time.
                    let now = Instant::now();
                    if let Some(delay) = key_to_next_activate_at_buffer
                        .values()
                        .map(|a| a.duration_since(now))
                        .min()
                    {
                        activator.activate_after(delay);
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
            KafkaWriter<String, FrontierBackup<u64>>,
            KafkaWriter<RecoveryKey<u64>, StateUpdate>,
            KafkaWriter<RecoveryKey<u64>, StateUpdate>,
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

impl<T: Debug> StateWriter<T> for NoopRecovery {
    fn write(&mut self, backup: &StateBackup<T>) {
        debug!("noop state write backup={backup:?}");
    }
}

impl<T: Debug> StateCollector<T> for NoopRecovery {
    fn delete(&mut self, recovery_key: &RecoveryKey<T>) {
        debug!("noop state delete recovery_key={recovery_key:?}");
    }
}

impl<T> StateReader<T> for NoopRecovery {
    fn read(&mut self) -> Option<StateBackup<T>> {
        debug!("noop state read");
        None
    }
}

impl<T: Debug> ProgressWriter<T> for NoopRecovery {
    fn write(&mut self, frontier_update: &FrontierBackup<T>) {
        debug!("noop frontier write frontier_update={frontier_update:?}");
    }
}

impl<T> ProgressReader<T> for NoopRecovery {
    fn read(&mut self) -> Option<FrontierBackup<T>> {
        debug!("noop frontier read");
        None
    }
}

// The recovery store summary.

/// The [`RecoveryStoreSummary`] doesn't need to retain full copies of
/// state to determine what is garbage (just that there was a reset or
/// an update), so have a little enum here to represent that.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
enum UpdateType {
    Upsert,
    Reset,
}

impl From<StateUpdate> for UpdateType {
    fn from(update: StateUpdate) -> Self {
        match update {
            StateUpdate::Upsert(..) => Self::Upsert,
            StateUpdate::Reset => Self::Reset,
        }
    }
}

// TODO: Find a way to not double-serialize StateUpdate. The recovery
// system has operators sending opaque blobs of bytes, so could we not
// serialize them again? The trouble is we need to distinguish between
// [`StateUpdate::Reset`] and deletion of a [`RecoveryKey`] for each
// data store.
fn to_bytes<T: Serialize>(obj: &T) -> Vec<u8> {
    // TODO: Figure out if there's a more robust-to-evolution way
    // to serialize this key. If the serialization changes between
    // versions, then recovery doesn't work. Or if we use an
    // encoding that isn't deterministic.
    bincode::serialize(obj).expect("Error serializing recovery data")
}

fn from_bytes<'a, T: Deserialize<'a>>(bytes: &'a [u8]) -> T {
    let t_name = type_name::<T>();
    bincode::deserialize(bytes).unwrap_or_else(|_| panic!("Error deserializing recovery {t_name})"))
}

/// In-memory summary of all keys this worker's recovery store knows
/// about.
///
/// This is used to quickly find garbage state without needing to
/// query the recovery store itself.
pub(crate) struct RecoveryStoreSummary<T> {
    db: HashMap<(StepId, StateKey), HashMap<T, UpdateType>>,
}

impl<T: Timestamp> RecoveryStoreSummary<T> {
    pub(crate) fn new() -> Self {
        Self { db: HashMap::new() }
    }

    /// Mark that state for this step ID, key, and epoch was backed
    /// up.
    // TODO: Find a way to not clone update data just to insert.
    pub(crate) fn insert(&mut self, backup: StateBackup<T>) {
        let StateBackup(RecoveryKey(step_id, key, epoch), update) = backup;
        let update_type = update.into();
        self.db
            .entry((step_id, key))
            .or_default()
            .insert(epoch, update_type);
    }

    /// Find and remove all garbage given a finalized epoch.
    ///
    /// Garbage is any state data before or during a finalized epoch,
    /// other than the last upsert for a key (since that's still
    /// relevant since it hasn't been overwritten yet).
    pub(crate) fn remove_garbage(&mut self, finalized_epoch: &T) -> Vec<RecoveryKey<T>> {
        let mut garbage = Vec::new();

        let mut empty_map_keys = Vec::new();
        for (map_key, epoch_updates) in self.db.iter_mut() {
            let (step_id, key) = map_key;

            // TODO: The following becomes way cleaner once
            // [`std::collections::BTreeMap::drain_filter`] and
            // [`std::collections::BTreeMap::first_entry`] hits
            // stable.

            let (mut map_key_garbage, mut map_key_non_garbage): (Vec<_>, Vec<_>) = epoch_updates
                .drain()
                .partition(|(epoch, _update_type)| epoch <= finalized_epoch);
            map_key_garbage.sort();

            // If the final bit of "garbage" is an upsert, keep it,
            // since it's the state we'd use to recover.
            if let Some(epoch_update) = map_key_garbage.pop() {
                let (_epoch, update_type) = &epoch_update;
                if update_type == &UpdateType::Upsert {
                    map_key_non_garbage.push(epoch_update);
                } else {
                    map_key_garbage.push(epoch_update);
                }
            }

            for (epoch, _update_type) in map_key_garbage {
                garbage.push(RecoveryKey(step_id.clone(), key.clone(), epoch));
            }

            // Non-garbage should remain in the in-mem DB.
            *epoch_updates = map_key_non_garbage.into_iter().collect::<HashMap<_, _>>();

            if epoch_updates.is_empty() {
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
