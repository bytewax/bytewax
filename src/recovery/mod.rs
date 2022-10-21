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

// The recovery store summary.
