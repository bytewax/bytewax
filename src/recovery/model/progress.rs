//! Data model representing progress in the dataflow and the recovery
//! system.
//!
//! A progress store is a K-V mapping from [`WorkerKey`] to a
//! finalized `Epoch`.

use super::change::*;
use serde::Deserialize;
use serde::Serialize;

/// Incrementing ID for a dataflow cluster.
///
/// This is used to ensure recovery progress information for a worker
/// `3` is not mis-interpreted to belong to a different cluster.
///
/// As you resume a dataflow, this will increase by 1 each time.
#[derive(Debug, Copy, Clone, Hash, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub(crate) struct Execution(pub(crate) u64);

pub(crate) use crate::execution::{WorkerCount, WorkerIndex};

/// Timely uses the unit type to represent a "tick" or "heartbeat" on
/// a clock stream, a dataflow stream that you only care about the
/// progress messages.
pub(crate) type Tick = ();

/// Key used to store progress information for a specific worker in
/// the recovery store.
#[derive(Debug, Copy, Clone, Hash, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct WorkerKey(pub(crate) Execution, pub(crate) WorkerIndex);

/// The oldest epoch for which work is still outstanding on a worker.
#[derive(Debug, Copy, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub(crate) struct WorkerFrontier(pub(crate) u64);

/// The epoch a new dataflow execution should resume from the
/// beginning of.
///
/// This will be the dataflow frontier of the last execution.
#[derive(Debug, Copy, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct ResumeEpoch(pub(crate) u64);

/// To resume a dataflow execution, you need to know which epoch to
/// resume for state, but also which execution to label progress data
/// with.
#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub(crate) struct ResumeFrom(pub(crate) Execution, pub(crate) ResumeEpoch);

/// Types of recovery data related to progress on a worker.
#[derive(Debug, Copy, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) enum ProgressMsg {
    /// Information about this execution. Applies during the entire
    /// execution.
    Init(WorkerCount, ResumeEpoch),
    /// Progress made by this worker.
    Advance(WorkerFrontier),
}

/// A change to the progress store.
///
/// Notes that a worker's finalized epoch has changed.
pub(crate) type ProgressChange = KChange<WorkerKey, ProgressMsg>;

/// All progress stores have to implement this writer.
///
/// Since trait aliases don't work in stable, don't actually `impl`
/// this, but it's used for bounds.
pub(crate) trait ProgressWriter: KWriter<WorkerKey, ProgressMsg> {}

impl<P> ProgressWriter for Box<P> where P: ProgressWriter + ?Sized {}

/// All progress stores have to implement this reader.
///
/// Since trait aliases don't work in stable, don't actually `impl`
/// this, but it's used for bounds.
pub(crate) trait ProgressReader: KReader<WorkerKey, ProgressMsg> {}

impl<P> ProgressReader for Box<P> where P: ProgressReader + ?Sized {}
