//! Data model representing progress in the dataflow and the recovery
//! system.
//!
//! A progress store is a K-V mapping from [`WorkerKey`] to a
//! finalized epoch `T`.

use super::change::*;
use serde::Deserialize;
use serde::Serialize;

pub(crate) use crate::execution::WorkerIndex;

/// Timely uses the unit type to represent a "tick" or "heartbeat" on
/// a clock stream, a dataflow stream that you only care about the
/// progress messages.
pub(crate) type Tick = ();

/// Unique ID for a worker.
#[derive(Debug, Copy, Clone, Hash, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct WorkerKey(pub(crate) WorkerIndex);

/// The newest epoch for which all work has been completed.
///
/// The epoch just before the frontier.
#[derive(Debug, Copy, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub(crate) struct BorderEpoch<T>(pub(crate) T);

/// The epoch we should resume from the beginning of.
///
/// This will be the dataflow frontier of the last execution.
#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub(crate) struct ResumeEpoch<T>(pub(crate) T);

/// A change to the progress store.
///
/// Notes that a worker's finalized epoch has changed.
pub(crate) type ProgressChange<T> = KChange<WorkerKey, BorderEpoch<T>>;

/// All progress stores have to implement this writer.
///
/// Since trait aliases don't work in stable, don't actually `impl`
/// this, but it's used for bounds.
pub(crate) trait ProgressWriter<T>: KWriter<WorkerKey, BorderEpoch<T>> {}

impl<T, P> ProgressWriter<T> for Box<P> where P: ProgressWriter<T> + ?Sized {}

/// All progress stores have to implement this reader.
///
/// Since trait aliases don't work in stable, don't actually `impl`
/// this, but it's used for bounds.
pub(crate) trait ProgressReader<T>: KReader<WorkerKey, BorderEpoch<T>> {}

impl<T, P> ProgressReader<T> for Box<P> where P: ProgressReader<T> + ?Sized {}
