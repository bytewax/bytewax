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

pub(crate) trait ProgressWriter<T> {
    fn write(&mut self, update: &ProgressUpdate<T>);
}

pub(crate) trait ProgressReader<T> {
    /// Has the same semantics as [`std::iter::Iterator::next`]:
    /// return [`None`] to signal EOF.
    fn read(&mut self) -> Option<ProgressUpdate<T>>;
}
