/// Possible modification operations that can be performed for each
/// [`ProgressRecoveryKey`].
///
/// Since progress can't be "deleted", we only allow upserts.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) enum ProgressOp<T> {
    /// Snapshot of new progress data.
    Upsert(Progress<T>),
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

/// An update to the progress table of the recovery store.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct ProgressUpdate<T>(pub(crate) ProgressRecoveryKey, pub(crate) ProgressOp<T>);

/// An update to the state table of the recovery store.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct StateUpdate<T>(pub(crate) StateRecoveryKey<T>, pub(crate) StateOp);
