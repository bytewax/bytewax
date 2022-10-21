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

/// Snapshot of state data.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct State {
    pub(crate) snapshot: StateBytes,
    pub(crate) next_awake: Option<DateTime<Utc>>,
}
