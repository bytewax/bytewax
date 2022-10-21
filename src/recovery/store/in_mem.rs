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
