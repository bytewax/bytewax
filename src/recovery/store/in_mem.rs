//! Implementation of in-memory state and progress stores.
//!
//! We use this a few places in recovery's resume and GC system to
//! build up some relevant state about the real recovery store and
//! query it, since we don't make the assumption that real recovery
//! stores have querying abilities.

use std::collections::HashMap;
use std::collections::{btree_map, BTreeMap};
use timely::progress::Timestamp;

use crate::recovery::model::*;

/// Used for storing just the type of changes for GC.
///
/// See [`ChangeType`] and
/// [`crate::recovery::operators::GarbageCollect`].
pub(crate) type StoreSummary<T> = InMemStore<T, ()>;

/// A state store with all data in memory.
#[derive(Debug)]
pub(crate) struct InMemStore<T, V> {
    db: HashMap<FlowKey, BTreeMap<T, Change<V>>>,
}

impl<T, V> InMemStore<T, V>
where
    T: Clone + Ord,
{
    pub(crate) fn new() -> Self {
        Self { db: HashMap::new() }
    }

    /// Drop all but the latest epoch for each key.
    pub(crate) fn filter_last(&mut self) {
        for changes in self.db.values_mut() {
            if let Some(split_epoch) = changes.keys().nth_back(0).cloned() {
                *changes = changes.split_off(&split_epoch);
            }
        }
    }

    /// Find all recovery keys for which the data is no longer needed
    /// for recovery.
    ///
    /// This is all but the last upsert for each routing key before
    /// the cluster frontier, since we only need to recover to the
    /// cluster frontier.
    ///
    /// State changes more recent than the frontier could be used
    /// later, so we must keep them.
    pub(crate) fn drain_garbage(&mut self, frontier: &T) -> impl Iterator<Item = StoreKey<T>> {
        let mut garbage = Vec::new();

        let mut empty_keys = Vec::new();
        for (key, non_garbage_changes) in self.db.iter_mut() {
            // This now contains `(epoch, op)` in epoch order where
            // epoch < frontier. So all operations that could be GCd.
            // Unfortunately [`BTreeMap::split_off`] returns the "high
            // end" / not garbage, so we have to swap around pointers
            // to get ownership right without cloning.
            let tmp = non_garbage_changes.split_off(frontier);
            let mut garbage_changes = std::mem::replace(non_garbage_changes, tmp);

            // If the newest bit of "garbage" in epoch order is an
            // upsert, keep it, since it's the state we'd use to
            // recover at the finalized epoch. TODO: Could combine
            // these two ifs with [`BTreeMap::last_entry`] when
            // stable.
            if let Some(newest_epoch) = garbage_changes.keys().last().cloned() {
                if let btree_map::Entry::Occupied(newest_change) =
                    garbage_changes.entry(newest_epoch)
                {
                    match newest_change.get() {
                        Change::Upsert(..) => {
                            // Put it back in the "not garbage".
                            let (epoch, op) = newest_change.remove_entry();
                            non_garbage_changes.insert(epoch, op);
                        }
                        Change::Discard => {}
                    }
                }
            }

            garbage.extend(
                garbage_changes
                    .into_keys()
                    .map(|epoch| StoreKey(epoch, key.clone())),
            );

            if non_garbage_changes.is_empty() {
                empty_keys.push(key.clone());
            }
        }

        // Clean up any keys that aren't seen again.
        for key in empty_keys {
            self.db.remove(&key);
        }

        garbage.into_iter()
    }

    /// Drain all changes in this recovery store into operator
    /// changes.
    ///
    /// Emit changes for each routing key in epoch order. This'll let
    /// you write them into the staet for that operator.
    pub(crate) fn drain_flatten(&mut self) -> impl Iterator<Item = KChange<FlowKey, V>> + '_ {
        self.db.drain().flat_map(|(key, changes)| {
            changes
                // Emits in epoch order.
                .into_values()
                .map(move |change| KChange(key.clone(), change))
        })
    }
}

#[test]
fn filter_last_works() {
    let mut store = InMemStore::new();

    let key1 = FlowKey(StepId("op1".to_owned()), StateKey::Hash("a".to_owned()));
    let key2 = FlowKey(StepId("op1".to_owned()), StateKey::Hash("b".to_owned()));

    let upx = Change::Upsert("x".to_owned());
    let upy = Change::Upsert("y".to_owned());
    let upz = Change::Upsert("z".to_owned());

    store.db = HashMap::from([
        (
            key1.clone(),
            BTreeMap::from([(10, upz.clone()), (5, upx.clone()), (6, upy.clone())]),
        ),
        (
            key2.clone(),
            BTreeMap::from([(2, upx.clone()), (3, upz.clone()), (1, upy.clone())]),
        ),
    ]);

    store.filter_last();

    let expected = HashMap::from([
        (key1.clone(), BTreeMap::from([(10, upz.clone())])),
        (key2.clone(), BTreeMap::from([(3, upz.clone())])),
    ]);
    assert_eq!(store.db, expected);
}

#[test]
fn drain_garbage_works() {
    use std::collections::HashSet;

    let mut store = InMemStore::new();

    let key1 = FlowKey(StepId("op1".to_owned()), StateKey::Hash("a".to_owned()));
    let key2 = FlowKey(StepId("op1".to_owned()), StateKey::Hash("b".to_owned()));

    let upx = Change::Upsert("x".to_owned());
    let upy = Change::Upsert("y".to_owned());
    let upz = Change::Upsert("z".to_owned());

    store.db = HashMap::from([
        (
            key1.clone(),
            BTreeMap::from([(10, upz.clone()), (5, upx.clone()), (6, upy.clone())]),
        ),
        (
            key2.clone(),
            BTreeMap::from([(2, upx.clone()), (3, upz.clone()), (1, upy.clone())]),
        ),
    ]);

    let found: HashSet<_> = store.drain_garbage(&6).collect();
    let expected = HashSet::from([StoreKey(1, key2.clone()), StoreKey(2, key2.clone())]);
    assert_eq!(found, expected);
}

#[test]
fn drain_garbage_includes_newest_discard() {
    use std::collections::HashSet;

    let mut store = InMemStore::new();

    let key1 = FlowKey(StepId("op1".to_owned()), StateKey::Hash("a".to_owned()));

    let upx = Change::Upsert("x".to_owned());
    let upy = Change::Upsert("y".to_owned());

    store.db = HashMap::from([(
        key1.clone(),
        BTreeMap::from([
            (2, upx.clone()),
            (3, Change::Discard),
            (1, upy.clone()),
            (7, Change::Discard),
        ]),
    )]);

    let found: HashSet<_> = store.drain_garbage(&6).collect();
    let expected = HashSet::from([
        StoreKey(1, key1.clone()),
        StoreKey(2, key1.clone()),
        StoreKey(3, key1.clone()),
    ]);
    assert_eq!(found, expected);
}

#[test]
fn drain_garbage_drops_unused_keys() {
    let mut store = InMemStore::new();

    let key1 = FlowKey(StepId("op1".to_owned()), StateKey::Hash("a".to_owned()));

    let upx = Change::Upsert("x".to_owned());
    let upy = Change::Upsert("y".to_owned());

    store.db = HashMap::from([(
        key1.clone(),
        BTreeMap::from([(2, upx.clone()), (3, Change::Discard), (1, upy.clone())]),
    )]);

    // Must use the iterator.
    store.drain_garbage(&6).for_each(drop);

    let expected = HashMap::from([]);
    assert_eq!(store.db, expected);
}

#[test]
fn drain_works() {
    let mut store = InMemStore::new();

    let key1 = FlowKey(StepId("op1".to_owned()), StateKey::Hash("a".to_owned()));

    let upx = Change::Upsert("x".to_owned());
    let upy = Change::Upsert("y".to_owned());
    let upz = Change::Upsert("z".to_owned());

    store.db = HashMap::from([(
        key1.clone(),
        BTreeMap::from([(10, upz.clone()), (5, upx.clone()), (6, upy.clone())]),
    )]);

    let found: Vec<_> = store.drain_flatten().collect();
    let expected = vec![
        KChange(key1.clone(), upx.clone()),
        KChange(key1.clone(), upy.clone()),
        KChange(key1.clone(), upz.clone()),
    ];
    assert_eq!(found, expected);
}

impl<T, V> KWriter<StoreKey<T>, Change<V>> for InMemStore<T, V>
where
    T: Ord,
{
    fn write(&mut self, kchange: KChange<StoreKey<T>, Change<V>>) {
        let KChange(StoreKey(epoch, key), change) = kchange;
        let changes = self.db.entry(key.clone()).or_default();
        changes.write(KChange(epoch, change));
        if changes.is_empty() {
            self.db.remove(&key);
        }
    }
}

#[test]
fn write_upserts() {
    let mut store = InMemStore::new();

    let key1 = FlowKey(StepId("op1".to_owned()), StateKey::Hash("a".to_owned()));

    let upx = Change::Upsert("x".to_owned());
    let upy = Change::Upsert("y".to_owned());

    store.write(KChange(
        StoreKey(5, key1.clone()),
        Change::Upsert(upx.clone()),
    ));
    store.write(KChange(
        StoreKey(5, key1.clone()),
        Change::Upsert(upy.clone()),
    ));

    let expected = HashMap::from([(key1.clone(), BTreeMap::from([(5, upy.clone())]))]);
    assert_eq!(store.db, expected);
}

#[test]
fn write_discard_drops_key() {
    let mut store = InMemStore::new();

    let key1 = FlowKey(StepId("op1".to_owned()), StateKey::Hash("a".to_owned()));

    let upx = Change::Upsert("x".to_owned());

    store.write(KChange(
        StoreKey(5, key1.clone()),
        Change::Upsert(upx.clone()),
    ));
    store.write(KChange(StoreKey(5, key1.clone()), Change::Discard));

    let expected = HashMap::from([]);
    assert_eq!(store.db, expected);
}

/// A progress store with all data in-memory.
#[derive(Debug)]
pub(crate) struct InMemProgress<T>(HashMap<WorkerKey, T>);

impl<T> InMemProgress<T>
where
    T: Timestamp,
{
    pub(crate) fn new() -> Self {
        Self(HashMap::new())
    }

    /// Calculate the current cluster frontier.
    pub(crate) fn frontier(&self) -> T {
        self.0.values().min().cloned().unwrap_or_else(T::minimum)
    }
}

#[test]
fn cluster_frontier_works() {
    let mut progress = InMemProgress::new();

    progress.0 = HashMap::from([
        (WorkerKey(WorkerIndex(1)), 5),
        (WorkerKey(WorkerIndex(1)), 2),
    ]);

    let found = progress.frontier();
    let expected = 2;
    assert_eq!(found, expected);
}

#[test]
fn cluster_frontier_returns_starting_epoch() {
    let progress = InMemProgress::<u64>::new();

    let found = progress.frontier();
    let expected = 0;
    assert_eq!(found, expected);
}

impl<T> KWriter<WorkerKey, T> for InMemProgress<T> {
    fn write(&mut self, kchange: KChange<WorkerKey, T>) {
        self.0.write(kchange)
    }
}
