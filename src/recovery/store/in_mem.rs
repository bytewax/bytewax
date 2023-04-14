//! Implementation of in-memory state and progress stores.
//!
//! We use this a few places in recovery's resume and GC system to
//! build up some relevant state about the real recovery store and
//! query it, since we don't make the assumption that real recovery
//! stores have querying abilities.

use std::cmp::Ordering;
use std::collections::HashMap;
use std::collections::{btree_map, BTreeMap};
use timely::progress::Timestamp;

use crate::recovery::model::*;
use crate::worker::{WorkerCount, WorkerIndex};

/// Used for storing just the type of changes for GC.
///
/// See [`ChangeType`] and
/// [`crate::recovery::operators::GarbageCollect`].
pub(crate) type StoreSummary = InMemStore<()>;

/// A state store with all data in memory.
#[derive(Debug)]
pub(crate) struct InMemStore<V> {
    db: HashMap<FlowKey, BTreeMap<SnapshotEpoch, Change<V>>>,
}

impl<V> InMemStore<V> {
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
    /// the cluster finalized epoch, since we only need to recover to
    /// the resume epoch.
    ///
    /// State changes more recent than the resume epoch could be used
    /// later, so we must keep them.
    pub(crate) fn drain_garbage(&mut self, before: &ResumeEpoch) -> impl Iterator<Item = StoreKey> {
        let mut garbage = Vec::new();

        let mut empty_keys = Vec::new();
        for (key, changes) in self.db.iter_mut() {
            // This now contains `(epoch, op)` in epoch order where
            // epoch < resume epoch. So all operations that could be
            // GCd.  Unfortunately [`BTreeMap::split_off`] returns the
            // "high end" / not garbage, so we have to swap around
            // pointers to get ownership right without cloning.
            let (mut garbage_changes, non_garbage_changes) = {
                let tmp = changes.split_off(&SnapshotEpoch(before.0));
                let garbage_changes = std::mem::replace(changes, tmp);
                (garbage_changes, changes)
            };

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

    let key1 = FlowKey(StepId("op1".to_owned()), StateKey("a".to_owned()));
    let key2 = FlowKey(StepId("op1".to_owned()), StateKey("b".to_owned()));

    let upx = Change::Upsert("x".to_owned());
    let upy = Change::Upsert("y".to_owned());
    let upz = Change::Upsert("z".to_owned());

    store.db = HashMap::from([
        (
            key1.clone(),
            BTreeMap::from([
                (SnapshotEpoch(10), upz.clone()),
                (SnapshotEpoch(5), upx.clone()),
                (SnapshotEpoch(6), upy.clone()),
            ]),
        ),
        (
            key2.clone(),
            BTreeMap::from([
                (SnapshotEpoch(2), upx),
                (SnapshotEpoch(3), upz.clone()),
                (SnapshotEpoch(1), upy),
            ]),
        ),
    ]);

    store.filter_last();

    let expected = HashMap::from([
        (key1, BTreeMap::from([(SnapshotEpoch(10), upz.clone())])),
        (key2, BTreeMap::from([(SnapshotEpoch(3), upz)])),
    ]);
    assert_eq!(store.db, expected);
}

#[test]
fn drain_garbage_works() {
    use std::collections::HashSet;

    let mut store = InMemStore::new();

    let key1 = FlowKey(StepId("op1".to_owned()), StateKey("a".to_owned()));
    let key2 = FlowKey(StepId("op1".to_owned()), StateKey("b".to_owned()));

    let upx = Change::Upsert("x".to_owned());
    let upy = Change::Upsert("y".to_owned());
    let upz = Change::Upsert("z".to_owned());

    store.db = HashMap::from([
        (
            key1,
            BTreeMap::from([
                (SnapshotEpoch(10), upz.clone()),
                (SnapshotEpoch(5), upx.clone()),
                (SnapshotEpoch(6), upy.clone()),
            ]),
        ),
        (
            key2.clone(),
            BTreeMap::from([
                (SnapshotEpoch(2), upx),
                (SnapshotEpoch(3), upz),
                (SnapshotEpoch(1), upy),
            ]),
        ),
    ]);

    let found: HashSet<_> = store.drain_garbage(&ResumeEpoch(6)).collect();
    let expected = HashSet::from([
        StoreKey(SnapshotEpoch(1), key2.clone()),
        StoreKey(SnapshotEpoch(2), key2),
    ]);
    assert_eq!(found, expected);
}

#[test]
fn drain_garbage_includes_newest_discard() {
    use std::collections::HashSet;

    let mut store = InMemStore::new();

    let key1 = FlowKey(StepId("op1".to_owned()), StateKey("a".to_owned()));

    let upx = Change::Upsert("x".to_owned());
    let upy = Change::Upsert("y".to_owned());

    store.db = HashMap::from([(
        key1.clone(),
        BTreeMap::from([
            (SnapshotEpoch(2), upx),
            (SnapshotEpoch(3), Change::Discard),
            (SnapshotEpoch(1), upy),
            (SnapshotEpoch(7), Change::Discard),
        ]),
    )]);

    let found: HashSet<_> = store.drain_garbage(&ResumeEpoch(6)).collect();
    let expected = HashSet::from([
        StoreKey(SnapshotEpoch(1), key1.clone()),
        StoreKey(SnapshotEpoch(2), key1.clone()),
        StoreKey(SnapshotEpoch(3), key1),
    ]);
    assert_eq!(found, expected);
}

#[test]
fn drain_garbage_drops_unused_keys() {
    let mut store = InMemStore::new();

    let key1 = FlowKey(StepId("op1".to_owned()), StateKey("a".to_owned()));

    let upx = Change::Upsert("x".to_owned());
    let upy = Change::Upsert("y".to_owned());

    store.db = HashMap::from([(
        key1,
        BTreeMap::from([
            (SnapshotEpoch(2), upx),
            (SnapshotEpoch(3), Change::Discard),
            (SnapshotEpoch(1), upy),
        ]),
    )]);

    // Must use the iterator.
    store.drain_garbage(&ResumeEpoch(6)).for_each(drop);

    let expected = HashMap::from([]);
    assert_eq!(store.db, expected);
}

#[test]
fn drain_works() {
    let mut store = InMemStore::new();

    let key1 = FlowKey(StepId("op1".to_owned()), StateKey("a".to_owned()));

    let upx = Change::Upsert("x".to_owned());
    let upy = Change::Upsert("y".to_owned());
    let upz = Change::Upsert("z".to_owned());

    store.db = HashMap::from([(
        key1.clone(),
        BTreeMap::from([
            (SnapshotEpoch(10), upz.clone()),
            (SnapshotEpoch(5), upx.clone()),
            (SnapshotEpoch(6), upy.clone()),
        ]),
    )]);

    let found: Vec<_> = store.drain_flatten().collect();
    let expected = vec![
        KChange(key1.clone(), upx),
        KChange(key1.clone(), upy),
        KChange(key1, upz),
    ];
    assert_eq!(found, expected);
}

impl<V> KWriter<StoreKey, Change<V>> for InMemStore<V> {
    fn write(&mut self, kchange: KChange<StoreKey, Change<V>>) {
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

    let key1 = FlowKey(StepId("op1".to_owned()), StateKey("a".to_owned()));

    let upx = Change::Upsert("x".to_owned());
    let upy = Change::Upsert("y".to_owned());

    store.write(KChange(
        StoreKey(SnapshotEpoch(5), key1.clone()),
        Change::Upsert(upx),
    ));
    store.write(KChange(
        StoreKey(SnapshotEpoch(5), key1.clone()),
        Change::Upsert(upy.clone()),
    ));

    let expected = HashMap::from([(key1, BTreeMap::from([(SnapshotEpoch(5), upy)]))]);
    assert_eq!(store.db, expected);
}

#[test]
fn write_discard_drops_key() {
    let mut store = InMemStore::new();

    let key1 = FlowKey(StepId("op1".to_owned()), StateKey("a".to_owned()));

    let upx = Change::Upsert("x".to_owned());

    store.write(KChange(
        StoreKey(SnapshotEpoch(5), key1.clone()),
        Change::Upsert(upx),
    ));
    store.write(KChange(StoreKey(SnapshotEpoch(5), key1), Change::Discard));

    let expected = HashMap::from([]);
    assert_eq!(store.db, expected);
}

/// A progress store with all data in-memory.
#[derive(Debug)]
pub(crate) struct InMemProgress {
    /// Stores the current execution.
    ///
    /// Defaults to `-1` so the initial call to [`resume_from`] will
    /// generate the execution `0`. There will never be any recovery
    /// progress data that will match execution `-1`, so as soon as we
    /// start reading any, we'll bump to the correct execution.
    ex: i128,
    frontiers: HashMap<WorkerIndex, WorkerFrontier>,
}

impl InMemProgress {
    /// Init to the "default" progress of a cluster of a given
    /// size.
    ///
    /// You have to specify a worker count so the initial data is
    /// well-formed.
    pub(crate) fn new(count: WorkerCount) -> Self {
        let ex = Into::<i128>::into(<u64 as Timestamp>::minimum()) - 1;
        let frontiers = count
            .iter()
            .map(|worker| (worker, WorkerFrontier(<u64 as Timestamp>::minimum())))
            .collect();
        Self { ex, frontiers }
    }

    pub(crate) fn worker_count(&self) -> WorkerCount {
        WorkerCount(self.frontiers.len())
    }

    fn init(&mut self, ex: Execution, count: WorkerCount, epoch: ResumeEpoch) {
        // Tidbit: you almost never want to use `as` numerical
        // casts. They do "bit folding" on downcasts and don't panic
        // when you're out of range, which is probably what you want.
        let in_ex: i128 = ex.0.into();

        match in_ex.cmp(&self.ex) {
            // Execution should never regress because the entire cluster
            // is shut down before resuming.
            Ordering::Less => panic!("Cluster execution regressed"),
            // It's ok if in_ex == self.ex since we might have multiple
            // workers from the previous execution multiplexed into the
            // same progress partition.
            Ordering::Equal => {
                assert!(
                    count == self.worker_count(),
                    "Single execution has inconsistent worker count"
                );
            }
            Ordering::Greater => {
                self.ex = in_ex;
                self.frontiers = count
                    .iter()
                    .map(|worker| (worker, WorkerFrontier(epoch.0)))
                    .collect();
            }
        }
    }

    fn advance(&mut self, ex: Execution, worker: WorkerIndex, epoch: WorkerFrontier) {
        let in_ex: i128 = ex.0.into();
        // Each progress partition has an ordered view of events on
        // some number of workers, so we should always see an init to
        // an execution before any advances in that execution
        // (otherwise a single worker has written progress
        // out-of-order). But it is possible during resume that
        // progress partitions are interleaved, so we might read all
        // progress for one worker, then "go back in time" and all
        // progress for a second worker. But that's fine, those old
        // advance messages can be discarded because we know there was
        // a later execution anyway.
        assert!(
            in_ex <= self.ex,
            "Advance without init in single progress partition"
        );
        if in_ex == self.ex {
            let last_epoch = self
                .frontiers
                .insert(worker, epoch)
                .expect("Advancing unknown worker");
            // Double check Timely's sanity and recovery store ordering.
            assert!(last_epoch <= epoch, "Worker frontier regressed");
        }
    }

    /// Calculate the resume execution and epoch from the previous
    /// cluster state.
    ///
    /// This should be the next execution (starting from 0) and the
    /// previous cluster frontier.
    pub(crate) fn resume_from(&self) -> ResumeFrom {
        let next_ex = self.ex + 1;
        let next_ex = Execution(
            next_ex
                .try_into()
                .expect("Next execution ID would overflow u64"),
        );

        assert!(
            self.worker_count()
                .iter()
                .all(|worker| self.frontiers.contains_key(&worker)),
            "Progress map is missing some workers"
        );
        let resume_epoch = ResumeEpoch(
            self.frontiers
                .values()
                .min()
                .copied()
                .unwrap_or(WorkerFrontier(<u64 as Timestamp>::minimum()))
                .0,
        );

        ResumeFrom(next_ex, resume_epoch)
    }
}

#[test]
fn worker_count_works() {
    let progress = InMemProgress::new(WorkerCount(5));

    let found = progress.worker_count();
    let expected = WorkerCount(5);
    assert_eq!(found, expected);
}

#[test]
#[should_panic]
fn init_panics_on_execution_regress() {
    let mut progress = InMemProgress::new(WorkerCount(2));

    let ex1 = Execution(1);
    progress.init(ex1, WorkerCount(3), ResumeEpoch(1));

    let ex0 = Execution(0);
    progress.init(ex0, WorkerCount(3), ResumeEpoch(1));
}

#[test]
#[should_panic]
fn init_panics_on_inconsistent_worker_count() {
    let mut progress = InMemProgress::new(WorkerCount(2));

    let ex1 = Execution(1);
    progress.init(ex1, WorkerCount(3), ResumeEpoch(1));

    progress.init(ex1, WorkerCount(5), ResumeEpoch(1));
}

#[test]
#[should_panic]
fn advance_panics_on_frontier_regress() {
    let mut progress = InMemProgress::new(WorkerCount(2));

    let ex1 = Execution(1);
    progress.init(ex1, WorkerCount(3), ResumeEpoch(1));

    progress.advance(ex1, WorkerIndex(0), WorkerFrontier(2));
    progress.advance(ex1, WorkerIndex(0), WorkerFrontier(1));
}

#[test]
#[should_panic]
fn advance_panics_on_execution_skip() {
    let mut progress = InMemProgress::new(WorkerCount(2));

    let ex1 = Execution(1);
    progress.init(ex1, WorkerCount(3), ResumeEpoch(1));

    let ex2 = Execution(2);
    progress.advance(ex2, WorkerIndex(0), WorkerFrontier(2));
}

#[test]
fn advance_ignores_late_executions() {
    let mut progress = InMemProgress::new(WorkerCount(2));

    let ex1 = Execution(1);
    progress.init(ex1, WorkerCount(3), ResumeEpoch(4));

    let ex0 = Execution(0);
    progress.advance(ex0, WorkerIndex(0), WorkerFrontier(2));

    let found = progress.resume_from();
    let expected = ResumeFrom(Execution(2), ResumeEpoch(4));
    assert_eq!(found, expected);
}

#[test]
fn resume_from_default_works() {
    let progress = InMemProgress::new(WorkerCount(2));

    let found = progress.resume_from();
    let expected = ResumeFrom(
        Execution(<u64 as Timestamp>::minimum()),
        ResumeEpoch(<u64 as Timestamp>::minimum()),
    );
    assert_eq!(found, expected);
}

#[test]
fn resume_from_works() {
    let mut progress = InMemProgress::new(WorkerCount(1));

    let ex0 = Execution(0);
    progress.init(ex0, WorkerCount(2), ResumeEpoch(0));

    progress.advance(ex0, WorkerIndex(0), WorkerFrontier(1));
    progress.advance(ex0, WorkerIndex(1), WorkerFrontier(1));
    progress.advance(ex0, WorkerIndex(0), WorkerFrontier(2));

    let ex1 = Execution(1);
    progress.init(ex1, WorkerCount(3), ResumeEpoch(1));

    progress.advance(ex1, WorkerIndex(0), WorkerFrontier(2));
    progress.advance(ex1, WorkerIndex(1), WorkerFrontier(2));
    progress.advance(ex1, WorkerIndex(2), WorkerFrontier(2));

    let found = progress.resume_from();
    let expected = ResumeFrom(Execution(2), ResumeEpoch(2));
    assert_eq!(found, expected);
}

impl KWriter<WorkerKey, ProgressMsg> for InMemProgress {
    fn write(&mut self, kchange: KChange<WorkerKey, ProgressMsg>) {
        let KChange(key, change) = kchange;
        let WorkerKey(ex, index) = key;
        match change {
            Change::Upsert(msg) => match msg {
                ProgressMsg::Init(count, epoch) => {
                    self.init(ex, count, epoch);
                }
                ProgressMsg::Advance(epoch) => {
                    self.advance(ex, index, epoch);
                }
            },
            Change::Discard => {}
        }
    }
}
