//! Low-level, generic extensions for Timely.

use std::cmp::Ordering;
use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::collections::BinaryHeap;
use std::collections::HashMap;
use std::fmt::Debug;
use std::fmt::Display;
use std::hash::BuildHasher;
use std::hash::Hash;
use std::hash::Hasher;
use std::time::Duration;

use num::CheckedSub;
use opentelemetry::global;
use opentelemetry::KeyValue;
use serde::Deserialize;
use serde::Serialize;
use timely::communication::message::RefOrMut;
use timely::dataflow::channels::pact::Exchange;
use timely::dataflow::channels::pact::ParallelizationContract;
use timely::dataflow::channels::pact::Pipeline;
use timely::dataflow::operators::generic::builder_rc::OperatorBuilder;
use timely::dataflow::operators::generic::operator::empty;
use timely::dataflow::operators::generic::InputHandle;
use timely::dataflow::operators::Broadcast;
use timely::dataflow::operators::Capability;
use timely::dataflow::operators::Exchange as ExchangeOp;
use timely::dataflow::operators::Map;
use timely::dataflow::Scope;
use timely::dataflow::Stream;
use timely::order::TotalOrder;
use timely::progress::frontier::MutableAntichain;
use timely::progress::Timestamp;
use timely::worker::AsWorker;
use timely::Data;
use timely::ExchangeData;

use crate::with_timer;

/// Timely's name for a dataflow stream where you only care about the
/// progress messages.
pub(crate) type ClockStream<S> = Stream<S, ()>;

/// Helper class for buffering input in a Timely operator because it
/// arrives out of order.
pub(crate) struct InBuffer<T, D>
where
    T: Timestamp,
{
    tmp: Vec<D>,
    buffer: BTreeMap<T, Vec<D>>,
}

impl<T, D> InBuffer<T, D>
where
    T: Timestamp,
    D: Clone,
{
    pub(crate) fn new() -> Self {
        Self {
            tmp: Vec::new(),
            buffer: BTreeMap::new(),
        }
    }

    /// Buffer that this input was received on this epoch.
    pub(crate) fn extend(&mut self, epoch: T, incoming: RefOrMut<Vec<D>>) {
        assert!(self.tmp.is_empty());
        incoming.swap(&mut self.tmp);
        self.buffer
            .entry(epoch)
            .and_modify(|buf| buf.append(&mut self.tmp))
            // Prevent Vec copy on new data for this epoch.
            .or_insert_with(|| std::mem::take(&mut self.tmp));
    }

    /// Get all input received on a given epoch.
    ///
    /// It's your job to decide if this epoch will see more data using
    /// some sort of notificator or checking the frontier.
    pub(crate) fn remove(&mut self, epoch: &T) -> Option<Vec<D>> {
        self.buffer.remove(epoch)
    }

    /// Return an iterator of all the epochs currently buffered in
    /// epoch order.
    pub(crate) fn epochs(&self) -> impl Iterator<Item = T> + '_ {
        self.buffer.keys().cloned()
    }
}

/// Extension trait for frontiers.
pub(crate) trait FrontierEx<T>
where
    T: Timestamp + TotalOrder,
{
    /// Collapse a frontier into a single epoch value.
    ///
    /// We can do this because we're using a totally ordered epoch in
    /// our dataflows.
    fn simplify(&self) -> Option<T>;

    /// Is a given epoch closed based on this frontier?
    fn is_closed(&self, epoch: &T) -> bool {
        self.simplify().iter().all(|frontier| *epoch < *frontier)
    }

    /// Is this input EOF and will see no more values?
    fn is_eof(&self) -> bool {
        self.simplify().is_none()
    }
}

impl<T> FrontierEx<T> for MutableAntichain<T>
where
    T: Timestamp + TotalOrder,
{
    fn simplify(&self) -> Option<T> {
        self.frontier().iter().min().cloned()
    }
}

/// To allow collapsing frontiers of all inputs in an operator.
impl<T> FrontierEx<T> for [MutableAntichain<T>]
where
    T: Timestamp + TotalOrder,
{
    fn simplify(&self) -> Option<T> {
        self.iter().flat_map(|ma| ma.simplify()).min()
    }
}

/// Extension trait for iterators of capabilities.
pub(crate) trait CapabilityIterEx<'a, T> {
    /// Run [`Capability::downgrade`] on all capabilities.
    ///
    /// Since capabilities retain their output port, you can do this
    /// on the vec of initial caps.
    fn downgrade_all(&'a mut self, epoch: &T);
}

// Wild bounds, but they mean this trait applies to any type which
// you can get an iterator over the mutable contents.
//
// This means it works for both [`Vec`] and [`Option`].
impl<'a, C, T> CapabilityIterEx<'a, T> for C
where
    C: 'a,
    &'a mut C: IntoIterator<Item = &'a mut Capability<T>>,
    T: Timestamp,
{
    fn downgrade_all(&'a mut self, epoch: &T) {
        self.into_iter().for_each(|cap| cap.downgrade(epoch));
    }
}

/// Manages running logic functions and iteratively downgrading
/// capabilities based on the current operator's input frontiers.
///
/// Any state that you need want to drop on EOF you can put in the
/// state variable. Otherwise, you can close over it mutably in a
/// single epoch logic callback function.
///
/// This is like [`timely::dataflow::operators::Notificator`] but does
/// _not_ wait until the epoch is fully closed to run logic. This
/// requires that the timestamp be totally ordered, though to ensure
/// that makes sense.
pub(crate) struct EagerNotificator<T, D>
where
    T: Timestamp + TotalOrder,
{
    /// We have to retain separate capabilities per-output. This seems
    /// to be only documented in
    /// https://github.com/TimelyDataflow/timely-dataflow/pull/187
    caps_state: Option<(Vec<Capability<T>>, D)>,
    queue: BTreeSet<T>,
}

impl<T, D> EagerNotificator<T, D>
where
    T: Timestamp + TotalOrder,
{
    pub(crate) fn new(init_caps: Vec<Capability<T>>, init_state: D) -> Self {
        Self {
            caps_state: Some((init_caps, init_state)),
            queue: BTreeSet::new(),
        }
    }

    /// Mark this epoch as having seen input items and logics should
    /// be called when the time is right.
    ///
    /// We need to use the "notify at" pattern of Timely's built in
    /// [`timely::dataflow::operators::Notificator`] in order to keep
    /// track of all epochs outstanding because otherwise we don't
    /// have access to the largest epoch value to process as closed
    /// when an EOF occurs; he input handles only tell us "empty
    /// frontier".
    pub(crate) fn notify_at(&mut self, epoch: T) {
        assert!(self.caps_state.is_some(), "Got items after EOF");
        self.queue.insert(epoch);
    }

    /// Do some logic in epoch order eagerly.
    ///
    /// Call this on each operator activation with the current input
    /// frontiers.
    ///
    /// Eager logic could be called multiple times for an epoch; if
    /// the frontier does not move, will be called on each
    /// activation. Close logic will be called only once when each
    /// epoch closes.
    ///
    /// Both logics are only called if that epoch is marked via
    /// [`notify_at`].
    ///
    /// The slice of [`Capability`]s will be the automatically
    /// downgraded versions of `init_caps` passed to [`new`].
    pub(crate) fn for_each(
        &mut self,
        input_frontiers: &[MutableAntichain<T>],
        mut eager_logic: impl FnMut(&[Capability<T>], &mut D),
        mut close_logic: impl FnMut(&[Capability<T>], &mut D),
    ) {
        // Ignore if we re-activate multiple times after EOF.
        self.caps_state = self.caps_state.take().and_then(|(mut caps, mut state)| {
            let frontier = input_frontiers.simplify();

            // Drain off epochs from the queue that are less than the
            // frontier and so we can run both logics on them. Ones in
            // advance of the frontier remain in the queue for later.
            let closed_epochs = if let Some(frontier) = &frontier {
                // Do a little swap-a-roo since
                // [`BTreeSet::split_off`] only calculates >=, but we
                // want <.
                let ge_frontier = self.queue.split_off(frontier);
                std::mem::replace(&mut self.queue, ge_frontier)
            } else {
                // None means EOF on all inputs. All epochs in the
                // queue are closed. Replace `self.queue` with an
                // empty [`BTreeSet`].
                std::mem::take(&mut self.queue)
            };

            // Will iterate in epoch order since [`BTreeSet`].
            for epoch in closed_epochs {
                caps.downgrade_all(&epoch);

                eager_logic(&caps, &mut state);
                close_logic(&caps, &mut state);
            }

            if let Some(frontier) = &frontier {
                caps.downgrade_all(frontier);

                // Now eagerly execute the frontier. No need to call
                // logic if we haven't any data at the frontier yet.
                if self.queue.contains(frontier) {
                    eager_logic(&caps, &mut state);
                }

                Some((caps, state))
            } else {
                // This drops the state and the caps since EOF.
                None
            }
        });
    }
}

/// Extension trait for [`InputHandle`].
pub(crate) trait OpInputHandleEx<T, D>
where
    T: Timestamp + TotalOrder,
    D: Data,
{
    /// Buffer all incoming items into an [`InBuffer`] and mark all
    /// epochs seen in those items to notified at in an
    /// [`EagerNotificator`].
    fn buffer_notify<S>(&mut self, inbuf: &mut InBuffer<T, D>, ncater: &mut EagerNotificator<T, S>);
}

impl<T, D, P> OpInputHandleEx<T, D> for InputHandle<T, D, P>
where
    T: Timestamp + TotalOrder,
    D: Data,
    P: timely::communication::Pull<timely::dataflow::channels::Bundle<T, D>>,
{
    fn buffer_notify<S>(
        &mut self,
        inbuf: &mut InBuffer<T, D>,
        ncater: &mut EagerNotificator<T, S>,
    ) {
        self.for_each(|cap, incoming| {
            let epoch = cap.time();
            inbuf.extend(epoch.clone(), incoming);
            ncater.notify_at(epoch.clone());
        });
    }
}

/// Integer representing the index of a worker in a cluster.
#[derive(Debug, Copy, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub(crate) struct WorkerIndex(pub(crate) usize);

/// Integer representing the number of workers in a cluster.
#[derive(Debug, Copy, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct WorkerCount(pub(crate) usize);

impl WorkerCount {
    /// Iterate through all workers in this cluster.
    pub(crate) fn iter(&self) -> impl Iterator<Item = WorkerIndex> {
        (0..self.0).map(WorkerIndex)
    }
}

#[test]
fn worker_count_iter_works() {
    let count = WorkerCount(3);
    let found: Vec<_> = count.iter().collect();
    let expected = vec![WorkerIndex(0), WorkerIndex(1), WorkerIndex(2)];
    assert_eq!(found, expected);
}

/// Extension trait for Worker-like things.
pub(crate) trait AsWorkerExt {
    /// Return our newtyped worker index.
    fn w_index(&self) -> WorkerIndex;

    /// Return our newtyped worker count.
    fn w_count(&self) -> WorkerCount;
}

impl<W> AsWorkerExt for W
where
    W: AsWorker,
{
    fn w_index(&self) -> WorkerIndex {
        WorkerIndex(self.index())
    }

    fn w_count(&self) -> WorkerCount {
        WorkerCount(self.peers())
    }
}

pub(crate) trait IntoStreamAtOp<D> {
    /// Convert this iterator into a stream, emitting all items in the
    /// given epoch.
    fn into_stream_at<S>(self, scope: &S, epoch: S::Timestamp) -> Stream<S, D>
    where
        S: Scope;
}

impl<C, I, D> IntoStreamAtOp<D> for C
where
    C: IntoIterator<Item = D, IntoIter = I> + 'static,
    I: Iterator<Item = D> + 'static,
    D: Data,
{
    fn into_stream_at<S>(self, scope: &S, epoch: S::Timestamp) -> Stream<S, D>
    where
        S: Scope,
    {
        let mut op_builder = OperatorBuilder::new(String::from("into_stream_at"), scope.clone());

        let (mut output, items) = op_builder.new_output();

        op_builder.build(move |mut init_caps| {
            let mut cap = init_caps.pop().unwrap();
            cap.downgrade(&epoch);
            let mut state = Some((cap, self));

            move |_input_frontiers| {
                // [`Option::take`] will cause this to only used once.
                if let Some((cap, self_)) = state.take() {
                    output
                        .activate()
                        .session(&cap)
                        .give_iterator(self_.into_iter());
                }
            }
        });

        items
    }
}

pub(crate) trait IntoStreamOnceAtOp<D> {
    /// Convert this iterator into a stream but only on the 0th
    /// worker, emitting all items in the given epoch.
    fn into_stream_once_at<S>(self, scope: &S, epoch: S::Timestamp) -> Stream<S, D>
    where
        S: Scope;
}

impl<I> IntoStreamOnceAtOp<I::Item> for I
where
    I: IntoIterator + 'static,
    I::Item: Data,
{
    fn into_stream_once_at<S>(self, scope: &S, epoch: S::Timestamp) -> Stream<S, I::Item>
    where
        S: Scope,
    {
        if scope.index() == 0 {
            self.into_stream_at(scope, epoch)
        } else {
            empty(scope)
        }
    }
}

pub(crate) trait IntoBroadcastOp<K>
where
    K: ExchangeData,
{
    /// Convert this iterator into a stream with items on each worker
    /// labeled with the originating worker and broadcast.
    fn into_broadcast<S>(self, scope: &S, epoch: S::Timestamp) -> Stream<S, (K, WorkerIndex)>
    where
        S: Scope;
}

impl<C, I, K> IntoBroadcastOp<K> for C
where
    C: IntoIterator<Item = K, IntoIter = I>,
    I: Iterator<Item = K> + 'static,
    K: ExchangeData,
{
    fn into_broadcast<S>(self, scope: &S, epoch: S::Timestamp) -> Stream<S, (K, WorkerIndex)>
    where
        S: Scope,
    {
        let this_worker = scope.w_index();
        self.into_iter()
            .map(move |part| (part, this_worker))
            .into_stream_at(scope, epoch)
            .broadcast()
    }
}

/// Define a way to partition keyed items into a set of partitions.
pub(crate) trait PartitionFn<K> {
    /// Determine the partition index for this key.
    ///
    /// The return value will be modulo wrapped into the total number
    /// of known partitions.
    fn assign(&self, key: &K) -> usize;
}

/// Blanket implementation for any Rust hash function.
impl<K, B> PartitionFn<K> for B
where
    B: BuildHasher,
    K: Hash,
{
    fn assign(&self, key: &K) -> usize {
        let mut hasher = self.build_hasher();
        key.hash(&mut hasher);
        hasher.finish() as usize
    }
}

pub(crate) trait PartitionOp<S, K, V>
where
    S: Scope,
    S::Timestamp: TotalOrder,
    K: Data,
    V: Data,
{
    /// Assign and label incoming key-value tuples with a partition by
    /// key.
    ///
    /// Also requires a stream of known partitions.
    ///
    /// The resulting hash value of the [`PartitionFn`] is wrapped to
    /// the total number of partitions and used as an index into them
    /// sorted.
    // TODO: Could drop returning K when we have non-linear dataflows
    // and don't need to forward output items whole.
    fn partition<P>(
        &self,
        name: String,
        known: &Stream<S, P>,
        pf: impl PartitionFn<K> + 'static,
    ) -> Stream<S, (P, (K, V))>
    where
        P: Data + Ord + Eq + Debug;
}

impl<S, K, V> PartitionOp<S, K, V> for Stream<S, (K, V)>
where
    S: Scope,
    S::Timestamp: TotalOrder,
    K: Data,
    V: Data,
{
    fn partition<P>(
        &self,
        name: String,
        known: &Stream<S, P>,
        pf: impl PartitionFn<K> + 'static,
    ) -> Stream<S, (P, (K, V))>
    where
        P: Data + Ord + Eq + Debug,
    {
        let mut op_builder = OperatorBuilder::new(name.clone(), self.scope());

        let mut items_input = op_builder.new_input(self, Pipeline);
        let mut known_input = op_builder.new_input(known, Pipeline);

        let (mut partd_output, partd_items) = op_builder.new_output();

        op_builder.build(move |init_caps| {
            let known: BTreeSet<P> = BTreeSet::new();

            let mut items_inbuf = InBuffer::new();
            let mut known_inbuf = InBuffer::new();
            let mut ncater = EagerNotificator::new(init_caps, known);

            move |input_frontiers| {
                tracing::debug_span!("operator", operator = name).in_scope(|| {
                    items_input.buffer_notify(&mut items_inbuf, &mut ncater);
                    known_input.buffer_notify(&mut known_inbuf, &mut ncater);

                    ncater.for_each(
                        input_frontiers,
                        |caps, known| {
                            let cap = &caps[0];
                            let epoch = cap.time();

                            if let Some(items) = items_inbuf.remove(epoch) {
                                assert!(!known.is_empty(), "Known partitions in {name} is empty; did you forget to broadcast initial partitions in the 0th epoch?");

                                let mut handle = partd_output.activate();
                                let mut session = handle.session(cap);
                                let len = known.len();
                                for (key, value) in items {
                                    let idx = pf.assign(&key);
                                    let wrapped_idx = idx % len;
                                    tracing::trace!("Assigner gave value {idx} % {len}; wrapped to {wrapped_idx}");
                                    let part = known
                                        .iter()
                                        .nth(wrapped_idx)
                                        .expect("hash idx was not in len of known parts")
                                        .clone();
                                    session.give((part, (key, value)));
                                }
                            }
                        },
                        |caps, known| {
                            let cap = &caps[0];
                            let epoch = cap.time();

                            if let Some(parts) = known_inbuf.remove(epoch) {
                                known.extend(parts);
                            }
                        },
                    );
                });
            }
        });

        partd_items
    }
}

/// Figure out a "primary" worker for each partition. Do this so we
/// load balance worker use.
///
/// This will only be run on worker 0, but results will be broadcast.
fn calc_primaries<P>(known: &BTreeMap<P, BTreeSet<WorkerIndex>>) -> BTreeMap<P, WorkerIndex>
where
    P: Clone + Ord + Eq,
{
    // Go through the partitions from "fewest workers have this part"
    // to "most workers have this part" in an attempt to balance
    // better.
    let known_by_count = {
        // We must use a BTreeMap so that key iteration is in a
        // deterministic order.
        let mut ps: Vec<_> = known.keys().cloned().collect();
        // An unstable sort is fine here since it is still
        // deterministic.
        ps.sort_unstable_by_key(|p| known.get(p).unwrap().len());
        ps
    };
    let mut primaries = BTreeMap::new();
    for part in known_by_count {
        // We must use a sorted [`BTreeSet`] so we can index into it.
        let mut candidates: Vec<_> = known.get(&part).unwrap().iter().copied().collect();
        // Pick the worker out of the candidates which has the fewest
        // number of partitions already assigned.
        let (_, primary_worker, _) = candidates
            // An unstable sort is fine here since it is still
            // deterministic. We don't care that we pick the lowest
            // worker index that has the fewest assigned partition
            // (which is initial sort order) just one worker
            // deterministically.
            .select_nth_unstable_by_key(0, |w1| {
                // Count the number of partitions this worker has
                // already been assigned. This means this can't be a
                // .map().collect() because we need to iteratively
                // reference the map being built.
                primaries.values().filter(|w2| *w1 == **w2).count()
            });
        primaries.insert(part, *primary_worker);
    }
    assert!(known.keys().eq(primaries.keys()));
    primaries
}

pub(crate) trait AssignPrimariesOp<S, P>
where
    S: Scope,
    S::Timestamp: TotalOrder,
    P: ExchangeData + Ord + Eq + Debug,
{
    /// Collect all known partitions from workers and at the end of
    /// the epoch, calculate the primary assignments for each
    /// partition then broadcast them.
    ///
    /// Primary assignments are "balanced": If multiple workers have
    /// the same partition, distribute which worker is the primary for
    /// that partition so we balance load on workers. E.g. if all
    /// workers have access to all partitions; make sure we spread the
    /// load so all writes don't just go to worker 1.
    ///
    /// Primary worker assignments are only emitted at the end of each
    /// epoch.
    fn assign_primaries(&self, name: String) -> Stream<S, (P, WorkerIndex)>;
}

impl<S, P> AssignPrimariesOp<S, P> for Stream<S, (P, WorkerIndex)>
where
    S: Scope,
    S::Timestamp: TotalOrder,
    P: ExchangeData + Ord + Eq + Debug,
{
    fn assign_primaries(&self, name: String) -> Stream<S, (P, WorkerIndex)> {
        // Route all data to worker 0, this means that only worker 0
        // will assign primaries. We'll broadcast them at the end of
        // this operator.
        let singular_self = self.exchange(|_| 0);

        let mut op_builder = OperatorBuilder::new(name.clone(), self.scope());

        let mut known_input = op_builder.new_input(&singular_self, Pipeline);

        let (mut routing_output, routing) = op_builder.new_output();

        op_builder.build(move |init_caps| {
            let known: BTreeMap<P, BTreeSet<WorkerIndex>> = BTreeMap::new();
            let primaries: BTreeMap<P, WorkerIndex> = BTreeMap::new();

            let mut inbuf = InBuffer::new();
            let mut ncater = EagerNotificator::new(init_caps, (known, primaries));

            move |input_frontiers| {
                tracing::debug_span!("operator", operator = name).in_scope(|| {
                    known_input.buffer_notify(&mut inbuf, &mut ncater);

                    ncater.for_each(
                        input_frontiers,
                        |caps, (known, _primaries)| {
                            let cap = &caps[0];
                            let epoch = cap.time();

                            if let Some(routes) = inbuf.remove(epoch) {
                                for (key, worker) in routes {
                                    tracing::trace!("{worker:?} has {key:?} at epoch {epoch:?}");
                                    known.entry(key).or_default().insert(worker);
                                }
                            }
                        },
                        |caps, (known, old_primaries)| {
                            let cap = &caps[0];
                            let epoch = cap.time();

                            let new_primaries = calc_primaries(known);
                            if new_primaries.is_empty() {
                                panic!("No partitions found on any worker; did you forget to init them?");
                            }

                            let mut handle = routing_output.activate();
                            let mut session = handle.session(cap);
                            // Send downstream any updated assignments.
                            for (key, worker) in &new_primaries {
                                let worker = *worker;
                                if old_primaries.remove(key) != Some(worker) {
                                    tracing::trace!(
                                        "{worker:?} is primary for {key:?} at epoch {epoch:?}"
                                    );
                                    session.give((key.clone(), worker));
                                }
                            }

                            let _drop_old = std::mem::replace(old_primaries, new_primaries);
                        },
                    );
                });
            }
        });

        routing.broadcast()
    }
}

pub(crate) trait RouteOp<S, K, V>
where
    S: Scope,
    S::Timestamp: TotalOrder,
    K: Data + Hash + Eq + Debug,
    V: Data,
{
    /// Label a stream of key-value pairs with what worker is primary
    /// for that key. This also requires a stream of the primary
    /// assignments.
    ///
    /// The routing table can not be part of another operator's
    /// exchange function because we need to synchronize routing table
    /// updates to each item's epoch.
    ///
    /// It does not work to apply routing updates in the same epoch as
    /// the items come in because then you could get non-deterministic
    /// routing if a worker assignment changes.
    fn route(
        &self,
        name: String,
        routing: &Stream<S, (K, WorkerIndex)>,
    ) -> Stream<S, (WorkerIndex, (K, V))>;
}

impl<S, K, V> RouteOp<S, K, V> for Stream<S, (K, V)>
where
    S: Scope,
    S::Timestamp: TotalOrder,
    K: Data + Hash + Eq + Debug,
    V: Data,
{
    fn route(
        &self,
        name: String,
        routing: &Stream<S, (K, WorkerIndex)>,
    ) -> Stream<S, (WorkerIndex, (K, V))> {
        let mut op_builder = OperatorBuilder::new(name.clone(), self.scope());

        let mut items_input = op_builder.new_input(self, Pipeline);
        let mut routing_input = op_builder.new_input(routing, Pipeline);

        let (mut routed_output, routed_items) = op_builder.new_output();

        op_builder.build(move |init_caps| {
            let routing_map: HashMap<K, WorkerIndex> = HashMap::new();

            let mut items_inbuf = InBuffer::new();
            let mut routing_inbuf = InBuffer::new();
            let mut ncater = EagerNotificator::new(init_caps, routing_map);

            move |input_frontiers| {
                tracing::debug_span!("operator", operator = name).in_scope(|| {
                items_input.buffer_notify(&mut items_inbuf, &mut ncater);
                routing_input.buffer_notify(&mut routing_inbuf, &mut ncater);

                ncater.for_each(
                    input_frontiers,
                    |caps, routing_map| {
                        let cap = &caps[0];
                        let epoch = cap.time();

                        if let Some(items) = items_inbuf.remove(epoch) {
                            assert!(!routing_map.is_empty(), "Routing map is empty; did you forget to broadcast initial routes in the 0th epoch?");

                            let mut handle = routed_output.activate();
                            let mut session = handle.session(cap);
                            for key_value in items {
                                let (key, _value) = &key_value;
                                let worker = *routing_map.get(key).unwrap_or_else(|| {
                                    panic!("Key {key:?} is not in this worker's routing map; known keys are {:?}", routing_map.keys());
                                });
                                session.give((worker, key_value));
                            }
                        }
                    },
                    |caps, routing_map| {
                        let cap = &caps[0];
                        let epoch = cap.time();

                        if let Some(routes) = routing_inbuf.remove(epoch) {
                            for (part, worker) in routes {
                                routing_map.insert(part, worker);
                            }
                        }
                    },
                );
                });
            }
        });

        routed_items
    }
}

/// Build a [`ParallelizationContract`] which routes items to the
/// worker explicitly specified in the tuple of each item.
pub(crate) fn routed_exchange<T, D>() -> impl ParallelizationContract<T, (WorkerIndex, D)>
where
    T: Timestamp,
    D: ExchangeData,
{
    Exchange::<(WorkerIndex, D), _>::new(|(worker, _)| worker.0 as u64)
}

/// Defines a writing component.
pub(crate) trait Writer {
    /// Item type to be able to write.
    type Item;

    /// Write a batch of items.
    fn write_batch(&mut self, items: Vec<Self::Item>);
}

pub(crate) trait PartitionedWriteOp<S, K, V>
where
    S: Scope,
    S::Timestamp: TotalOrder,
    K: ExchangeData + Debug,
    V: ExchangeData,
{
    /// Write a stream of key-value tuples to a distributed
    /// partitioned output. This requires each worker to know what
    /// partitions are "local" (and could be written to), a partition
    /// function which assigns pairs to partitions, and a builder
    /// function which knows how to build a partition on this worker
    /// if it is assigned to be primary for that partition.
    ///
    /// Outputs downstream a clock so you can monitor what epoch's
    /// items have been fully written.
    ///
    /// This can't be unified into the output system operators because
    /// they are either stateful (and we'd have a circular dependency
    /// on the state managment) or unpartitioned (and we don't need
    /// the partitioned magic at all).
    fn partd_write<P, W>(
        &self,
        name: String,
        local_parts: Vec<P>,
        pf: impl PartitionFn<K> + 'static,
        builder: impl FnMut(&P) -> W + 'static,
    ) -> ClockStream<S>
    where
        P: ExchangeData + Hash + Ord + Eq + Debug + Display,
        W: Writer<Item = V> + 'static;
}

impl<S, K, V> PartitionedWriteOp<S, K, V> for Stream<S, (K, V)>
where
    S: Scope,
    S::Timestamp: TotalOrder,
    K: ExchangeData + Debug,
    V: ExchangeData,
{
    fn partd_write<P, W>(
        &self,
        name: String,
        local_parts: Vec<P>,
        pf: impl PartitionFn<K> + 'static,
        mut builder: impl FnMut(&P) -> W + 'static,
    ) -> ClockStream<S>
    where
        P: ExchangeData + Hash + Ord + Eq + Debug + Display,
        W: Writer<Item = V> + 'static,
    {
        let this_worker = self.scope().w_index();

        let meter = global::meter("dataflow");
        let histogram = meter
            .f64_histogram("partd_write_duration_seconds")
            .with_description("partitioned state write duration in seconds")
            .init();
        let worker_label = KeyValue::new("worker_id", this_worker.0.to_string());
        // Create a map of metric labels to use for each part_id
        let part_label_map: HashMap<P, Vec<KeyValue>> = local_parts
            .iter()
            .map(|part_key| {
                (
                    part_key.clone(),
                    vec![
                        worker_label.clone(),
                        KeyValue::new("part_id", part_key.to_string()),
                    ],
                )
            })
            .collect();

        let all_parts = local_parts.into_broadcast(&self.scope(), S::Timestamp::minimum());
        let primary_updates = all_parts.assign_primaries(format!("{name}.assign_primaries"));

        let routed_self = self
            .partition(
                format!("{name}.partition"),
                &all_parts.map(|(part, _worker)| part),
                pf,
            )
            .route(format!("{name}.route"), &primary_updates);

        let op_name = format!("{name}.partd_write");
        let mut op_builder = OperatorBuilder::new(op_name.clone(), self.scope());

        let mut routed_input = op_builder.new_input(&routed_self, routed_exchange());
        let mut primaries_input = op_builder.new_input(&primary_updates, Pipeline);

        let (mut clock_output, clock) = op_builder.new_output();

        op_builder.build(move |init_caps| {
            let parts: BTreeMap<P, W> = BTreeMap::new();

            let mut routed_tmp = Vec::new();
            let mut items_inbuf: BTreeMap<S::Timestamp, BTreeMap<P, Vec<V>>> = BTreeMap::new();
            let mut primaries_inbuf = InBuffer::new();
            let mut ncater = EagerNotificator::new(init_caps, parts);

            move |input_frontiers| {
                tracing::debug_span!("operator", operator = op_name).in_scope(|| {
                routed_input.for_each(|cap, incoming| {
                    let epoch = cap.time();
                    assert!(routed_tmp.is_empty());
                    incoming.swap(&mut routed_tmp);
                    for (_worker, (part, (_key, value))) in routed_tmp.drain(..) {
                        items_inbuf.entry(epoch.clone())
                            .or_insert_with(BTreeMap::new)
                            .entry(part)
                            .or_insert_with(Vec::new)
                            .push(value);
                    }

                    ncater.notify_at(epoch.clone());
                });
                primaries_input.buffer_notify(&mut primaries_inbuf, &mut ncater);

                ncater.for_each(
                    input_frontiers,
                    |caps, parts| {
                        let cap = &caps[0];
                        let epoch = cap.time();

                        // Writing happens eagerly in each epoch. We
                        // still use a notificator at all because we
                        // need to ensure that writes happen in epoch
                        // order.
                        if let Some(part_to_items) = items_inbuf.remove(epoch) {
                            let known_parts: Vec<_> = parts.keys().cloned().collect();
                            for (part_key, items) in part_to_items {
                                let part = parts
                                    .get_mut(&part_key)
                                    .unwrap_or_else(|| {
                                        panic!("Items routed to partition {part_key} but this worker only has {known_parts:?}");
                                    });

                                let labels = part_label_map
                                    .get(&part_key)
                                    .expect("No metric labels found for part key {part_key}");
                                with_timer!(histogram, labels, part.write_batch(items));
                            }
                        }
                    },
                    |caps, parts| {
                        let cap = &caps[0];
                        let epoch = cap.time();

                        if let Some(primaries) = primaries_inbuf.remove(epoch) {
                            // If this worker was assigned to be
                            // primary for a partition, build it.
                            for (part_key, worker) in primaries {
                                if worker == this_worker && !parts.contains_key(&part_key) {
                                    let part = builder(&part_key);
                                    parts.insert(part_key, part);
                                } else {
                                    parts.remove(&part_key);
                                }
                            }
                        }

                        // Emit our progress clock.
                        clock_output.activate().session(cap).give(());
                    },
                );
                });
            }
        });

        clock
    }
}

/// Defines a loading component.
pub(crate) trait BatchIterator {
    /// Item type to read.
    type Item;

    /// Return the next batch of items.
    ///
    /// Batch size must be tuned to ensure we play nicely with
    /// Timely's cooperative multi-tasking.
    ///
    /// If this returns [`None`], then EOF and should not be called
    /// again.
    fn next_batch(&mut self) -> Option<Vec<Self::Item>>;
}

pub(crate) trait PartitionedLoadOp<S>
where
    S: Scope,
    S::Timestamp: TotalOrder,
{
    /// Load a stream of items from a distributed partitioned
    /// loader. This requires each worker to know what partitions are
    /// "local" (and could be read from), and a builder function which
    /// knows how to build a partition on this worker if it is
    /// assigned to be primary for that partition.
    ///
    /// Load differs from what we call "reader" in the input module in
    /// that it dumps all items into a single epoch.
    ///
    /// Will distribute reading of partitions across the whole cluster
    /// based on what workers have access to them. Reads all data in
    /// the desired epoch.
    ///
    /// Readers must have all data ready. Don't use this operator with
    /// possibly delayed data as it will not advance the epoch if data
    /// is "slow". Thus downstream consumers of this data must process
    /// it in the activation it is received because there is no
    /// backpressure because all data from a partition is emitted in
    /// the same epoch. It's sort of hard to write an operator that
    /// doesn't do that, though.
    fn partd_load<P, L>(
        &mut self,
        name: String,
        local_parts: Vec<P>,
        builder: impl FnMut(&P) -> L + 'static,
        epoch: S::Timestamp,
    ) -> Stream<S, L::Item>
    where
        P: ExchangeData + Hash + Ord + Eq + Debug + Display,
        L: BatchIterator + 'static,
        L::Item: Data;
}

/// Bundle together the operator state for loading a single partition.
struct LoadPartEntry<T, P, L>
where
    T: Timestamp,
{
    cap: Capability<T>,
    key: P,
    part: L,
}

impl<T, P, L> PartialEq for LoadPartEntry<T, P, L>
where
    T: Timestamp,
{
    fn eq(&self, other: &Self) -> bool {
        *self.cap.time() == *other.cap.time()
    }
}

impl<T, P, L> Eq for LoadPartEntry<T, P, L> where T: Timestamp {}

impl<T, P, L> PartialOrd for LoadPartEntry<T, P, L>
where
    T: Timestamp,
{
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        self.cap.time().partial_cmp(other.cap.time())
    }
}

impl<T, P, L> Ord for LoadPartEntry<T, P, L>
where
    T: Timestamp + TotalOrder,
{
    fn cmp(&self, other: &Self) -> Ordering {
        self.cap.time().cmp(other.cap.time())
    }
}

impl<S> PartitionedLoadOp<S> for S
where
    S: Scope,
    S::Timestamp: TotalOrder,
{
    fn partd_load<P, L>(
        &mut self,
        name: String,
        local_parts: Vec<P>,
        mut builder: impl FnMut(&P) -> L + 'static,
        epoch: S::Timestamp,
    ) -> Stream<S, L::Item>
    where
        P: ExchangeData + Hash + Ord + Eq + Debug + Display,
        L: BatchIterator + 'static,
        L::Item: Data,
    {
        let this_worker = self.w_index();

        let meter = global::meter("dataflow");
        let histogram = meter
            .f64_histogram("partd_load_builder_duration_seconds")
            .with_description("partitioned state load duration in seconds")
            .init();
        let worker_label = KeyValue::new("worker_id", this_worker.0.to_string());
        // Create a map of metric labels to use for each part_id
        let part_label_map: HashMap<P, Vec<KeyValue>> = local_parts
            .iter()
            .map(|part_key| {
                (
                    part_key.clone(),
                    vec![
                        worker_label.clone(),
                        KeyValue::new("name", name.to_string()),
                        KeyValue::new("part_id", part_key.to_string()),
                    ],
                )
            })
            .collect();

        // This will emit the routing table in the 0th epoch. The
        // below code reads all of a partition in the epoch it is
        // assigned a primary, so it'll also be the 0th epoch.
        let primary_updates = local_parts
            .into_broadcast(self, epoch)
            .assign_primaries(format!("{name}.assign_primaries"));

        let op_name = format!("{name}.partd_load");
        let mut op_builder = OperatorBuilder::new(op_name.clone(), self.clone());

        let mut primaries_input = op_builder.new_input(&primary_updates, Pipeline);

        let (mut items_output, output) = op_builder.new_output();

        let info = op_builder.operator_info();
        let activator = self.activator_for(&info.address[..]);
        let cooldown = Duration::from_millis(1);

        // We can't use a notificator here because reading from a
        // partition must live across activations since we need to
        // yield back to Timely to not dump everything into its
        // queues. Notificators don't have a way to "hold onto" a
        // capability after the epoch is marked as closed once.
        op_builder.build(move |_init_caps| {
            let mut inbuf = Vec::new();

            let mut queue: BinaryHeap<LoadPartEntry<S::Timestamp, P, L>> = BinaryHeap::new();

            move |_input_frontiers| {
                tracing::debug_span!("operator", operator = op_name).in_scope(|| {
                    let mut just_emitted = false;

                    primaries_input.for_each(|cap, incoming| {
                        let epoch = cap.time();
                        assert!(inbuf.is_empty());
                        incoming.swap(&mut inbuf);

                        for (part_key, worker) in inbuf.drain(..) {
                            if worker == this_worker {
                                let labels = part_label_map
                                    .get(&part_key)
                                    .expect("No metric labels found for part key {part_key}");
                                let part = with_timer!(histogram, labels, builder(&part_key));
                                tracing::debug!("Init-ing {part_key} at epoch {epoch:?}");
                                let entry = LoadPartEntry {
                                    cap: cap.delayed(epoch),
                                    key: part_key,
                                    part,
                                };
                                queue.push(entry);
                            } else {
                                // Because reading happens all in one
                                // epoch, it doesn't make sense to "move"
                                // a partition if re-assigned.
                            }
                        }
                    });

                    // Read partitions in epoch order so downstream things
                    // hopefully do not have to queue due to frontiers
                    // being open.
                    let mut eof = false;
                    if let Some(mut entry) = queue.peek_mut() {
                        tracing::trace_span!("partition", part_key = ?entry.key).in_scope(|| {
                            if let Some(mut items) = entry.part.next_batch() {
                                if !items.is_empty() {
                                    just_emitted = true;
                                }

                                items_output
                                    .activate()
                                    .session(&entry.cap)
                                    .give_vec(&mut items);

                                // This part isn't EOF, so leave it.
                            } else {
                                // If the part is EOF, drop it.
                                eof = true;
                                tracing::debug!("EOFd");
                            }
                        });
                    }
                    if eof {
                        queue.pop();
                    }

                    if !queue.is_empty() {
                        if just_emitted {
                            activator.activate();
                        } else {
                            activator.activate_after(cooldown);
                        }
                    }
                });
            }
        });

        output
    }
}

/// Defines a committing component.
pub(crate) trait Committer<T> {
    /// Commit that an epoch will never be processed again, even
    /// across failure and resume.
    fn commit(&mut self, epoch: &T);
}

pub(crate) trait PartitionedCommitOp<S>
where
    S: Scope,
    S::Timestamp: TotalOrder + CheckedSub,
{
    /// Commit epochs as they are closed from a distributed
    /// partitioned committer. This requires each worker to know what
    /// partitions are "local" (and could be read from), and a builder
    /// function which knows how to build a partition on this worker
    /// if it is assigned to be primary for that partition.
    ///
    /// "Committing" could mean GCing the recovery store, but it could
    /// also mean ACKing input messages or other things.
    ///
    /// A delay can be specified so the epoch reported as committed is
    /// actually a certain amount behind the frontier epoch of the
    /// input.
    fn partd_commit<P, L>(
        &self,
        name: String,
        local_parts: Vec<P>,
        builder: impl FnMut(&P) -> L + 'static,
        delay: S::Timestamp,
    ) -> ClockStream<S>
    where
        P: ExchangeData + Ord + Eq + Debug,
        L: Committer<S::Timestamp> + 'static;
}

impl<S> PartitionedCommitOp<S> for ClockStream<S>
where
    S: Scope,
    S::Timestamp: TotalOrder + CheckedSub,
{
    fn partd_commit<P, L>(
        &self,
        name: String,
        local_parts: Vec<P>,
        mut builder: impl FnMut(&P) -> L + 'static,
        delay: S::Timestamp,
    ) -> ClockStream<S>
    where
        P: ExchangeData + Ord + Eq + Debug,
        L: Committer<S::Timestamp> + 'static,
    {
        let this_worker = self.scope().w_index();

        let primary_updates = local_parts
            .into_broadcast(&self.scope(), S::Timestamp::minimum())
            .assign_primaries(format!("{name}.assign_primaries"));

        let op_name = format!("{name}.partd_commit");
        let mut op_builder = OperatorBuilder::new(op_name.clone(), self.scope());

        let mut clock_input = op_builder.new_input(self, Pipeline);
        let mut primaries_input = op_builder.new_input(&primary_updates, Pipeline);

        let (mut clock_output, clock) = op_builder.new_output();

        op_builder.build(move |init_caps| {
            let parts: BTreeMap<P, L> = BTreeMap::new();

            let mut clock_inbuf = InBuffer::new();
            let mut primaries_inbuf = InBuffer::new();
            let mut ncater = EagerNotificator::new(init_caps, parts);

            move |input_frontiers| {
                tracing::debug_span!("operator", operator = op_name).in_scope(|| {
                    clock_input.buffer_notify(&mut clock_inbuf, &mut ncater);
                    primaries_input.buffer_notify(&mut primaries_inbuf, &mut ncater);

                    ncater.for_each(
                        input_frontiers,
                        |_caps, _parts| {},
                        |caps, parts| {
                            let cap = &caps[0];
                            let epoch = cap.time();

                            // We don't use the incoming ticks.
                            clock_inbuf.remove(epoch);
                            // Call commit before updating the partitions
                            // in order to mirror the semantics of the
                            // write operator.
                            if let Some(delayed) = epoch.checked_sub(&delay) {
                                for (part_key, part) in parts.iter_mut() {
                                    tracing::trace_span!("partition", part_key = ?part_key)
                                        .in_scope(|| {
                                            part.commit(&delayed);
                                        });
                                }
                            }

                            if let Some(primaries) = primaries_inbuf.remove(epoch) {
                                // If this worker was assigned to be
                                // primary for a partition, build it.
                                for (part_key, worker) in primaries {
                                    if worker == this_worker && !parts.contains_key(&part_key) {
                                        let part = builder(&part_key);
                                        parts.insert(part_key, part);
                                    } else {
                                        parts.remove(&part_key);
                                    }
                                }
                            }

                            // Emit our progress clock.
                            clock_output.activate().session(&cap).give(());
                        },
                    );
                });
            }
        });

        clock
    }
}
