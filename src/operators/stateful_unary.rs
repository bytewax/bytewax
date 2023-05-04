//! This is the most primitive stateful operator for non-input cases.
//!
//! To derive a new stateful operator from this, create a new
//! [`StatefulLogic`] impl and pass it to the [`StatefulUnary`] Timely
//! operator. If you fullfil the API of [`StatefulLogic`], you will
//! get proper recovery behavior.
//!
//! The general idea is that you pass a **logic builder** which takes
//! any previous state snapshots from the last execution and builds an
//! instance of your logic. Then your logic is **snapshotted** at the
//! end of each epoch, and that state durably saved in the recovery
//! store.

use std::{
    collections::{BTreeSet, HashMap, HashSet},
    task::Poll,
    time::Duration,
};

use chrono::{DateTime, Utc};
use timely::{
    dataflow::{channels::pact::Exchange, operators::generic::builder_rc::OperatorBuilder, Scope},
    progress::Antichain,
    Data, ExchangeData,
};

use crate::{
    recovery::model::*,
    timely::{CapabilityVecEx, FrontierEx},
};
use crate::{recovery::operators::Route, timely::InBuffer};

// Re-export for convenience. If you want to write a stateful
// operator, just use * this module.

pub(crate) use crate::recovery::model::StateBytes;
pub(crate) use crate::recovery::model::StepId;
pub(crate) use crate::recovery::model::StepStateBytes;
pub(crate) use crate::recovery::operators::FlowChangeStream;
pub(crate) use crate::recovery::operators::StatefulStream;

/// If a [`StatefulLogic`] for a key should be retained by
/// [`StatefulUnary::stateful_unary`].
///
/// See [`StatefulLogic::fate`].
pub(crate) enum LogicFate {
    /// This logic for this key should be retained and used again
    /// whenever new data for this key comes in.
    Retain,
    /// The logic for this key is "complete" and should be
    /// discarded. It will be built again if the key is encountered
    /// again.
    Discard,
}

/// Impl this trait to create an operator which maintains recoverable
/// state.
///
/// Pass a builder of this to [`StatefulUnary::stateful_unary`] to
/// create the Timely operator. A separate instance of this will be
/// created for each key in the input stream. There is no way to
/// interact across keys.
pub(crate) trait StatefulLogic<V, R, I>
where
    V: Data,
    I: IntoIterator<Item = R>,
{
    /// Logic to run when this operator is awoken.
    ///
    /// `next_value` has the same semantics as
    /// [`std::async_iter::AsyncIterator::poll_next`]:
    ///
    /// - [`Poll::Pending`]: no new values ready yet. We were probably
    ///   awoken because of a timeout.
    ///
    /// - [`Poll::Ready`] with a [`Some`]: a new value has arrived.
    ///
    /// - [`Poll::Ready`] with a [`None`]: the stream has ended and
    ///   logic will not be called again.
    ///
    /// This must return values to be emitted downstream.
    fn on_awake(&mut self, next_value: Poll<Option<V>>) -> I;

    /// Called when [`StatefulUnary::stateful_unary`] is deciding if
    /// the logic for this key is still relevant.
    ///
    /// Since [`StatefulUnary::stateful_unary`] owns this logic, we
    /// need a way to communicate back up wheither it should be
    /// retained.
    ///
    /// This will be called after each awakening.
    fn fate(&self) -> LogicFate;

    /// Return the next system time this operator should be awoken at,
    /// if any.
    ///
    /// This will be called after each awakening.
    ///
    /// Any previously recorded awake times are forgotten after each
    /// call. The logic internally needs to keep track of multiple
    /// awake times (if it needs that) and keep returning the next
    /// one.
    fn next_awake(&self) -> Option<DateTime<Utc>>;

    /// Snapshot the internal state of this operator.
    ///
    /// Serialize any and all state necessary to re-construct the
    /// operator exactly how it currently is in the
    /// [`StatefulUnary::stateful_unary`]'s `logic_builder`.
    ///
    /// This will be called at the end of each epoch.
    fn snapshot(&self) -> StateBytes;
}

/// Extension trait for [`Stream`].
// Based on the good work in
// https://github.com/TimelyDataflow/timely-dataflow/blob/0d0d84885672d6369a78cd9aff7beb2048390d3b/timely/src/dataflow/operators/aggregation/state_machine.rs#L57
pub(crate) trait StatefulUnary<S, V>
where
    S: Scope,
    V: ExchangeData,
{
    /// Create a new generic stateful operator.
    ///
    /// This is the core Timely operator that all Bytewax stateful
    /// operators are implemented in terms of. It is awkwardly generic
    /// because of that. We do this so we only have to implement the
    /// very tricky recovery system interop once.
    ///
    /// # Input
    ///
    /// The input must be a stream of `(key, value)` 2-tuples. They
    /// will automatically be routed to the same worker and logic
    /// instance by key.
    ///
    /// # Logic Builder
    ///
    /// This is a closure which should build a new instance of your
    /// logic for a key, given the last snapshot of its state for that
    /// key. You should implement the deserialization from
    /// [`StateBytes`] in this builder; it should be the reverse of
    /// your [`StatefulLogic::snapshot`].
    ///
    /// See [`StatefulLogic`] for the semantics of the logic.
    ///
    /// This will be called periodically as new keys are encountered
    /// and the first time a key is seen during a resume.
    ///
    /// # Output
    ///
    /// The output will be a stream of `(key, value)` 2-tuples. Values
    /// emitted by [`StatefulLogic::awake_with`] will be automatically
    /// paired with the key in the output stream.
    fn stateful_unary<R, I, L, LB>(
        &self,
        step_id: StepId,
        logic_builder: LB,
        resume_epoch: ResumeEpoch,
        resume_state: StepStateBytes,
    ) -> (StatefulStream<S, R>, FlowChangeStream<S>)
    where
        R: Data,                                   // Output value type
        I: IntoIterator<Item = R>,                 // Iterator of output values
        L: StatefulLogic<V, R, I> + 'static,       // Logic
        LB: Fn(Option<StateBytes>) -> L + 'static  // Logic builder
    ;
}

impl<S, V> StatefulUnary<S, V> for StatefulStream<S, V>
where
    S: Scope<Timestamp = u64>,
    V: ExchangeData, // Input value type
{
    fn stateful_unary<R, I, L, LB>(
        &self,
        step_id: StepId,
        logic_builder: LB,
        resume_epoch: ResumeEpoch,
        resume_state: StepStateBytes,
    ) -> (StatefulStream<S, R>, FlowChangeStream<S>)
    where
        R: Data,                                   // Output value type
        I: IntoIterator<Item = R>,                 // Iterator of output values
        L: StatefulLogic<V, R, I> + 'static,       // Logic
        LB: Fn(Option<StateBytes>) -> L + 'static, // Logic builder
    {
        let mut op_builder = OperatorBuilder::new(step_id.0.to_string(), self.scope());

        let (mut output_wrapper, output_stream) = op_builder.new_output();
        let (mut change_wrapper, change_stream) = op_builder.new_output();

        let mut input_handle = op_builder.new_input_connection(
            self,
            Exchange::new(move |(key, _value): &(StateKey, V)| key.route()),
            // This is saying this input results in items on either
            // output.
            vec![Antichain::from_elem(0), Antichain::from_elem(0)],
            // TODO: Figure out the magic incantation of
            // S::Timestamp::minimum() and S::Timestamp::Summary that
            // lets you do this without the trait bound S:
            // Scope<Timestamp = u64> above.
        );

        let info = op_builder.operator_info();
        let activator = self.scope().activator_for(&info.address[..]);
        let cooldown = Duration::from_millis(1);

        op_builder.build(move |mut init_caps| {
            // Since we might emit downstream without any incoming
            // items, like on window timeout, ensure we FFWD to the
            // resume epoch.
            init_caps.downgrade_all(&resume_epoch.0);
            // We have to retain separate capabilities
            // per-output. This seems to be only documented in
            // https://github.com/TimelyDataflow/timely-dataflow/pull/187
            // In reverse order because of how [`Vec::pop`] removes
            // from back.
            let mut change_cap = init_caps.pop();
            let mut output_cap = init_caps.pop();

            // Logic struct for each key. There is only a single logic
            // for each key representing the state at the frontier
            // epoch; we only modify state carefully in epoch order
            // once we know we won't be getting any input on closed
            // epochs.
            let mut current_logic: HashMap<StateKey, L> = HashMap::new();
            // Next awaken timestamp for each key. There is only a
            // single awake time for each key, representing the next
            // awake time.
            let mut current_next_awake: HashMap<StateKey, DateTime<Utc>> = HashMap::new();

            for (key, snapshot) in resume_state {
                // TODO: How do we double check that resume state was
                // routed correctly? We don't have access to how
                // [`Exchange`] is converted to worker index, so we
                // can't even write this:

                // assert!(key.route() == worker_index);

                let state = StateBytes::de::<(StateBytes, Option<DateTime<Utc>>)>(snapshot);
                let (logic_snapshot, next_awake) = state;
                current_logic.insert(key.clone(), logic_builder(Some(logic_snapshot)));
                if let Some(next_awake) = next_awake {
                    current_next_awake.insert(key.clone(), next_awake);
                } else {
                    current_next_awake.remove(&key);
                }
            }

            // Here we have "buffers" that store items across
            // activations.

            // Persistent across activations buffer keeping track of
            // out-of-order inputs. Push in here when Timely says we
            // have new data; pull out of here in epoch order to
            // process. This spans activations and will have epochs
            // removed from it as the input frontier progresses.
            let mut inbuf = InBuffer::new();
            // Persistent across activations buffer of what keys were
            // awoken during the most recent epoch. This is used to
            // only snapshot state of keys that could have resulted in
            // state modifications. This is drained after each epoch
            // is processed.
            let mut awoken_keys_buffer: HashSet<StateKey> = HashSet::new();

            // Here are some temporary working sets that we allocate
            // once, then drain and re-use each activation of this
            // operator.

            // Temp ordered set of epochs that can be processed
            // because all their input has been finalized or it's the
            // frontier epoch. This is filled from buffered data and
            // drained and re-used each activation of this operator.
            let mut tmp_closed_epochs: BTreeSet<S::Timestamp> = BTreeSet::new();
            // Temp list of `(StateKey, Poll<Option<V>>)` to awake the
            // operator logic within each epoch. This is drained and
            // re-used each activation of this operator.
            let mut tmp_awake_logic_with: Vec<(StateKey, Poll<Option<V>>)> = Vec::new();

            move |input_frontiers| {
                if let (Some(output_cap), Some(state_update_cap)) =
                    (output_cap.as_mut(), change_cap.as_mut())
                {
                    assert!(output_cap.time() == state_update_cap.time());
                    assert!(tmp_closed_epochs.is_empty());
                    assert!(tmp_awake_logic_with.is_empty());
                    // Do not assert awoken_keys_buffer is empty,
                    // because we might have worked on the current
                    // epoch in the last activation.

                    let now = chrono::offset::Utc::now();

                    // Buffer the inputs so we can apply them to the
                    // state cache in epoch order.
                    input_handle.for_each(|cap, incoming| {
                        let epoch = cap.time();
                        inbuf.extend(*epoch, incoming);
                    });

                    let last_output_epoch = *output_cap.time();
                    let frontier_epoch = input_frontiers
                        .simplify()
                        // If we're at EOF and there's no "current
                        // epoch", use the last seen epoch to still
                        // allow output. EagerNotificator does not
                        // allow this.
                        .unwrap_or(last_output_epoch);

                    // Now let's find out which epochs we should wake
                    // up the logic for.

                    // On the last activation, we eagerly executed the
                    // frontier at that time (which may or may not
                    // still be the frontier), even though it wasn't
                    // closed. Thus, we haven't run the "epoch closed"
                    // code yet. Make sure that close code is run if
                    // that epoch is now closed on this activation.
                    if input_frontiers.is_closed(&last_output_epoch) {
                        tmp_closed_epochs.insert(last_output_epoch);
                    }
                    // Try to process all the epochs we have input
                    // for. Filter out epochs that are not closed; the
                    // state at the beginning of those epochs are not
                    // truly known yet, so we can't apply input in
                    // those epochs yet.
                    tmp_closed_epochs
                        .extend(inbuf.epochs().filter(|e| input_frontiers.is_closed(e)));
                    // Eagerly execute the current frontier (even
                    // though it's not closed) as long as it could
                    // actually get data. All inputs will have a flash
                    // of their frontier being 0 before the resume
                    // epoch.
                    if frontier_epoch >= resume_epoch.0 {
                        tmp_closed_epochs.insert(frontier_epoch);
                    }

                    // For each epoch in order. This drains
                    // tmp_closed_epochs to be re-used on next
                    // activation.
                    while let Some(epoch) = tmp_closed_epochs.pop_first() {
                        // Since the frontier has advanced to at least
                        // this epoch (because we're going through
                        // them in order), say that we'll not be
                        // sending output at any older epochs. This
                        // also asserts "apply changes in epoch order"
                        // to the state cache.
                        output_cap.downgrade(&epoch);
                        state_update_cap.downgrade(&epoch);

                        // Now let's find all the key-value pairs to
                        // awaken logic with.

                        // Include all the incoming data.
                        if let Some(incoming_state_key_values) = inbuf.remove(&epoch) {
                            tmp_awake_logic_with.extend(
                                incoming_state_key_values
                                    .into_iter()
                                    .map(|(state_key, value)| {
                                        (state_key, Poll::Ready(Some(value)))
                                    }),
                            );
                        }

                        // Then extend the values with any "awake"
                        // activations after the input.
                        if input_frontiers.is_eof() {
                            // If this is the last activation, signal
                            // that all keys have
                            // terminated. Repurpose
                            // [`awoken_keys_buffer`] because it
                            // contains outstanding keys from the last
                            // activation. It's ok that we drain it
                            // because those keys will be re-inserted
                            // due to the EOF items.

                            // First all "new" keys in this input.
                            awoken_keys_buffer.extend(
                                tmp_awake_logic_with
                                    .iter()
                                    .map(|(state_key, _value)| state_key)
                                    .cloned(),
                            );
                            // Then all keys that are still waiting on
                            // awakening. Keys that do not have a
                            // pending awakening will not see EOF
                            // messages (otherwise we'd have to retain
                            // data for all keys ever seen).
                            awoken_keys_buffer.extend(current_next_awake.keys().cloned());
                            // Since this is EOF, we will never
                            // activate this operator again.
                            tmp_awake_logic_with.extend(
                                awoken_keys_buffer
                                    .drain()
                                    .map(|state_key| (state_key, Poll::Ready(None))),
                            );
                        } else {
                            // Otherwise, wake up any keys that are
                            // past their requested awakening time.
                            tmp_awake_logic_with.extend(
                                current_next_awake
                                    .iter()
                                    .filter(|(_state_key, next_awake)| **next_awake <= now)
                                    .map(|(state_key, _next_awake)| {
                                        (state_key.clone(), Poll::Pending)
                                    }),
                            );
                        }

                        let mut output_handle = output_wrapper.activate();
                        let mut output_session = output_handle.session(&output_cap);

                        // Drain to re-use allocation.
                        for (key, next_value) in tmp_awake_logic_with.drain(..) {
                            // Ok, let's actually run the logic code!
                            // Pull out or build the logic for the
                            // current key.
                            let mut logic = current_logic
                                .remove(&key)
                                .unwrap_or_else(|| logic_builder(None));
                            let output = logic.on_awake(next_value);
                            output_session
                                .give_iterator(output.into_iter().map(|item| (key.clone(), item)));

                            // Figure out if we should discard it.
                            let fate = logic.fate();
                            match fate {
                                LogicFate::Discard => {
                                    // Remove any pending awake times,
                                    // since that's part of the state.
                                    current_next_awake.remove(&key);

                                    // Do not re-insert the
                                    // logic. It'll be dropped.
                                }
                                LogicFate::Retain => {
                                    // If we don't discard it, ask
                                    // when to wake up next and
                                    // overwrite that.
                                    if let Some(next_awake) = logic.next_awake() {
                                        current_next_awake.insert(key.clone(), next_awake);
                                    } else {
                                        current_next_awake.remove(&key);
                                    }

                                    current_logic.insert(key.clone(), logic);
                                }
                            };

                            awoken_keys_buffer.insert(key);
                        }

                        // Determine the fate of each key's logic at
                        // the end of each epoch. If a key wasn't
                        // awoken, then there's no state change so
                        // ignore it here. Snapshot and output state
                        // changes. Remove will ensure we slowly drain
                        // the buffer.
                        if input_frontiers.is_closed(&epoch) {
                            let mut change_handle = change_wrapper.activate();
                            let mut change_session = change_handle.session(&state_update_cap);

                            // Go through all keys awoken in this
                            // epoch. This might involve keys from the
                            // previous activation.
                            for state_key in awoken_keys_buffer.drain() {
                                // Now snapshot the logic and next
                                // awake at value, if any.
                                let change = if let Some(logic) = current_logic.get(&state_key) {
                                    let logic_snapshot = logic.snapshot();
                                    let next_awake = current_next_awake.get(&state_key).cloned();
                                    let state = (logic_snapshot, next_awake);
                                    let snapshot = StateBytes::ser::<(
                                        StateBytes,
                                        Option<DateTime<Utc>>,
                                    )>(&state);
                                    Change::Upsert(snapshot)
                                } else {
                                    // It's ok if there's no logic,
                                    // because on that logic's last
                                    // awake it might have had a
                                    // LogicFate::Discard and been
                                    // dropped.
                                    Change::Discard
                                };
                                let flow_key = FlowKey(step_id.clone(), state_key);
                                let kchange = KChange(flow_key, change);
                                change_session.give(kchange);
                            }
                        }
                    }

                    // Schedule operator activation at the soonest
                    // requested logic awake time for any key.
                    if let Some(soonest_next_awake) = current_next_awake
                        .values()
                        .map(|next_awake| *next_awake - now)
                        .min()
                    {
                        activator.activate_after(soonest_next_awake.to_std().unwrap_or(cooldown));
                    }
                }

                if input_frontiers.is_eof() {
                    output_cap = None;
                    change_cap = None;
                }
            }
        });

        (output_stream, change_stream)
    }
}
