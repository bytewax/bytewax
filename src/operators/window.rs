//! Code implementing Bytewax's windowing operators on top of Timely's
//! operators.

use std::collections::HashMap;
use std::collections::HashSet;
use std::hash::Hash;

use log::debug;
use timely::dataflow::channels::pact;
use timely::dataflow::operators::generic::builder_rc::OperatorBuilder;
use timely::dataflow::operators::generic::FrontieredInputHandle;
use timely::dataflow::operators::FrontierNotificator;
use timely::dataflow::Scope;
use timely::dataflow::Stream;
use timely::ExchangeData;

use pyo3::prelude::*;
use timely::progress::Antichain;

use crate::pyo3_extensions::StateKey;
use crate::pyo3_extensions::TdPyAny;

// Based on the good work in
// https://github.com/TimelyDataflow/timely-dataflow/blob/0d0d84885672d6369a78cd9aff7beb2048390d3b/timely/src/dataflow/operators/aggregation/state_machine.rs#L57
pub(crate) trait TumblingWindow<S: Scope, K: ExchangeData + Hash + Eq, V: ExchangeData> {
    /// First return stream is window output, second is
    /// updates for recovery you'll probably pass into a [`Backup`]
    /// operator.
    fn tumbling_window<
        W: Fn(&K, Option<V>, Vec<V>) -> V + 'static, // Windowing function
        H: Fn(&K) -> u64 + 'static,                   // Hash function of key to worker
    >(
        &self,
        windowing_stream: Stream<S, ()>,
        state_cache: HashMap<K, V>,
        window_fn: W,
        hasher: H,
    ) -> (Stream<S, (K, V)>, Stream<S, (K, Option<V>)>)
    where
        S: Scope<Timestamp = u64>;
}

impl<S: Scope, K: ExchangeData + Hash + Eq, V: ExchangeData> TumblingWindow<S, K, V>
    for Stream<S, (K, V)>
{
    fn tumbling_window<
        W: Fn(&K, Option<V>, Vec<V>) -> V + 'static, // Windowing function
        H: Fn(&K) -> u64 + 'static,                   // Hash function of key to worker
    >(
        &self,
        windowing_stream: Stream<S, ()>,
        mut state_cache: HashMap<K, V>,
        window_fn: W,
        hasher: H,
    ) -> (Stream<S, (K, V)>, Stream<S, (K, Option<V>)>) {
        let mut op_builder = OperatorBuilder::new("TumblingWindow".to_owned(), self.scope());

        let mut input = op_builder.new_input(
            self,
            pact::Exchange::new(move |&(ref key, ref _value)| hasher(key)),
        );

        let (mut downstream_output_wrapper, downstream_stream) = op_builder.new_output();
        let (mut state_update_output_wrapper, state_update_stream) = op_builder.new_output();

        let mut windowing_input = op_builder.new_input_connection(
            &windowing_stream,
            pact::Pipeline,
            vec![Antichain::new(), Antichain::new()],
        );

        op_builder.build(move |_init_capabilities| {
            let mut tmp_key_values = Vec::new();
            let mut window_buffer = HashMap::new();

            let mut tmp_updated_keys = HashSet::new();
            let mut outgoing_epoch_to_state_updates_buffer = HashMap::new();

            let mut downstream_fncater = FrontierNotificator::new();
            let mut state_update_fncater = FrontierNotificator::new();
            move |input_frontiers| {
                let mut input_handle = FrontieredInputHandle::new(&mut input, &input_frontiers[0]);
                let mut window_handle = FrontieredInputHandle::new(&mut windowing_input, &input_frontiers[1]);
                let mut downstream_output_handle = downstream_output_wrapper.activate();
                let mut state_update_output_handle = state_update_output_wrapper.activate();

                window_handle.for_each(|cap, _value| {
                    debug!("Window input received: {cap:?}");
                    let epoch = cap.time();
                    downstream_fncater.notify_at(cap.delayed_for_output(epoch, 0));
                });

                input_handle.for_each(|cap, key_values| {
                    let epoch = cap.time();
                    debug!("Normal input received, epoch: {epoch:?}");
                    key_values.swap(&mut tmp_key_values);

                    for (key, value) in tmp_key_values.drain(..) {
                        window_buffer
                            .entry(key.clone())
                            .or_insert_with(Vec::new)
                            .push(value);
                    }

                    state_update_fncater.notify_at(cap.delayed_for_output(epoch, 1));
                });

                // This runs once per epoch that will be no longer be
                // seen in the input of this operator (not the whole
                // dataflow, though).
                downstream_fncater.for_each(
                    &[window_handle.frontier()],
                    |downstream_cap, _ncater| {
                        let epoch = downstream_cap.time();
                        for (key, value) in window_buffer.drain() {
                            let current_window = state_cache.remove(&key);
                            let updated_window_for_key = window_fn(&key, current_window, value);
                            // Save the window so we can use
                            // it if there are multiple values
                            // within this epoch. We do not save
                            // to the recovery store here because
                            // we don't have finalized state for
                            // the epoch.
                            let mut downstream_output_session =
                                downstream_output_handle.session(&downstream_cap);
                            downstream_output_session.give((key.clone(), updated_window_for_key));
                            state_cache.remove(&key);
                        }

                        // Now that this epoch is over, find the final
                        // updated state for all keys that were
                        // touched and save that to be emitted
                        // downstream for the second output.
                        for key in tmp_updated_keys.drain() {
                            let updated_state = state_cache.get(&key).cloned();
                            outgoing_epoch_to_state_updates_buffer
                                .entry(epoch.clone())
                                .or_insert_with(Vec::new)
                                .push((key, updated_state));
                        }
                    },
                );

                state_update_fncater.for_each(
                    &[input_handle.frontier()],
                    |state_update_cap, _ncater| {
                        let epoch = state_update_cap.time();
                        let mut state_update_output_session =
                            state_update_output_handle.session(&state_update_cap);
                        // Now that this epoch is complete, write out any
                        // modified state. `remove()` will ensure we
                        // slowly drain the buffer.
                        if let Some(state_updates) =
                            outgoing_epoch_to_state_updates_buffer.remove(epoch)
                        {
                            for (key, updated_state) in state_updates {
                                state_update_output_session.give((key, updated_state));
                            }
                        }
                    },
                );
            }
        });

        (downstream_stream, state_update_stream)
    }
}

pub(crate) fn aggregate_window(
    key: &StateKey,
    current_window: Option<TdPyAny>,
    values: Vec<TdPyAny>,
) -> TdPyAny {
    Python::with_gil(|py| {
        let updated_window = if let Some(current_window) = current_window {
            let mut current_window: Vec<TdPyAny> = current_window.extract(py).unwrap();
            current_window.extend(values);
            current_window
        } else {
            values.to_vec()
        };

        debug!("aggregate_window for key={key:?}) -> updated_window={updated_window:?}");
        updated_window.into_py(py).clone_ref(py).into()
    })
}
