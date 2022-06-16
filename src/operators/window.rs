//! Code implementing Bytewax's windowing operators on top of Timely's
//! operators.

use std::collections::HashMap;
use std::collections::HashSet;
use std::hash::Hash;
use std::time::Duration;
use std::time::Instant;

use log::debug;
use timely::dataflow::channels::pact;
use timely::dataflow::operators::generic::builder_rc::OperatorBuilder;
use timely::dataflow::operators::generic::FrontieredInputHandle;
use timely::dataflow::operators::FrontierNotificator;
use timely::dataflow::Scope;
use timely::dataflow::Stream;
use timely::ExchangeData;

use pyo3::prelude::*;

use crate::pyo3_extensions::StateKey;
use crate::pyo3_extensions::TdPyAny;

// Create tumbling windows of data based on a supplied duration time
pub(crate) trait TumblingWindow<S: Scope, K: ExchangeData + Hash + Eq, V: ExchangeData> {
    /// First return stream is the windowed output, the second are
    /// updates for recovery you'll probably pass into a [`Backup`]
    /// operator.
    fn tumbling_window<
        W: Fn(&K, Option<V>, Vec<V>) -> V + 'static, // Windowing function
        H: Fn(&K) -> u64 + 'static,                  // Hash function of key to worker
    >(
        &self,
        state_cache: HashMap<K, V>,
        window_fn: W,
        window_time: &TdPyAny,
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
        H: Fn(&K) -> u64 + 'static,                  // Hash function of key to worker
    >(
        &self,
        mut state_cache: HashMap<K, V>,
        window_fn: W,
        window_time: &TdPyAny,
        hasher: H,
    ) -> (Stream<S, (K, V)>, Stream<S, (K, Option<V>)>) {
        let mut op_builder = OperatorBuilder::new("TumblingWindow".to_owned(), self.scope());

        let mut input = op_builder.new_input(
            self,
            pact::Exchange::new(move |&(ref key, ref _value)| hasher(key)),
        );

        let (mut downstream_output_wrapper, downstream_stream) = op_builder.new_output();
        let (mut state_update_output_wrapper, state_update_stream) = op_builder.new_output();

        let activator = self.scope().activator_for(&op_builder.operator_info().address[..]);

        op_builder.build(move |_init_capabilities| {
            let mut tmp_key_values = Vec::new();
            let mut window_buffer = HashMap::new();

            let mut tmp_updated_keys = HashSet::new();
            let mut outgoing_epoch_to_state_updates_buffer = HashMap::new();

            let mut downstream_fncater = FrontierNotificator::new();
            let mut state_update_fncater = FrontierNotificator::new();

            let mut now = Instant::now();
            let window_time: u64 = Python::with_gil(|py| {
                window_time
                    .extract(py)
                    .expect("window_time argument must be a number")
            });
            let window_duration = Duration::new(window_time, 0);
            // Re-activate this operator every 500ms to trigger window completion and
            // emit items downstream.
            // TODO: Find a more efficient way to schedule this operator activation.
            activator.activate_after(Duration::from_millis(500));

            move |input_frontiers| {
                let mut input_handle = FrontieredInputHandle::new(&mut input, &input_frontiers[0]);
                let mut downstream_output_handle = downstream_output_wrapper.activate();
                let mut state_update_output_handle = state_update_output_wrapper.activate();

                input_handle.for_each(|cap, key_values| {
                    let epoch = cap.time();
                    key_values.swap(&mut tmp_key_values);

                    for (key, value) in tmp_key_values.drain(..) {
                        window_buffer
                            .entry(key.clone())
                            .or_insert_with(Vec::new)
                            .push(value);
                    }

                    downstream_fncater.notify_at(cap.delayed_for_output(epoch, 0));
                    state_update_fncater.notify_at(cap.delayed_for_output(epoch, 1));
                });

                // This runs once per epoch that will be no longer be
                // seen in the input of this operator (not the whole
                // dataflow, though).
                downstream_fncater.for_each(
                    &[input_handle.frontier()],
                    |downstream_cap, _ncater| {
                        let epoch = downstream_cap.time();
                        for (key, value) in window_buffer.drain() {
                            let current_window = state_cache.remove(&key);
                            let updated_window_for_key = window_fn(&key, current_window, value);

                            // Check to see if we are past our window_duration time,
                            // and if so, emit the updated_window downstream
                            let new_now = Instant::now();
                            if new_now.duration_since(now) > window_duration {
                                let mut downstream_output_session =
                                    downstream_output_handle.session(&downstream_cap);
                                downstream_output_session
                                    .give((key.clone(), updated_window_for_key));
                                state_cache.remove(&key);
                                now = new_now;
                            } else {
                                state_cache.insert(key.clone(), updated_window_for_key);
                            }
                            // Note which keys have had their state
                            // modified. Write that updated state to
                            // the recovery store once the epoch is
                            // complete.
                            tmp_updated_keys.insert(key);
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
            values
        };

        debug!("aggregate_window for key={key:?}) -> updated_window={updated_window:?}");
        updated_window.into_py(py).into()
    })
}
