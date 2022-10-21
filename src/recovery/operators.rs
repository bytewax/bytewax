// Here's operators related to recovery.

/// Dataflow streams that go into stateful operators are routed by
/// state key.
pub(crate) type StatefulStream<S, V> = Stream<S, (StateKey, V)>;

/// Timely stream containing state changes for the recovery system.
pub(crate) type StateUpdateStream<S> = Stream<S, StateUpdate<<S as ScopeParent>::Timestamp>>;

/// Timely stream containing progress changes for the recovery system.
pub(crate) type ProgressUpdateStream<S> = Stream<S, ProgressUpdate<<S as ScopeParent>::Timestamp>>;

impl<T: Timestamp> StateUpdate<T> {
    /// Route in Timely just by the state key to ensure that all
    /// relevant data ends up on the correct worker.
    fn pact_for_state_key() -> impl ParallelizationContract<T, StateUpdate<T>> {
        pact::Exchange::new(|StateUpdate(StateRecoveryKey { state_key, .. }, ..)| state_key.route())
    }
}

/// Extension trait for [`Stream`].
pub(crate) trait WriteState<S>
where
    S: Scope,
{
    /// Writes state backups in timestamp order.
    fn write_state_with(&self, writer: Box<dyn StateWriter<S::Timestamp>>) -> StateUpdateStream<S>;
}

impl<S> WriteState<S> for StateUpdateStream<S>
where
    S: Scope,
{
    fn write_state_with(
        &self,
        mut writer: Box<dyn StateWriter<S::Timestamp>>,
    ) -> StateUpdateStream<S> {
        self.unary_notify(pact::Pipeline, "WriteState", None, {
            let mut tmp_incoming = Vec::new();
            let mut incoming_buffer = HashMap::new();

            move |input, output, ncater| {
                input.for_each(|cap, incoming| {
                    let epoch = cap.time();
                    assert!(tmp_incoming.is_empty());
                    incoming.swap(&mut tmp_incoming);

                    incoming_buffer
                        .entry(epoch.clone())
                        .or_insert_with(Vec::new)
                        .append(&mut tmp_incoming);

                    ncater.notify_at(cap.retain());
                });

                // Use the notificator to ensure state is written in
                // epoch order.
                ncater.for_each(|cap, _count, _ncater| {
                    let epoch = cap.time();
                    if let Some(updates) = incoming_buffer.remove(epoch) {
                        for update in updates {
                            writer.write(&update);
                            output.session(&cap).give(update);
                        }
                    }
                });
            }
        })
    }
}

/// Extension trait for [`Stream`].
pub(crate) trait WriteProgress<S, D>
where
    S: Scope,
    D: Data,
{
    /// Write out the current frontier of the output this is connected
    /// to whenever it changes.
    fn write_progress_with(
        &self,
        writer: Box<dyn ProgressWriter<S::Timestamp>>,
    ) -> ProgressUpdateStream<S>;
}

impl<S, D> WriteProgress<S, D> for Stream<S, D>
where
    S: Scope,
    D: Data,
{
    fn write_progress_with(
        &self,
        mut writer: Box<dyn ProgressWriter<S::Timestamp>>,
    ) -> ProgressUpdateStream<S> {
        self.unary_notify(pact::Pipeline, "WriteProgress", None, {
            let worker_index = WorkerIndex(self.scope().index());

            let mut tmp_incoming = Vec::new();

            move |input, output, ncater| {
                input.for_each(|cap, incoming| {
                    // We drop the old data in tmp_incoming since we
                    // don't care about the items.
                    incoming.swap(&mut tmp_incoming);

                    ncater.notify_at(cap.retain());
                });

                ncater.for_each(|cap, _count, ncater| {
                    // 0 is our singular input.
                    let frontier = ncater.frontier(0).to_owned();

                    // Don't write out the last "empty" frontier to
                    // allow restarting from the end of the dataflow.
                    if !frontier.elements().is_empty() {
                        // TODO: Is this the right way to transform a frontier
                        // back into a recovery epoch?
                        let frontier = frontier
                            .elements()
                            .iter()
                            .cloned()
                            .min()
                            .unwrap_or_else(S::Timestamp::minimum);
                        let progress = Progress { frontier };
                        let key = ProgressRecoveryKey { worker_index };
                        let op = ProgressOp::Upsert(progress);
                        let update = ProgressUpdate(key, op);
                        writer.write(&update);
                        output.session(&cap).give(update);
                    }
                });
            }
        })
    }
}

/// Build a source which loads previously backed up progress data as
/// separate items.
///
/// Note that [`T`] is the epoch of the previous, failed dataflow that
/// is being read. [`S::Timestamp`] is the timestamp used in the
/// recovery epoch calculation dataflow.
///
/// The resulting stream only has the zeroth epoch.
///
/// Note that this pretty meta! This new _loading_ dataflow will only
/// have the zeroth epoch, but you can observe what progress was made
/// on the _previous_ dataflow.
pub(crate) fn progress_source<S, T>(
    scope: &S,
    mut reader: Box<dyn ProgressReader<T>>,
    probe: &ProbeHandle<S::Timestamp>,
) -> Stream<S, ProgressUpdate<T>>
where
    S: Scope,
    T: Timestamp + Data,
    S::Timestamp: TotalOrder,
{
    iterator_source(
        scope,
        "ProgressSource",
        move |last_cap| {
            reader.read().map(|update| IteratorSourceInput {
                lower_bound: S::Timestamp::minimum(),
                // An iterator of (timestamp, iterator of
                // items). Nested [`IntoIterator`]s.
                data: Some((S::Timestamp::minimum(), Some(update))),
                target: last_cap.clone(),
            })
        },
        probe.clone(),
    )
}

/// Build a source which loads previously backed up state data.
///
/// The resulting stream has each state update in its original epoch.
///
/// State must be stored in epoch order. [`WriteState`] does that.
pub(crate) fn state_source<S>(
    scope: &S,
    mut reader: Box<dyn StateReader<S::Timestamp>>,
    stop_at: S::Timestamp,
    probe: &ProbeHandle<S::Timestamp>,
) -> StateUpdateStream<S>
where
    S: Scope,
    S::Timestamp: TotalOrder,
{
    iterator_source(
        scope,
        "StateSource",
        move |last_cap| match reader.read() {
            Some(update) => {
                let StateUpdate(key, _op) = &update;
                let StateRecoveryKey { epoch, .. } = key;
                let epoch = epoch.clone();

                if epoch < stop_at {
                    Some(IteratorSourceInput {
                        lower_bound: S::Timestamp::minimum(),
                        // An iterator of (timestamp, iterator of
                        // items). Nested [`IntoIterators`].
                        data: Some((epoch, Some(update))),
                        target: last_cap.clone(),
                    })
                } else {
                    None
                }
            }
            None => None,
        },
        probe.clone(),
    )
}

/// Extension trait for [`Stream`].
pub(crate) trait CollectGarbage<S>
where
    S: Scope,
{
    /// Run the recovery garbage collector.
    ///
    /// This will be instantiated on stream of already written state
    /// and will observe the dataflow's frontier and then decide what
    /// is garbage and delete it.
    ///
    /// It needs a separate handle to write to the state store so that
    /// there's not contention between it and [`WriteState`].
    fn collect_garbage(
        &self,
        recovery_store_summary: RecoveryStoreSummary<S::Timestamp>,
        state_collector: Box<dyn StateCollector<S::Timestamp>>,
        dataflow_frontier: ProgressUpdateStream<S>,
    ) -> Stream<S, ()>;
}

impl<S> CollectGarbage<S> for StateUpdateStream<S>
where
    S: Scope,
{
    fn collect_garbage(
        &self,
        mut recovery_store_summary: RecoveryStoreSummary<S::Timestamp>,
        mut state_collector: Box<dyn StateCollector<S::Timestamp>>,
        dataflow_frontier: ProgressUpdateStream<S>,
    ) -> Stream<S, ()> {
        let mut op_builder = OperatorBuilder::new("CollectGarbage".to_string(), self.scope());

        let mut state_input = op_builder.new_input(self, pact::Pipeline);
        let mut dataflow_frontier_input = op_builder.new_input(&dataflow_frontier, pact::Pipeline);

        let (mut output_wrapper, stream) = op_builder.new_output();

        let mut fncater = FrontierNotificator::new();
        op_builder.build(move |_init_capabilities| {
            let mut tmp_incoming_state = Vec::new();
            let mut tmp_incoming_frontier = Vec::new();

            move |input_frontiers| {
                let mut state_input =
                    FrontieredInputHandle::new(&mut state_input, &input_frontiers[0]);
                let mut dataflow_frontier_input =
                    FrontieredInputHandle::new(&mut dataflow_frontier_input, &input_frontiers[1]);

                let mut output_handle = output_wrapper.activate();

                // Update our internal cache of the state store's
                // keys.
                state_input.for_each(|_cap, incoming| {
                    assert!(tmp_incoming_state.is_empty());
                    incoming.swap(&mut tmp_incoming_state);

                    // Drain items so we don't have to re-allocate.
                    for state_backup in tmp_incoming_state.drain(..) {
                        recovery_store_summary.insert(&state_backup);
                    }
                });

                // Tell the notificator to trigger on dataflow
                // frontier advance.
                dataflow_frontier_input.for_each(|cap, incoming| {
                    assert!(tmp_incoming_frontier.is_empty());
                    // Drain the dataflow frontier input so the
                    // frontier advances.
                    incoming.swap(&mut tmp_incoming_frontier);
                    tmp_incoming_frontier.drain(..);

                    fncater.notify_at(cap.retain());
                });

                // Collect garbage.
                fncater.for_each(
                    &[state_input.frontier(), dataflow_frontier_input.frontier()],
                    |cap, _ncater| {
                        // If the dataflow frontier has passed a
                        // notificator-retained epoch, it means it is
                        // fully output and backed up.
                        let finalized_epoch = cap.time();

                        // Now remove all dead items from the state
                        // store and the local cache.
                        for recovery_key in recovery_store_summary.remove_garbage(finalized_epoch) {
                            state_collector.delete(&recovery_key);
                        }

                        // Note progress on the output stream.
                        output_handle.session(&cap);
                    },
                );

                // NOTE: We won't call this GC code when the dataflow
                // frontier closes / input is complete. This makes
                // sense to me: It's not correct to say last_epoch+1
                // has been "finalized" as it never happened. And it
                // supports the use case of "continuing" a completed
                // dataflow by starting back up at that epoch.
            }
        });

        stream
    }
}
