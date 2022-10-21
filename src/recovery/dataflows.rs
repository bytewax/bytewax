// Here are our loading dataflows.

/// Compile a dataflow which reads the progress data from the previous
/// execution and calculates the resume epoch.
///
/// Each resume cluster worker is assigned to read the entire progress
/// data from some failed cluster worker.
///
/// 1. Find the oldest frontier for the worker since we want to know
/// how far it got. (That's the first accumulate).
///
/// 2. Broadcast all our oldest per-worker frontiers.
///
/// 3. Find the earliest worker frontier since we want to resume from
/// the last epoch fully completed by all workers in the
/// cluster. (That's the second accumulate).
///
/// 4. Send the resume epoch out of the dataflow via a channel.
///
/// Once the progress input is done, this dataflow will send the
/// resume epoch through a channel. The main function should consume
/// from that channel to pass on to the other loading and production
/// dataflows.
pub(crate) fn build_resume_epoch_calc_dataflow<A, T>(
    timely_worker: &mut Worker<A>,
    // TODO: Allow multiple (or none) FrontierReaders so you can recover a
    // different-sized cluster.
    progress_reader: Box<dyn ProgressReader<T>>,
    resume_epoch_tx: std::sync::mpsc::Sender<T>,
) -> StringResult<ProbeHandle<()>>
where
    A: Allocate,
    T: Timestamp,
{
    timely_worker.dataflow(|scope| {
        let mut probe = ProbeHandle::new();

        progress_source(scope, progress_reader, &probe)
            .map(|ProgressUpdate(key, op)| (key, op))
            .aggregate(
                |_key, op, worker_frontier_acc: &mut Option<T>| {
                    let worker_frontier = worker_frontier_acc.get_or_insert(T::minimum());
                    // For each worker in the failed cluster, find the
                    // latest frontier.
                    match op {
                        ProgressOp::Upsert(progress) => {
                            if &progress.frontier > worker_frontier {
                                *worker_frontier = progress.frontier;
                            }
                        }
                    }
                },
                |key, worker_frontier_acc| {
                    (
                        key.worker_index,
                        worker_frontier_acc
                            .expect("Did not update worker_frontier_acc in aggregation"),
                    )
                },
                ProgressRecoveryKey::route_by_worker,
            )
            // Each worker in the recovery cluster reads only some of
            // the progress data of workers in the failed
            // cluster. Broadcast to ensure all recovery cluster
            // workers can calculate the dataflow frontier.
            .broadcast()
            .accumulate(None, |dataflow_frontier_acc, worker_frontiers| {
                for (_worker_index, worker_frontier) in worker_frontiers.iter() {
                    let dataflow_frontier =
                        dataflow_frontier_acc.get_or_insert(worker_frontier.clone());
                    // The slowest of the workers in the failed
                    // cluster is the resume epoch.
                    if worker_frontier < dataflow_frontier {
                        *dataflow_frontier = worker_frontier.clone();
                    }
                }
            })
            .map(move |resume_epoch| {
                resume_epoch_tx
                    .send(resume_epoch.expect("Did not update dataflow_frontier_acc in accumulate"))
                    .unwrap()
            })
            .probe_with(&mut probe);

        Ok(probe)
    })
}

/// Compile a dataflow which reads state data from the previous
/// execution and loads state into hash maps per step ID and key, and
/// prepares the recovery store summary.
///
/// Once the state input is done, this dataflow will send the
/// collected recovery data through these channels. The main function
/// should consume from the channels to pass on to the production
/// dataflow.
pub(crate) fn build_state_loading_dataflow<A>(
    timely_worker: &mut Worker<A>,
    state_reader: Box<dyn StateReader<u64>>,
    resume_epoch: u64,
    resume_state_tx: std::sync::mpsc::Sender<(StepId, HashMap<StateKey, State>)>,
    recovery_store_summary_tx: std::sync::mpsc::Sender<RecoveryStoreSummary<u64>>,
) -> StringResult<ProbeHandle<u64>>
where
    A: Allocate,
{
    timely_worker.dataflow(|scope| {
        let mut probe = ProbeHandle::new();

        let source = state_source(scope, state_reader, resume_epoch, &probe);

        source
            .unary_frontier(
                StateUpdate::pact_for_state_key(),
                "RecoveryStoreSummaryCalc",
                |_init_cap, _info| {
                    let mut fncater = FrontierNotificator::new();

                    let mut tmp_incoming = Vec::new();
                    let mut resume_state_buffer = HashMap::new();
                    let mut recovery_store_summary = Some(RecoveryStoreSummary::new());

                    move |input, output| {
                        input.for_each(|cap, incoming| {
                            let epoch = cap.time();
                            assert!(tmp_incoming.is_empty());
                            incoming.swap(&mut tmp_incoming);

                            resume_state_buffer
                                .entry(*epoch)
                                .or_insert_with(Vec::new)
                                .append(&mut tmp_incoming);

                            fncater.notify_at(cap.retain());
                        });

                        fncater.for_each(&[input.frontier()], |cap, _ncater| {
                            let epoch = cap.time();
                            if let Some(updates) = resume_state_buffer.remove(epoch) {
                                let recovery_store_summary = recovery_store_summary
                                    .as_mut()
                                    .expect(
                                    "More input after recovery store calc input frontier was empty",
                                );
                                for update in &updates {
                                    recovery_store_summary.insert(update);
                                }
                            }

                            // Emit heartbeats so we can monitor progress
                            // at the probe.
                            output.session(&cap).give(());
                        });

                        if input.frontier().is_empty() {
                            if let Some(recovery_store_summary) = recovery_store_summary.take() {
                                recovery_store_summary_tx
                                    .send(recovery_store_summary)
                                    .unwrap();
                            }
                        }
                    }
                },
            )
            .probe_with(&mut probe);

        source
            .unary_frontier(StateUpdate::pact_for_state_key(), "StateCacheCalc", |_init_cap, _info| {
                let mut fncater = FrontierNotificator::new();

                let mut tmp_incoming = Vec::new();
                let mut resume_state_buffer = HashMap::new();
                let mut resume_state: Option<HashMap<StepId, HashMap<StateKey, State>>> =
                    Some(HashMap::new());

                move |input, output| {
                    input.for_each(|cap, incoming| {
                        let epoch = cap.time();
                        assert!(tmp_incoming.is_empty());
                        incoming.swap(&mut tmp_incoming);

                        resume_state_buffer
                            .entry(*epoch)
                            .or_insert_with(Vec::new)
                            .append(&mut tmp_incoming);

                        fncater.notify_at(cap.retain());
                    });

                    fncater.for_each(&[input.frontier()], |cap, _ncater| {
                        let epoch = cap.time();
                        if let Some(updates) = resume_state_buffer.remove(epoch) {
                            for StateUpdate(recovery_key, op) in updates {
                                let StateRecoveryKey { step_id, state_key, epoch: found_epoch } = recovery_key;
                                assert!(&found_epoch == epoch);

                                let resume_state =
                                    resume_state
                                    .as_mut()
                                    .expect("More input after resume state calc input frontier was empty")
                                    .entry(step_id)
                                    .or_default();

                                match op {
                                    StateOp::Upsert(state) => {
                                        resume_state.insert(state_key.clone(), state);
                                    },
                                    StateOp::Discard => {
                                        resume_state.remove(&state_key);
                                    },
                                };
                            }
                        }

                        // Emit heartbeats so we can monitor progress
                        // at the probe.
                        output.session(&cap).give(());
                    });

                    if input.frontier().is_empty() {
                        for resume_state in resume_state.take().expect("More input after resume state calc input frontier was empty") {
                            resume_state_tx.send(resume_state).unwrap();
                        }
                    }
                }
            })
            .probe_with(&mut probe);

        Ok(probe)
    })
}
