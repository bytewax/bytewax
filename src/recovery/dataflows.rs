//! Timely dataflows and parts of dataflows used for recovery.
//!
//! See [`super::operators`] for semantics and details of Timely
//! operators here.
//!
//! [![Block diagram of recovery
//! system.](https://mermaid.ink/img/pako:eNqNVF2LozAU_SshD0sLbaF9LMzA1n4gzGDRWXSofUj1tpVqIjGZoQT_-yZatzpj2Xkp9X6ce87xXhWOWAx4jk-c5Gf0tgxpIQ_1Q4hdKGQGaJWz6IwskkZoSQQ5puwzxCFd2UpxIHFZovEE5ZwdAE3Gz2jlmxwaj9GWsxOHorDOhJ5AR3RyodSCMxJHpBCms7_KV8rniYCyDCnQOKQa0kdPT8imr5A1HTrwjBY7n_ELcPRKEro3NbymDRVtU2KjX8izDUhLnCeIAPSiqST01BHmPRDm-a4B8j1TYph7gnFo0fbWSq2TVACvlX3Lu0q5ELGPpmCtJ7bzvtvS_WCGp5Qns4zw621I_dCB8b7bp4PGmwru1lJ5s7FMzjU5w6Z2xSR-7xYySWPzemIZiYTRfybtayTI79WVxU5Q_b5_cboHwdhsD2yaSzE0IjTXrKjIB8F0sJtMJvsqHuLBBa4j9EFSCcMQ1_qcYFBNPsr0DzVCgmE9-Ev4fai1NaQewgXBrDXxzsSZDhwpNEM0NfDOrHmcaVhnWoVMx1sSXeoGSymL0YgI43vHkZ6Xve5Ur62-fTDHQqKLzB_sU_tMHKtazg4jvzWjk9gq1RzRwyPc3tG7vT9q_tGdV8u36JO2sf6_56Z7oxVuCD-QE1gsTSES_VZtrLZXdvewdfJ2J3iEM-AZSWL9TVQhRXplxBkyCPFc_40Jv5jVLXWdzGO9a6s40ZPwXHAJI0ykYN6VRnh-JGkBTdEyIfoWslu0_AvgJcen?type=png)](https://mermaid.live/edit#pako:eNqNVF2LozAU_SshD0sLbaF9LMzA1n4gzGDRWXSofUj1tpVqIjGZoQT_-yZatzpj2Xkp9X6ce87xXhWOWAx4jk-c5Gf0tgxpIQ_1Q4hdKGQGaJWz6IwskkZoSQQ5puwzxCFd2UpxIHFZovEE5ZwdAE3Gz2jlmxwaj9GWsxOHorDOhJ5AR3RyodSCMxJHpBCms7_KV8rniYCyDCnQOKQa0kdPT8imr5A1HTrwjBY7n_ELcPRKEro3NbymDRVtU2KjX8izDUhLnCeIAPSiqST01BHmPRDm-a4B8j1TYph7gnFo0fbWSq2TVACvlX3Lu0q5ELGPpmCtJ7bzvtvS_WCGp5Qns4zw621I_dCB8b7bp4PGmwru1lJ5s7FMzjU5w6Z2xSR-7xYySWPzemIZiYTRfybtayTI79WVxU5Q_b5_cboHwdhsD2yaSzE0IjTXrKjIB8F0sJtMJvsqHuLBBa4j9EFSCcMQ1_qcYFBNPsr0DzVCgmE9-Ev4fai1NaQewgXBrDXxzsSZDhwpNEM0NfDOrHmcaVhnWoVMx1sSXeoGSymL0YgI43vHkZ6Xve5Ur62-fTDHQqKLzB_sU_tMHKtazg4jvzWjk9gq1RzRwyPc3tG7vT9q_tGdV8u36JO2sf6_56Z7oxVuCD-QE1gsTSES_VZtrLZXdvewdfJ2J3iEM-AZSWL9TVQhRXplxBkyCPFc_40Jv5jVLXWdzGO9a6s40ZPwXHAJI0ykYN6VRnh-JGkBTdEyIfoWslu0_AvgJcen)
//!
//! ```mermaid
//! graph TD
//! subgraph "Resume Epoch Calc Dataflow"
//! EI{{read}} -. probe .-> EW
//! EI -- ProgressChange --> EB{{Broadcast}} -- ProgressChange --> EW{{Write}}
//! end
//!
//! EW == InMemProgress ==> B[Worker Main] == resume epoch ==> I & SI
//!
//! subgraph "State Loading Dataflow"
//! SI{{read}} -. probe .-> SWR & SWS
//! SI -- StoreChange --> SF{{Filter}} -- StoreChange --> SR{{Recover}} -- FlowChange --> SWR{{Write}}
//! SI -- StoreChange --> SS{{Summary}} -- SummaryChange --> SWS{{Write}}
//! end
//!
//! SWS == StoreSummary ==> GC
//! SWR == FlowState ==> A[Build Production Dataflow] == StepState ==> I & SOX & SOY
//!
//! subgraph "Production Dataflow"
//! I(Input) -- items --> XX1([...]) -- "(key, value)" --> SOX(StatefulUnary X) & SOY(StatefulUnary Y)
//! SOX & SOY -- "(key, value)" --> XX2([...]) -- items --> O1(Output 1) & O2(Output 2)
//! O1 & O2 -- Tick --> OC{{Concat}}
//! I & SOX & SOY -- FlowChange --> FC{{Concat}}
//! FC -- FlowChange --> SB{{Backup}} -- StoreChange --> SW{{Write}}
//! OC & SW -- Tick --> WC{{Concat}} -- Tick --> P{{Progress}} -- ProgressChange --> PW{{Write}} -- Tick --> PP{{Progress}} -- ProgressChange --> PB{{Broadcast}} -- ProgressChange --> GC
//! SB -- StoreChange --> GCS{{Summary}} -- SummaryChange --> GC
//! GC{{GarbageCollect}} -- StoreChange --> GCW{{Write}}
//! I -. probe .-> GCW
//! end
//! ```

use std::cell::RefCell;
use std::rc::Rc;

use timely::dataflow::{operators::*, Scope};
use timely::order::TotalOrder;
use timely::progress::timestamp::Refines;
use timely::{communication::Allocate, dataflow::ProbeHandle, progress::Timestamp, worker::Worker};

use super::model::*;
use super::operators::*;
use super::store::in_mem::*;
use crate::common::StringResult;

/// Add in the recovery machinery to the production dataflow.
///
/// In overview: Convert all step state changes in the dataflow into
/// writes to the recovery store. Write those. Calculate the dataflow
/// frontier by looking at the combined clock stream of output and
/// state writes. Write that progress. Calculate GC deletes based on
/// calculating the resume epoch if we were to resume right this
/// instant.
pub(crate) fn attach_recovery_to_dataflow<S, PW, SW>(
    probe: &mut ProbeHandle<S::Timestamp>,
    worker_key: WorkerKey,
    resume_progress: InMemProgress<S::Timestamp>,
    store_summary: StoreSummary<S::Timestamp>,
    progress_writer: PW,
    state_writer: SW,
    step_changes: FlowChangeStream<S>,
    capture_clock: ClockStream<S>,
) where
    S: Scope<Timestamp = u64>,
    PW: ProgressWriter<S::Timestamp> + 'static,
    SW: StateWriter<S::Timestamp> + 'static,
{
    let state_writer = Rc::new(RefCell::new(state_writer));

    let store_changes = step_changes.backup();
    let backup_clock = store_changes.write(state_writer.clone());
    let worker_clock = backup_clock.concat(&capture_clock);
    let progress_clock = worker_clock
        .progress(worker_key.clone())
        .inspect(|kchange| tracing::trace!("Worker progress {kchange:?}"))
        .write(progress_writer);
    // GC works on a progress stream. But we don't want to GC state
    // until the progress messages are written, thus we need to view
    // the progress of the "writing the progress" clock stream.
    let cluster_progress = progress_clock.progress(worker_key).broadcast();
    let gc_clock = store_changes
        .summary()
        .garbage_collect(cluster_progress, resume_progress, store_summary)
        .write(state_writer);
    // Rate limit the whole dataflow on GC, not just on main
    // execution.
    gc_clock.probe_with(probe);
}

/// Compile a dataflow which loads the progress data from the previous
/// cluster.
///
/// Each resume cluster worker is assigned to read the entire progress
/// data from some previous cluster worker.
///
/// Read state out of the cell once the probe is done. Each resume
/// cluster worker will have the progress info of all workers in the
/// previous cluster.
#[allow(clippy::type_complexity)]
pub(crate) fn build_progress_loading_dataflow<A, R>(
    timely_worker: &mut Worker<A>,
    // TODO: Allow multiple (or none) FrontierReaders so you can recover a
    // different-sized cluster.
    reader: R,
) -> StringResult<(ProbeHandle<()>, Rc<RefCell<InMemProgress<u64>>>)>
where
    A: Allocate,
    R: ProgressReader<u64> + 'static,
{
    timely_worker.dataflow(|scope| {
        let mut probe = ProbeHandle::new();
        let resume_progress = Rc::new(RefCell::new(InMemProgress::new()));

        read(scope, reader, &probe)
            .broadcast_write(resume_progress.clone())
            .probe_with(&mut probe);

        Ok((probe, resume_progress))
    })
}

/// Compile a dataflow which loads state data from the previous
/// cluster.
///
/// Loads up to, but not including, the resume epoch, since the resume
/// epoch is where input will begin during this recovered cluster.
///
/// Read state out of the cells once the probe is done.
#[allow(clippy::type_complexity)]
pub(crate) fn build_state_loading_dataflow<A, T, R>(
    timely_worker: &mut Worker<A>,
    reader: R,
    resume_epoch: ResumeEpoch<T>,
) -> StringResult<(
    ProbeHandle<T>,
    Rc<RefCell<FlowStateBytes>>,
    Rc<RefCell<StoreSummary<T>>>,
)>
where
    A: Allocate,
    T: Timestamp + Refines<()> + TotalOrder,
    R: StateReader<T> + 'static,
{
    timely_worker.dataflow(|scope| {
        let mut probe = ProbeHandle::new();
        let resume_state = Rc::new(RefCell::new(FlowStateBytes::new()));
        let summary = Rc::new(RefCell::new(InMemStore::new()));

        let store_change_stream = read(scope, reader, &probe);

        store_change_stream
            // The resume epoch is the epoch we are starting at,
            // so only load state from before < that point. Not
            // <=.
            .filter(move |KChange(StoreKey(epoch, _flow_key), _change)| *epoch < resume_epoch.0)
            .recover()
            .write(resume_state.clone())
            .probe_with(&mut probe);

        // Might need to GC writes from some workers even if we
        // shouldn't be loading any state.
        store_change_stream
            .summary()
            .write(summary.clone())
            .probe_with(&mut probe);

        Ok((probe, resume_state, summary))
    })
}
