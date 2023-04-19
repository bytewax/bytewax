//! Internal code for input.
//!
//! For a user-centric version of input, read the `bytewax.inputs`
//! Python module docstring. Read that first.

use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::time::Duration;
use std::time::Instant;

use pyo3::exceptions::PyStopIteration;
use pyo3::exceptions::PyTypeError;
use pyo3::exceptions::PyValueError;
use pyo3::intern;
use pyo3::prelude::*;
use timely::dataflow::channels::pact::Pipeline;
use timely::dataflow::operators::generic::builder_rc::OperatorBuilder;
use timely::dataflow::operators::Capability;
use timely::dataflow::ProbeHandle;
use timely::dataflow::Scope;
use timely::dataflow::Stream;
use timely::progress::Timestamp;

use crate::errors::tracked_err;
use crate::errors::PythonException;
use crate::pyo3_extensions::TdPyAny;
use crate::recovery::*;
use crate::timely::*;
use crate::unwrap_any;

/// Length of epoch.
#[derive(Debug, Copy, Clone)]
pub(crate) struct EpochInterval(Duration);

impl EpochInterval {
    ///
    pub(crate) fn epochs_per(&self, other: Duration) -> u64 {
        (other.as_secs_f64() / self.0.as_secs_f64())
            // Round up to we always have at least the backup interval
            // time. Unless it's 0, then it's ok. The integer part of
            // the result will always fit into a u64 so chopping off
            // bits should be fine.
            .ceil() as u64
    }
}

#[test]
fn test_epochs_per() {
    let found = EpochInterval(Duration::from_millis(5000)).epochs_per(Duration::from_millis(12000));
    assert_eq!(found, 3);
}

#[test]
fn test_epochs_per_zero() {
    let found = EpochInterval(Duration::from_millis(5000)).epochs_per(Duration::from_millis(0));
    assert_eq!(found, 0);
}

impl Default for EpochInterval {
    fn default() -> Self {
        Self(Duration::from_secs(10))
    }
}

impl<'source> FromPyObject<'source> for EpochInterval {
    fn extract(ob: &'source PyAny) -> PyResult<Self> {
        ob.extract::<chrono::Duration>()?
            .to_std()
            .map_err(|_err| {
                tracked_err::<PyValueError>("epoch interval must be a positive duration")
            })
            .map(Self)
    }
}

/// Represents a `bytewax.inputs.Input` from Python.
#[derive(Clone)]
pub(crate) struct Input(Py<PyAny>);

/// Do some eager type checking.
impl<'source> FromPyObject<'source> for Input {
    fn extract(ob: &'source PyAny) -> PyResult<Self> {
        let abc = ob
            .py()
            .import("bytewax.inputs")?
            .getattr("Input")?
            .extract()?;
        if !ob.is_instance(abc)? {
            Err(PyTypeError::new_err(
                "input must subclass `bytewax.inputs.Input`",
            ))
        } else {
            Ok(Self(ob.into()))
        }
    }
}

impl IntoPy<Py<PyAny>> for Input {
    fn into_py(self, _py: Python<'_>) -> Py<PyAny> {
        self.0
    }
}

impl Input {
    pub(crate) fn extract<'p, D>(&'p self, py: Python<'p>) -> PyResult<D>
    where
        D: FromPyObject<'p>,
    {
        self.0.extract(py)
    }
}

/// Represents a `bytewax.inputs.PartitionedInput` from Python.
#[derive(Clone)]
pub(crate) struct PartitionedInput(Py<PyAny>);

/// Do some eager type checking.
impl<'source> FromPyObject<'source> for PartitionedInput {
    fn extract(ob: &'source PyAny) -> PyResult<Self> {
        let abc = ob
            .py()
            .import("bytewax.inputs")?
            .getattr("PartitionedInput")?
            .extract()?;
        if !ob.is_instance(abc)? {
            Err(PyTypeError::new_err(
                "partitioned input must subclass `bytewax.inputs.PartitionedInput`",
            ))
        } else {
            Ok(Self(ob.into()))
        }
    }
}

struct PartState {
    part: StatefulSource,
    downstream_cap: Capability<u64>,
    snap_cap: Capability<u64>,
    epoch_started: Instant,
}

impl PartitionedInput {
    fn list_parts(&self, py: Python) -> PyResult<Vec<StateKey>> {
        self.0.call_method0(py, "list_parts")?.extract(py)
    }

    fn build_part(
        &self,
        py: Python,
        for_part: &StateKey,
        resume_state: Option<TdPyAny>,
    ) -> PyResult<StatefulSource> {
        self.0
            .call_method1(py, "build_part", (for_part.clone(), resume_state))?
            .extract(py)
    }

    /// Read items from a partitioned input.
    ///
    /// This is a stateful operator, so the change stream must be
    /// incorporated into the recovery system, and the resume state
    /// must be routed back here.
    ///
    /// Will manage automatically distributing partition sources. All
    /// you have to do is pass in the definition.
    pub(crate) fn partitioned_input<S>(
        self,
        py: Python,
        scope: &mut S,
        step_id: StepId,
        epoch_interval: EpochInterval,
        probe: &ProbeHandle<u64>,
        start_at: ResumeEpoch,
        loads: &Stream<S, Snapshot>,
    ) -> PyResult<(Stream<S, TdPyAny>, Stream<S, Snapshot>)>
    where
        S: Scope<Timestamp = u64>,
    {
        let this_worker = scope.w_index();

        let local_parts = self.list_parts(py)?;
        let all_parts = local_parts
            .clone()
            .into_broadcast(scope, S::Timestamp::minimum());
        let primary_updates = all_parts.assign_primaries(format!("{step_id}.assign_primaries"));

        let routed_loads = loads
            .filter_snaps(step_id.clone())
            .route(format!("{step_id}.loads_route"), &primary_updates);

        let op_name = format!("{step_id}.partitioned_input");
        let mut op_builder = OperatorBuilder::new(op_name.clone(), scope.clone());

        let mut loads_input = op_builder.new_input(&routed_loads, routed_exchange());
        let mut primaries_input = op_builder.new_input(&primary_updates, Pipeline);

        let (mut downstream_output, downstream) = op_builder.new_output();
        let (mut snaps_output, snaps) = op_builder.new_output();

        let info = op_builder.operator_info();
        let activator = scope.activator_for(&info.address[..]);
        let cooldown = Duration::from_millis(1);
        let probe = probe.clone();

        op_builder.build(move |mut init_caps| {
            init_caps.downgrade_all(&start_at.0);
            let init_snap_cap = init_caps.pop().unwrap();
            let init_downstream_cap = init_caps.pop().unwrap();
            let mut init_caps = Some((init_downstream_cap, init_snap_cap));

            let mut parts: BTreeMap<StateKey, PartState> = BTreeMap::new();
            let mut primary_parts = BTreeSet::new();
            let mut eofd = BTreeSet::new();

            let mut tmp = Vec::new();
            let mut primaries_inbuf = InBuffer::new();

            move |input_frontiers| {
                tracing::debug_span!("operator", operator = op_name).in_scope(|| {
                    primaries_input.for_each(|cap, incoming| {
                        let epoch = cap.time();
                        primaries_inbuf.extend(*epoch, incoming);
                    });

                    loads_input.for_each(|cap, incoming| {
                        let load_epoch = cap.time();
                        assert!(tmp.is_empty());
                        incoming.swap(&mut tmp);

                        let now = Instant::now();
                        // Snapshots might be from an "old" epoch if there
                        // were no items during a more recent one, so
                        // ensure that we always FFWd the capabilities to
                        // where this execution should start.
                        let emit_epoch = std::cmp::max(*load_epoch, start_at.0);
                        for (worker, (part_key, change)) in tmp.drain(..) {
                            assert!(worker == this_worker);
                            if let StateChange::Upsert(state) = change {
                                tracing::info!("Resuming {part_key:?} at epoch {emit_epoch} with state {state:?}");
                                let part = unwrap_any!(Python::with_gil(|py| self.build_part(
                                    py,
                                    &part_key,
                                    Some(state)
                                )).reraise("error building StatefulSource with resume state"));
                                let state = PartState {
                                    part,
                                    downstream_cap: cap.delayed_for_output(&emit_epoch, 0),
                                    snap_cap: cap.delayed_for_output(&emit_epoch, 1),
                                    epoch_started: now,
                                };
                                parts.insert(part_key, state);
                            }
                        }
                    });

                    // Apply this worker's primary assignments in epoch
                    // order. We don't need a notificator here because we
                    // don't need any capability management.
                    let primaries_frontier = &input_frontiers[1];
                    let closed_primaries_epochs: Vec<_> = primaries_inbuf
                        .epochs()
                        .filter(|e| primaries_frontier.is_closed(e))
                        .collect();
                    for epoch in closed_primaries_epochs {
                        if let Some(primaries) = primaries_inbuf.remove(&epoch) {
                            for (part, worker) in primaries {
                                if worker == this_worker {
                                    primary_parts.insert(part);
                                }
                            }
                        }
                    }

                    // Init any partitions that didn't have load data once
                    // the loads input is EOF.
                    let loads_frontier = &input_frontiers[0];
                    if loads_frontier.is_eof() {
                        let now = Instant::now();
                        // We take this out of the Option so we drop the
                        // init caps and they don't linger.
                        if let Some((init_downstream_cap, init_snap_cap)) = init_caps.take() {
                            assert!(*init_downstream_cap.time() == *init_snap_cap.time());
                            let epoch = init_downstream_cap.time();
                            // This is a slight abuse of epoch semantics
                            // since have no way of synchronizing the
                            // evolution of `primary_parts` with the EOF
                            // of the load stream. But it's fine since
                            // we're never going to open up the loads
                            // stream again.
                            for part_key in &primary_parts {
                                if !parts.contains_key(part_key) {
                                    tracing::info!("Init-ing {part_key:?} at epoch {epoch:?}");
                                    let part = unwrap_any!(Python::with_gil(
                                        |py| self.build_part(py, part_key, None)
                                    ).reraise("error init StatefulSource"));
                                    let part_state = PartState {
                                        part,
                                        downstream_cap: init_downstream_cap.clone(),
                                        snap_cap: init_snap_cap.clone(),
                                        epoch_started: now,
                                    };
                                    parts.insert(part_key.clone(), part_state);
                                }
                            }
                        }
                    }

                    assert!(eofd.is_empty());
                    let mut handle = downstream_output.activate();
                    let mut just_emitted = false;
                    for (part_key, part_state) in parts.iter_mut() {
                        tracing::trace_span!("partition", part_key = ?part_key).in_scope(|| {
                            assert!(
                                *part_state.downstream_cap.time() == *part_state.snap_cap.time()
                            );
                            let epoch = part_state.downstream_cap.time();

                            if !probe.less_than(epoch) {
                                if let Some(mut batch) =
                                    unwrap_any!(Python::with_gil(|py| part_state
                                        .part
                                        .next_batch(py)).reraise("error getting next input batch"))
                                {
                                    if !batch.is_empty() {
                                        just_emitted = true;
                                    }

                                    handle
                                        .session(&part_state.downstream_cap)
                                        .give_vec(&mut batch);
                                } else {
                                    eofd.insert(part_key.clone());
                                    tracing::debug!("EOFd");
                                }

                                let now = Instant::now();
                                // Don't allow progress unless we've caught up,
                                // otherwise you can get cascading advancement and
                                // never poll input.
                                if now.duration_since(part_state.epoch_started) >= epoch_interval.0
                                {
                                    let state = unwrap_any!(Python::with_gil(|py| part_state.part.snapshot(py)).reraise("error snapshotting StatefulSource"));
                                    tracing::trace!("End of epoch {epoch} partition state now {state:?}");
                                    let snap = Snapshot(
                                        step_id.clone(),
                                        part_key.clone(),
                                        StateChange::Upsert(state),
                                    );
                                    snaps_output
                                        .activate()
                                        .session(&part_state.snap_cap)
                                        .give(snap);

                                    let next_epoch = *part_state.downstream_cap.time() + 1;
                                    part_state.downstream_cap.downgrade(&next_epoch);
                                    part_state.snap_cap.downgrade(&next_epoch);
                                    part_state.epoch_started = now;
                                    tracing::debug!("Advanced to epoch {next_epoch}");
                                }
                            }
                        });
                    }

                    while let Some(part) = eofd.pop_first() {
                        parts.remove(&part);
                    }

                    if !loads_frontier.is_eof() {
                        // If we're not done loading, don't explicitly
                        // request activation so we will only be
                        // awoken when there's new loading input and
                        // we don't spin during loading.
                    } else if !parts.is_empty() {
                        // Make sure to keep waking up if there's data
                        // to read.
                        if just_emitted {
                            activator.activate();
                        } else {
                            activator.activate_after(cooldown);
                        }
                    }
                });
            }
        });

        Ok((downstream, snaps))
    }
}

/// Represents a `bytewax.inputs.StatefulSource` in Python.
struct StatefulSource(Py<PyAny>);

/// Do some eager type checking.
impl<'source> FromPyObject<'source> for StatefulSource {
    fn extract(ob: &'source PyAny) -> PyResult<Self> {
        let abc = ob
            .py()
            .import("bytewax.inputs")?
            .getattr("StatefulSource")?
            .extract()?;
        if !ob.is_instance(abc)? {
            Err(tracked_err::<PyTypeError>(
                "stateful source must subclass `bytewax.inputs.StatefulSource`",
            ))
        } else {
            Ok(Self(ob.into()))
        }
    }
}

impl StatefulSource {
    fn next_batch(&self, py: Python) -> PyResult<Option<Vec<TdPyAny>>> {
        match self.0.call_method0(py, intern!(py, "next_batch")) {
            Err(stop_ex) if stop_ex.is_instance_of::<PyStopIteration>(py) => Ok(None),
            Err(err) => Err(err),
            Ok(items) => Ok(Some(items.extract(py).reraise(
                "`next_batch` method of StatefulSource did not return a list",
            )?)),
        }
    }

    fn snapshot(&self, py: Python) -> PyResult<TdPyAny> {
        Ok(self
            .0
            .call_method0(py, intern!(py, "snapshot"))
            .reraise("error calling StatefulSource.snapshot")?
            .into())
    }

    fn close(&self, py: Python) -> PyResult<()> {
        let _ = self
            .0
            .call_method0(py, "close")
            .reraise("error closing stateful source")?;
        Ok(())
    }
}

impl Drop for StatefulSource {
    fn drop(&mut self) {
        unwrap_any!(Python::with_gil(|py| self.close(py)).reraise("error closing StatefulSource"));
    }
}

/// Represents a `bytewax.inputs.DynamicInput` from Python.
#[derive(Clone)]
pub(crate) struct DynamicInput(Py<PyAny>);

/// Do some eager type checking.
impl<'source> FromPyObject<'source> for DynamicInput {
    fn extract(ob: &'source PyAny) -> PyResult<Self> {
        let abc = ob
            .py()
            .import("bytewax.inputs")?
            .getattr("DynamicInput")?
            .extract()?;
        if !ob.is_instance(abc)? {
            Err(tracked_err::<PyTypeError>(
                "dynamic input must subclass `bytewax.inputs.DynamicInput`",
            ))
        } else {
            Ok(Self(ob.into()))
        }
    }
}

impl DynamicInput {
    fn build(
        &self,
        py: Python,
        index: WorkerIndex,
        count: WorkerCount,
    ) -> PyResult<StatelessSource> {
        self.0
            .call_method1(py, "build", (index.0, count.0))?
            .extract(py)
    }

    /// Read items from a dynamic output.
    ///
    /// Will manage automatically building sinks. All you have to do
    /// is pass in the definition.
    pub(crate) fn dynamic_input<S>(
        self,
        py: Python,
        scope: &S,
        step_id: StepId,
        epoch_interval: EpochInterval,
        probe: &ProbeHandle<u64>,
        start_at: ResumeEpoch,
    ) -> PyResult<Stream<S, TdPyAny>>
    where
        S: Scope<Timestamp = u64>,
    {
        let worker_index = scope.w_index();
        let worker_count = scope.w_count();
        let source = self
            .build(py, worker_index, worker_count)
            .reraise("error building DynamicInput")?;

        let op_name = format!("{step_id}.dynamic_input");
        let mut op_builder = OperatorBuilder::new(op_name.clone(), scope.clone());

        let (mut output_wrapper, output_stream) = op_builder.new_output();

        let probe = probe.clone();
        let info = op_builder.operator_info();
        let activator = scope.activator_for(&info.address[..]);
        let cooldown = Duration::from_millis(1);

        op_builder.build(move |mut init_caps| {
            // Inputs must init to the resume epoch.
            init_caps.downgrade_all(&start_at.0);
            let output_cap = init_caps.pop().unwrap();

            let mut cap_src = Some((output_cap, source));
            let mut epoch_started = Instant::now();

            move |_input_frontiers| {
                tracing::debug_span!("operator", operator = op_name).in_scope(|| {
                    let mut eof = false;
                    let mut just_emitted = false;

                    if let Some((cap, source)) = &mut cap_src {
                        let epoch = cap.time();

                        if !probe.less_than(epoch) {
                            if let Some(mut batch) =
                                unwrap_any!(Python::with_gil(|py| source.next_batch(py))
                                    .reraise("error getting next input batch"))
                            {
                                if !batch.is_empty() {
                                    just_emitted = true;
                                }
                                output_wrapper.activate().session(&cap).give_vec(&mut batch);
                            } else {
                                eof = true;
                                tracing::trace!("EOFd");
                            }

                            let now = Instant::now();
                            // Don't allow progress unless we've caught up,
                            // otherwise you can get cascading advancement and
                            // never poll input.
                            if now.duration_since(epoch_started) >= epoch_interval.0 {
                                let next_epoch = epoch + 1;
                                epoch_started = now;
                                cap.downgrade(&next_epoch);
                                tracing::trace!("Advanced to epoch {next_epoch}");
                            }
                        }
                    }

                    if eof {
                        cap_src = None;
                    }

                    // Wake up constantly, because we never know when
                    // input will have new data.
                    if cap_src.is_some() {
                        if just_emitted {
                            activator.activate();
                        } else {
                            activator.activate_after(cooldown);
                        }
                    }
                });
            }
        });

        Ok(output_stream)
    }
}

/// Represents a `bytewax.inputs.StatelessSource` in Python.
struct StatelessSource(Py<PyAny>);

/// Do some eager type checking.
impl<'source> FromPyObject<'source> for StatelessSource {
    fn extract(ob: &'source PyAny) -> PyResult<Self> {
        let abc = ob
            .py()
            .import("bytewax.inputs")?
            .getattr("StatelessSource")?
            .extract()?;
        if !ob.is_instance(abc)? {
            Err(tracked_err::<PyTypeError>(
                "stateless source must subclass `bytewax.inputs.StatelessSource`",
            ))
        } else {
            Ok(Self(ob.into()))
        }
    }
}

impl StatelessSource {
    fn next_batch(&self, py: Python) -> PyResult<Option<Vec<TdPyAny>>> {
        match self.0.call_method0(py, intern!(py, "next_batch")) {
            Err(stop_ex) if stop_ex.is_instance_of::<PyStopIteration>(py) => Ok(None),
            Err(err) => Err(err),
            Ok(items) => Ok(Some(items.extract(py).reraise(
                "`next_batch` method of StatelessSource did not return a list",
            )?)),
        }
    }

    fn close(&self, py: Python) -> PyResult<()> {
        let _ = self.0.call_method0(py, "close")?;
        Ok(())
    }
}

impl Drop for StatelessSource {
    fn drop(&mut self) {
        unwrap_any!(Python::with_gil(|py| self.close(py)).reraise("error closing StatelessSource"));
    }
}
