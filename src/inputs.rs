//! Internal code for input.
//!
//! For a user-centric version of input, read the `bytewax.inputs`
//! Python module docstring. Read that first.

use crate::errors::{tracked_err, PythonException};
use crate::pyo3_extensions::TdPyAny;
use crate::recovery::model::*;
use crate::recovery::operators::{FlowChangeStream, Route};
use crate::timely::CapabilityVecEx;
use crate::unwrap_any;
use crate::worker::{WorkerCount, WorkerIndex};
use chrono::{DateTime, Utc};
use pyo3::exceptions::{PyStopIteration, PyTypeError, PyValueError};
use pyo3::prelude::*;
use std::collections::{BTreeSet, HashMap, HashSet};
use std::time::{Duration, Instant};
use timely::dataflow::operators::generic::builder_rc::OperatorBuilder;
use timely::dataflow::{ProbeHandle, Scope, Stream};

/// Length of epoch.
#[derive(Debug, Clone)]
pub(crate) struct EpochInterval(Duration);

impl EpochInterval {
    pub(crate) fn new(dur: Duration) -> Self {
        Self(dur)
    }
}

impl<'source> FromPyObject<'source> for EpochInterval {
    fn extract(ob: &'source PyAny) -> PyResult<Self> {
        match ob.extract::<chrono::Duration>()?.to_std() {
            Err(err) => Err(tracked_err::<PyValueError>(&format!(
                "invalid epoch interval: {err}"
            ))),
            Ok(dur) => Ok(Self(dur)),
        }
    }
}

impl Default for EpochInterval {
    fn default() -> Self {
        Self(Duration::from_secs(10))
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

impl PartitionedInput {
    /// Build all partitions for this input for this worker.
    fn build(
        &self,
        py: Python,
        step_id: StepId,
        index: WorkerIndex,
        worker_count: WorkerCount,
        mut resume_state: StepStateBytes,
    ) -> PyResult<HashMap<StateKey, StatefulSource>> {
        let keys: BTreeSet<StateKey> = self
            .0
            .call_method0(py, "list_parts")
            .reraise("errror in `list_parts`")?
            .extract(py)
            .reraise("can't convert parts to set")?;

        let parts = keys
            .into_iter()
            // We are using the [`StateKey`] routing hash as the way to
            // divvy up partitions to workers. This is kinda an abuse of
            // behavior, but also means we don't have to find a way to
            // propogate the correct partition:worker mappings into the
            // restore system, which would be more difficult as we have to
            // find a way to treat this kind of state key differently. I
            // might regret this.
            .filter(|key| key.is_local(index, worker_count))
            .flat_map(|key| {
                let state = resume_state.remove(&key).map(StateBytes::de::<TdPyAny>);
                tracing::info!(
                    "{index:?} building input {step_id:?} \
                    source {key:?} with resume state {state:?}"
                );
                match self
                    .0
                    .call_method1(py, "build_part", (key.clone(), state))
                    .and_then(|part| part.extract(py))
                {
                    Err(err) => Some(Err(err)),
                    Ok(None) => None,
                    Ok(Some(part)) => Some(Ok((key, part))),
                }
            })
            .collect::<PyResult<HashMap<StateKey, StatefulSource>>>()
            .reraise("error creating input source partitions")?;

        if !resume_state.is_empty() {
            tracing::warn!(
                "Resume state exists for {step_id:?} \
                for unknown partitions {:?}; \
                changing partition counts? \
                recovery state routing bug?",
                resume_state.keys()
            );
        }

        Ok(parts)
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
        scope: &S,
        step_id: StepId,
        epoch_interval: EpochInterval,
        index: WorkerIndex,
        count: WorkerCount,
        probe: &ProbeHandle<u64>,
        start_at: ResumeEpoch,
        resume_state: StepStateBytes,
    ) -> PyResult<(Stream<S, TdPyAny>, FlowChangeStream<S>)>
    where
        S: Scope<Timestamp = u64>,
    {
        let mut parts = self.build(py, step_id.clone(), index, count, resume_state)?;

        let mut op_builder = OperatorBuilder::new(step_id.0.clone(), scope.clone());

        let (mut output_wrapper, output_stream) = op_builder.new_output();
        let (mut change_wrapper, change_stream) = op_builder.new_output();

        let probe = probe.clone();
        let info = op_builder.operator_info();
        let activator = scope.activator_for(&info.address[..]);
        let min_cooldown = chrono::Duration::milliseconds(1);

        op_builder.build(move |mut init_caps| {
            // Inputs must init to the resume epoch.
            init_caps.downgrade_all(&start_at.0);
            let change_cap = init_caps.pop().unwrap();
            let output_cap = init_caps.pop().unwrap();

            let mut caps = Some((output_cap, change_cap));
            let mut epoch_started = Instant::now();
            let mut emit_keys_buffer: HashSet<StateKey> = HashSet::new();
            let mut eofd_keys_buffer: HashSet<StateKey> = HashSet::new();
            let mut snapshot_keys_buffer: HashSet<StateKey> = HashSet::new();
            let mut activate_after = None;

            move |_input_frontiers| {
                caps = caps.take().and_then(|(output_cap, change_cap)| {
                    assert!(output_cap.time() == change_cap.time());
                    let epoch = output_cap.time();

                    if !probe.less_than(epoch) {
                        let mut output_handle = output_wrapper.activate();
                        let mut output_session = output_handle.session(&output_cap);
                        for (key, part) in parts.iter() {
                            // Ask the next awake time to the source.
                            let next_awake =
                                unwrap_any!(Python::with_gil(|py| part.next_awake(py)));
                            let now = Utc::now();
                            // Calculate for how long to wait for the next poll.
                            activate_after = Some(
                                next_awake
                                    .signed_duration_since(now)
                                    .max(min_cooldown)
                                    // The maximum time is the minimum of all the parts.
                                    .min(
                                        activate_after.unwrap_or_else(chrono::Duration::max_value),
                                    ),
                            );
                            if next_awake <= now {
                                let next =
                                    Python::with_gil(|py| part.next(py)).reraise_with(|| {
                                        format!("error getting input for key {key:?}")
                                    });
                                if let Some(mut items) = unwrap_any!(next) {
                                    output_session.give_vec(&mut items);
                                    emit_keys_buffer.insert(key.clone());
                                } else {
                                    tracing::trace!(
                                        "Input {step_id:?} partition {key:?} reached EOF"
                                    );
                                    eofd_keys_buffer.insert(key.clone());
                                }
                            }
                        }
                    }
                    // Don't allow progress unless we've caught up,
                    // otherwise you can get cascading advancement and
                    // never poll input.
                    let advance =
                        !probe.less_than(epoch) && epoch_started.elapsed() > epoch_interval.0;

                    // If the the current epoch will be over, snapshot
                    // to get "end of the epoch state".
                    if advance {
                        snapshot_keys_buffer.extend(emit_keys_buffer.drain());
                    }
                    snapshot_keys_buffer.extend(eofd_keys_buffer.clone());

                    if !snapshot_keys_buffer.is_empty() {
                        let kchanges = snapshot_keys_buffer
                            .drain()
                            .map(|state_key| {
                                let part = parts
                                    .get(&state_key)
                                    .expect("Unknown partition {state_key:?} to snapshot");
                                let snap = unwrap_any!(Python::with_gil(|py| part
                                    .snapshot(py)
                                    .reraise("error snapshotting input part")));
                                (state_key, snap)
                            })
                            .map(|(state_key, snap)| (FlowKey(step_id.clone(), state_key), snap))
                            .map(|(flow_key, snap)| KChange(flow_key, Change::Upsert(snap)));
                        change_wrapper
                            .activate()
                            .session(&change_cap)
                            .give_iterator(kchanges);
                    }

                    for key in eofd_keys_buffer.drain() {
                        let part = parts
                            .remove(&key)
                            .expect("Unknown partition {key:?} marked as EOF");
                        unwrap_any!(Python::with_gil(|py| part
                            .close(py)
                            .reraise("error closing input part")));
                    }

                    if parts.is_empty() {
                        tracing::trace!("Input {step_id:?} reached EOF");
                        None
                    } else if advance {
                        let next_epoch = epoch + 1;
                        epoch_started = Instant::now();
                        tracing::trace!("Input {step_id:?} advancing to epoch {next_epoch:?}");
                        Some((
                            output_cap.delayed(&next_epoch),
                            change_cap.delayed(&next_epoch),
                        ))
                    } else {
                        Some((output_cap, change_cap))
                    }
                });

                if caps.is_some() {
                    activator.activate_after(
                        activate_after
                            .take()
                            .unwrap_or(min_cooldown)
                            .to_std()
                            .unwrap(),
                    );
                }
            }
        });

        Ok((output_stream, change_stream))
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
    fn next(&self, py: Python) -> PyResult<Option<Vec<TdPyAny>>> {
        match self.0.call_method0(py, "next") {
            Err(stop_ex) if stop_ex.is_instance_of::<PyStopIteration>(py) => Ok(None),
            Err(err) => Err(err),
            Ok(items) => Ok(Some(
                items
                    .extract(py)
                    .reraise("`StatefulSource.next` did not return a list")?,
            )),
        }
    }

    fn next_awake(&self, py: Python) -> PyResult<DateTime<Utc>> {
        self.0
            .call_method0(py, "next_awake")
            .reraise("error calling `StatelessSource.next_awake`")?
            .extract(py)
            .reraise("error converting `StatefulSource.next_awake` return value to UTC datetime")
    }

    fn snapshot(&self, py: Python) -> PyResult<StateBytes> {
        let state = self
            .0
            .call_method0(py, "snapshot")
            .reraise("error calling `StatefulSource.snapshot`")?
            .into();
        Ok(StateBytes::ser::<TdPyAny>(&state))
    }

    fn close(self, py: Python) -> PyResult<()> {
        self.0
            .call_method0(py, "close")
            .reraise("error calling `StatefulSource.close`")?;
        Ok(())
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
            .call_method1(py, "build", (index, count))?
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
        index: WorkerIndex,
        count: WorkerCount,
        probe: &ProbeHandle<u64>,
        start_at: ResumeEpoch,
    ) -> PyResult<Stream<S, TdPyAny>>
    where
        S: Scope<Timestamp = u64>,
    {
        let source = self
            .build(py, index, count)
            .reraise("error building DynamicInput")?;

        let mut op_builder = OperatorBuilder::new(step_id.0.to_string(), scope.clone());

        let (mut output_wrapper, output_stream) = op_builder.new_output();

        let probe = probe.clone();
        let info = op_builder.operator_info();
        let activator = scope.activator_for(&info.address[..]);
        let min_cooldown = chrono::Duration::milliseconds(1);

        op_builder.build(move |mut init_caps| {
            // Inputs must init to the resume epoch.
            init_caps.downgrade_all(&start_at.0);
            let output_cap = init_caps.pop().unwrap();

            let mut cap_src = Some((output_cap, source));
            let mut epoch_started = Instant::now();
            let mut activate_after = min_cooldown;

            move |_input_frontiers| {
                let now = Utc::now();
                // We can't return an error here, but we need to stop execution
                // if we have an error in the user's code.
                // When this happens we panic with unwrap_any! and reraise
                // the exeption and adding a message to explain.
                cap_src = cap_src.take().and_then(|(cap, source)| {
                    let epoch = cap.time();

                    let mut eof = false;

                    // Ask the next awake time to the source.
                    let next_awake = unwrap_any!(Python::with_gil(|py| source.next_awake(py)));
                    activate_after = next_awake.signed_duration_since(now).max(min_cooldown);

                    // Only call `next` if `next_awake` is passed.
                    if !probe.less_than(epoch) && next_awake <= now {
                        let next = Python::with_gil(|py| source.next(py))
                            .reraise("error getting input from DynamicInput");

                        if let Some(mut items) = unwrap_any!(next) {
                            if !items.is_empty() {
                                output_wrapper.activate().session(&cap).give_vec(&mut items);
                            }
                        } else {
                            eof = true;
                        }
                    }
                    // Don't allow progress unless we've caught up,
                    // otherwise you can get cascading advancement and
                    // never poll input.
                    let advance =
                        !probe.less_than(epoch) && epoch_started.elapsed() > epoch_interval.0;

                    if eof {
                        tracing::trace!("Input {step_id:?} reached EOF");
                        unwrap_any!(
                            Python::with_gil(|py| source.close(py)).reraise("error closing source")
                        );
                        None
                    } else if advance {
                        let next_epoch = epoch + 1;
                        epoch_started = Instant::now();
                        tracing::trace!("Input {step_id:?} advancing to epoch {next_epoch:?}");
                        Some((cap.delayed(&next_epoch), source))
                    } else {
                        Some((cap, source))
                    }
                });

                // Park the worker for `activate_after` seconds.
                if cap_src.is_some() {
                    activator.activate_after(activate_after.to_std().unwrap());
                }
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
    fn next(&self, py: Python) -> PyResult<Option<Vec<TdPyAny>>> {
        match self.0.call_method0(py, "next") {
            Err(stop_ex) if stop_ex.is_instance_of::<PyStopIteration>(py) => Ok(None),
            Err(err) => Err(err),
            Ok(items) => Ok(Some(
                items
                    .extract(py)
                    .reraise("`StatelessSource.next` did not return a list")?,
            )),
        }
    }

    fn next_awake(&self, py: Python) -> PyResult<DateTime<Utc>> {
        self.0
            .call_method0(py, "next_awake")
            .reraise("error calling `StatelessSource.next_awake`")?
            .extract(py)
            .reraise("error converting `StatelessSource.next_awake` return value to datetime")
    }

    fn close(self, py: Python) -> PyResult<()> {
        let _ = self.0.call_method0(py, "close")?;
        Ok(())
    }
}
