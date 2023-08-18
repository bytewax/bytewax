//! Internal code for output.
//!
//! For a user-centric version of output, read the `bytewax.output`
//! Python module docstring. Read that first.

use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::time::Instant;

use pyo3::exceptions::PyTypeError;
use pyo3::intern;
use pyo3::prelude::*;
use timely::dataflow::channels::pact::Pipeline;
use timely::dataflow::operators::generic::builder_rc::OperatorBuilder;
use timely::dataflow::operators::Map;
use timely::dataflow::operators::Operator;
use timely::dataflow::Scope;
use timely::dataflow::Stream;
use timely::progress::Timestamp;

use crate::errors::tracked_err;
use crate::errors::PythonException;
use crate::pyo3_extensions::extract_state_pair;
use crate::pyo3_extensions::TdPyAny;
use crate::pyo3_extensions::TdPyCallable;
use crate::recovery::*;
use crate::timely::*;
use crate::unwrap_any;

/// Represents a `bytewax.outputs.Output` from Python.
#[derive(Clone)]
pub(crate) struct Output(Py<PyAny>);

/// Do some eager type checking.
impl<'source> FromPyObject<'source> for Output {
    fn extract(ob: &'source PyAny) -> PyResult<Self> {
        let abc = ob
            .py()
            .import("bytewax.outputs")?
            .getattr("Output")?
            .extract()?;
        if !ob.is_instance(abc)? {
            Err(tracked_err::<PyTypeError>(
                "output must subclass `bytewax.outputs.Output`",
            ))
        } else {
            Ok(Self(ob.into()))
        }
    }
}

impl IntoPy<Py<PyAny>> for Output {
    fn into_py(self, _py: Python<'_>) -> Py<PyAny> {
        self.0
    }
}

impl ToPyObject for Output {
    fn to_object(&self, py: Python<'_>) -> PyObject {
        self.0.to_object(py)
    }
}

impl Output {
    pub(crate) fn extract<'p, D>(&'p self, py: Python<'p>) -> PyResult<D>
    where
        D: FromPyObject<'p>,
    {
        self.0.extract(py)
    }
}

/// Represents a `bytewax.outputs.PartitionedOutput` from Python.
#[derive(Clone)]
pub(crate) struct PartitionedOutput(Py<PyAny>);

/// Do some eager type checking.
impl<'source> FromPyObject<'source> for PartitionedOutput {
    fn extract(ob: &'source PyAny) -> PyResult<Self> {
        let abc = ob
            .py()
            .import("bytewax.outputs")?
            .getattr("PartitionedOutput")?
            .extract()?;
        if !ob.is_instance(abc)? {
            Err(tracked_err::<PyTypeError>(
                "partitioned output must subclass `bytewax.outputs.PartitionedOutput`",
            ))
        } else {
            Ok(Self(ob.into()))
        }
    }
}

impl PartitionedOutput {
    fn list_parts(&self, py: Python) -> PyResult<Vec<StateKey>> {
        self.0.call_method0(py, "list_parts")?.extract(py)
    }

    fn build_part(
        &self,
        py: Python,
        for_part: &StateKey,
        resume_state: Option<TdPyAny>,
    ) -> PyResult<StatefulSink> {
        self.0
            .call_method1(py, "build_part", (for_part.clone(), resume_state))?
            .extract(py)
    }

    fn build_part_assigner(&self, py: Python) -> PyResult<PartitionAssigner> {
        Ok(PartitionAssigner(
            self.0.getattr(py, "part_fn")?.extract(py)?,
        ))
    }
}

/// Represents a `bytewax.outputs.StatefulSink` in Python.
struct StatefulSink(Py<PyAny>);

/// Do some eager type checking.
impl<'source> FromPyObject<'source> for StatefulSink {
    fn extract(ob: &'source PyAny) -> PyResult<Self> {
        let abc = ob
            .py()
            .import("bytewax.outputs")?
            .getattr("StatefulSink")?
            .extract()?;
        if !ob.is_instance(abc)? {
            Err(tracked_err::<PyTypeError>(
                "stateful sink must subclass `bytewax.outputs.StatefulSink`",
            ))
        } else {
            Ok(Self(ob.into()))
        }
    }
}

impl StatefulSink {
    fn write_batch(
        &self,
        py: Python,
        values: Vec<(StateKey, TdPyAny)>,
    ) -> PyResult<Option<Vec<TdPyAny>>> {
        self.0
            .call_method1(py, intern!(py, "write_batch"), (values,))?
            .extract(py)
    }

    fn snapshot(&self, py: Python) -> PyResult<TdPyAny> {
        Ok(self.0.call_method0(py, intern!(py, "snapshot"))?.into())
    }

    fn close(&self, py: Python) -> PyResult<()> {
        let _ = self.0.call_method0(py, "close")?;
        Ok(())
    }
}

impl Drop for StatefulSink {
    fn drop(&mut self) {
        unwrap_any!(Python::with_gil(|py| self.close(py)).reraise("error closing StatefulSink"));
    }
}

/// This is a separate object than the bundle so we can use Python's
/// RC to clone it into the exchange closure.
struct PartitionAssigner(TdPyCallable);

impl PartitionAssigner {
    fn part_fn(&self, py: Python, key: &StateKey) -> PyResult<usize> {
        self.0.call1(py, (key.clone(),))?.extract(py)
    }
}

impl PartitionFn<StateKey> for PartitionAssigner {
    fn assign(&self, key: &StateKey) -> usize {
        unwrap_any!(Python::with_gil(|py| self.part_fn(py, key))
            .reraise("error assigning output partition"))
    }
}

pub(crate) trait PartitionedOutputOp<S>
where
    S: Scope,
{
    /// Write items to a partitioned output.
    ///
    /// Will manage assigning primary workers for all partitions and
    /// building them.
    ///
    /// This can't be unified into the recovery system output
    /// operators because they are stateless.
    fn partitioned_output(
        &self,
        py: Python,
        step_id: StepId,
        output: PartitionedOutput,
        loads: &Stream<S, Snapshot>,
        min_batch_size: usize,
        timeout: Option<chrono::Duration>,
        // TODO: Make this return a clock stream or no downstream once
        // we have non-linear dataflows.
    ) -> PyResult<(Stream<S, TdPyAny>, Stream<S, Snapshot>)>;
}

impl<S> PartitionedOutputOp<S> for Stream<S, TdPyAny>
where
    S: Scope<Timestamp = u64>,
{
    fn partitioned_output(
        &self,
        py: Python,
        step_id: StepId,
        output: PartitionedOutput,
        loads: &Stream<S, Snapshot>,
        min_batch_size: usize,
        timeout: Option<chrono::Duration>,
    ) -> PyResult<(Stream<S, TdPyAny>, Stream<S, Snapshot>)> {
        let this_worker = self.scope().w_index();

        let local_parts = output.list_parts(py).reraise("error listing partitions")?;
        let all_parts = local_parts.into_broadcast(&self.scope(), S::Timestamp::minimum());
        let primary_updates = all_parts.assign_primaries(format!("{step_id}.assign_primaries"));

        let pf = output.build_part_assigner(py)?;
        let routed_self = self
            .map(extract_state_pair)
            .partition(
                format!("{step_id}.partition"),
                &all_parts.map(|(part, _worker)| part),
                pf,
            )
            .route(format!("{step_id}.self_route"), &primary_updates);
        // This has all the actual loads and must come after the
        // routing info in the 0th epoch for deterministic building.
        let routed_loads = loads
            .filter_snaps(step_id.clone())
            .route(format!("{step_id}.loads_route"), &primary_updates);

        let op_name = format!("{step_id}.partitioned_output");
        let mut op_builder = OperatorBuilder::new(op_name.clone(), self.scope());

        let mut routed_input = op_builder.new_input(&routed_self, routed_exchange());
        let mut loads_input = op_builder.new_input(&routed_loads, routed_exchange());

        let (mut clock_output, clock) = op_builder.new_output();
        let (mut snaps_output, snaps) = op_builder.new_output();

        op_builder.build(move |init_caps| {
            let parts: BTreeMap<StateKey, StatefulSink> = BTreeMap::new();
            // Which partitions were written to in this epoch. We only snapshot those.
            let awoken: BTreeSet<StateKey> = BTreeSet::new();

            // First `StateKey` is partition, second is data routing.
            type PartToInBufferMap = BTreeMap<StateKey, Vec<(StateKey, TdPyAny)>>;
            let mut items_inbuf: BTreeMap<S::Timestamp, PartToInBufferMap> = BTreeMap::new();
            let mut loads_inbuf = InBuffer::new();
            let mut ncater = EagerNotificator::new(init_caps, (parts, awoken));

            let mut last_activation = Instant::now();
            let timeout = timeout.map(|t| {
                t.to_std()
                    .expect("output's timeout should be a positive timedelta")
            });
            let mut routed_tmp: BTreeMap<(WorkerIndex, StateKey), Vec<(StateKey, TdPyAny)>> =
                BTreeMap::new();

            move |input_frontiers| {
                tracing::debug_span!("operator", operator = op_name).in_scope(|| {
                    routed_input.for_each(|cap, incoming| {
                        let epoch = cap.time();
                        for (worker, (part, item)) in incoming.take() {
                            routed_tmp
                                .entry((worker, part))
                                .or_insert_with(|| Vec::with_capacity(min_batch_size))
                                .push(item);
                        }

                        // Emit items if the vec reached the min_batch_size len,
                        // or if enough time has passed, whichever comes first.
                        // Keep the rest of the messages in routed_tmp
                        routed_tmp = routed_tmp
                            .iter_mut()
                            .filter_map(|((worker, part), items)| {
                                assert!(*worker == this_worker);
                                if items.is_empty() {
                                    return None;
                                }

                                let timeout_condition = timeout
                                    // If timeout is Some, check that enough time
                                    // has passed sine last_activation.
                                    .map(|t| last_activation.elapsed() >= t)
                                    // If timeout is None, set this to false and only activate
                                    // once items.len() >= min_batch_size.
                                    .unwrap_or(false);
                                if items.len() >= min_batch_size || timeout_condition {
                                    last_activation = Instant::now();
                                    items_inbuf
                                        .entry(*epoch)
                                        .or_insert_with(BTreeMap::new)
                                        .entry(part.clone())
                                        .or_insert_with(Vec::new)
                                        .append(items);
                                    None
                                } else {
                                    Some(((*worker, part.clone()), items.drain(..).collect()))
                                }
                            })
                            .collect();
                        ncater.notify_at(*epoch);
                    });
                    loads_input.buffer_notify(&mut loads_inbuf, &mut ncater);

                    ncater.for_each(
                        input_frontiers,
                        |caps, (parts, awoken)| {
                            let clock_cap = &caps[0];
                            let epoch = clock_cap.time();

                            // Writing happens eagerly in each epoch. We
                            // still use a notificator at all because we
                            // need to ensure that writes happen in epoch
                            // order.
                            if let Some(part_to_items) = items_inbuf.remove(epoch) {
                                for (part_key, items) in part_to_items {
                                    let part = parts
                                        .entry(part_key.clone())
                                        // If there's no resume data for
                                        // this partition, lazily create
                                        // it.
                                        .or_insert_with_key(|part_key| {
                                            unwrap_any!(Python::with_gil(|py| output
                                                .build_part(py, part_key, None)
                                                .reraise("error init StatefulSink")))
                                        });

                                    // Get items that the output wants to pass through
                                    let pass_through = unwrap_any!(Python::with_gil(
                                        |py| part.write_batch(py, items)
                                    ));
                                    if let Some(mut items) = pass_through {
                                        clock_output
                                            .activate()
                                            .session(clock_cap)
                                            .give_vec(&mut items);
                                    }
                                    awoken.insert(part_key);
                                }
                            }
                        },
                        |caps, (parts, awoken)| {
                            let clock_cap = &caps[0];
                            let snaps_cap = &caps[1];
                            let epoch = clock_cap.time();

                            // Always snapshot before building. If we have
                            // an incoming load, it means we have recovery
                            // state already at the end of the epoch, so
                            // it would be wasted to snap it again. Also
                            // this handles the "don't snapshot a
                            // just-made-`None`-state partition" problem.

                            let mut handle = snaps_output.activate();
                            let mut session = handle.session(snaps_cap);
                            // Make sure to only snapshot partitions
                            // that had data, otherwise we'll snapshot
                            // as loads are happening.
                            while let Some(part_key) = awoken.pop_first() {
                                let part = parts.get(&part_key).unwrap();
                                let state = unwrap_any!(Python::with_gil(|py| part
                                    .snapshot(py)
                                    .reraise("error snapshotting StatefulSink")));
                                let snap =
                                    Snapshot(step_id.clone(), part_key, StateChange::Upsert(state));
                                session.give(snap);
                            }

                            // We must reset `awake` on each epoch.
                            assert!(awoken.is_empty());

                            if let Some(loads) = loads_inbuf.remove(epoch) {
                                // If this worker was assigned to be
                                // primary for a partition, build it.
                                for (worker, (part_key, change)) in loads {
                                    if worker == this_worker {
                                        match change {
                                            StateChange::Upsert(state) => {
                                                let part = unwrap_any!(Python::with_gil(|py| {
                                                    output
                                                        .build_part(py, &part_key, Some(state))
                                                        .reraise("error resuming StatefulSink")
                                                }));
                                                parts.insert(part_key, part);
                                            }
                                            StateChange::Discard => {
                                                parts.remove(&part_key);
                                            }
                                        }
                                    } else {
                                        parts.remove(&part_key);
                                    }
                                }
                            }
                        },
                    );
                });
            }
        });

        Ok((clock, snaps))
    }
}

/// Represents a `bytewax.outputs.DynamicOutput` from Python.
#[derive(Clone)]
pub(crate) struct DynamicOutput(Py<PyAny>);

/// Do some eager type checking.
impl<'source> FromPyObject<'source> for DynamicOutput {
    fn extract(ob: &'source PyAny) -> PyResult<Self> {
        let abc = ob
            .py()
            .import("bytewax.outputs")?
            .getattr("DynamicOutput")?
            .extract()?;
        if !ob.is_instance(abc)? {
            Err(tracked_err::<PyTypeError>(
                "dynamic output must subclass `bytewax.outputs.DynamicOutput`",
            ))
        } else {
            Ok(Self(ob.into()))
        }
    }
}

impl DynamicOutput {
    fn build(self, py: Python, index: WorkerIndex, count: WorkerCount) -> PyResult<StatelessSink> {
        self.0
            .call_method1(py, "build", (index.0, count.0))?
            .extract(py)
    }
}

/// Represents a `bytewax.outputs.StatelessSink` in Python.
struct StatelessSink(Py<PyAny>);

/// Do some eager type checking.
impl<'source> FromPyObject<'source> for StatelessSink {
    fn extract(ob: &'source PyAny) -> PyResult<Self> {
        let abc = ob
            .py()
            .import("bytewax.outputs")?
            .getattr("StatelessSink")?
            .extract()?;
        if !ob.is_instance(abc)? {
            Err(tracked_err::<PyTypeError>(
                "stateless sink must subclass `bytewax.outputs.StatelessSink`",
            ))
        } else {
            Ok(Self(ob.into()))
        }
    }
}

impl StatelessSink {
    fn write_batch(&self, py: Python, items: Vec<TdPyAny>) -> PyResult<Option<Vec<TdPyAny>>> {
        self.0
            .call_method1(py, intern!(py, "write_batch"), (items,))?
            // Pass the returned value through
            .extract(py)
    }

    fn close(&self, py: Python) -> PyResult<()> {
        let _ = self.0.call_method0(py, "close")?;
        Ok(())
    }
}

impl Drop for StatelessSink {
    fn drop(&mut self) {
        unwrap_any!(Python::with_gil(|py| self
            .close(py)
            .reraise("error closing StatelessSink")));
    }
}

pub(crate) trait DynamicOutputOp<S>
where
    S: Scope,
{
    /// Write items to a dynamic output.
    ///
    /// Will manage automatically building sinks. All you have to do
    /// is pass in the definition.
    fn dynamic_output(
        &self,
        py: Python,
        step_id: StepId,
        output: DynamicOutput,
        min_batch_size: usize,
        timeout: Option<chrono::Duration>,
    ) -> PyResult<Stream<S, TdPyAny>>;
}

impl<S> DynamicOutputOp<S> for Stream<S, TdPyAny>
where
    S: Scope<Timestamp = u64>,
{
    fn dynamic_output(
        &self,
        py: Python,
        step_id: StepId,
        output: DynamicOutput,
        min_batch_size: usize,
        timeout: Option<chrono::Duration>,
    ) -> PyResult<Stream<S, TdPyAny>> {
        let worker_index = self.scope().w_index();
        let worker_count = self.scope().w_count();
        let mut sink = Some(output.build(py, worker_index, worker_count)?);
        let mut last_activation = Instant::now();
        let timeout = timeout.map(|t| {
            t.to_std()
                .expect("output's timeout should be a positive timedelta")
        });

        let downstream = self.unary_frontier(Pipeline, &step_id.0, |_init_cap, _info| {
            let mut tmp_incoming: Vec<TdPyAny> = Vec::with_capacity(min_batch_size);

            move |input, output| {
                sink = sink.take().and_then(|sink| {
                    input.for_each(|cap, incoming| {
                        tmp_incoming.extend(incoming.take());
                        let timeout_condition = timeout
                            // If timeout is Some, check that enough time
                            // has passed sine last_activation.
                            .map(|t| last_activation.elapsed() >= t)
                            // If timeout is None, set this to false and only activate
                            // once items.len() >= min_batch_size.
                            .unwrap_or(false);
                        if tmp_incoming.len() >= min_batch_size || timeout_condition {
                            last_activation = Instant::now();
                            let mut output_session = output.session(&cap);

                            let pass_through = unwrap_any!(Python::with_gil(|py| sink
                                .write_batch(py, tmp_incoming.drain(..).collect())
                                .reraise("error writing dynamic output")));

                            if let Some(mut items) = pass_through {
                                output_session.give_vec(&mut items);
                            }
                        }
                    });

                    if input.frontier().is_empty() {
                        None
                    } else {
                        Some(sink)
                    }
                });
            }
        });

        Ok(downstream)
    }
}
