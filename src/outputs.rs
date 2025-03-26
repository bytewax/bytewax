//! Internal code for output.
//!
//! For a user-centric version of output, read the `bytewax.output`
//! Python module docstring. Read that first.

use std::collections::BTreeMap;
use std::collections::BTreeSet;

use opentelemetry::KeyValue;
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
use crate::operators::ExtractKeyOp;
use crate::pyo3_extensions::TdPyAny;
use crate::pyo3_extensions::TdPyCallable;
use crate::recovery::*;
use crate::timely::*;
use crate::unwrap_any;
use crate::with_timer;

/// Represents a `bytewax.outputs.Sink` from Python.
#[derive(IntoPyObject)]
pub(crate) struct Sink(Py<PyAny>);

/// Do some eager type checking.
impl<'py> FromPyObject<'py> for Sink {
    fn extract_bound(ob: &Bound<'py, PyAny>) -> PyResult<Self> {
        let py = ob.py();
        let abc = py.import("bytewax.outputs")?.getattr("Sink")?;
        if !ob.is_instance(&abc)? {
            Err(tracked_err::<PyTypeError>(
                "sink must subclass `bytewax.outputs.Sink`",
            ))
        } else {
            Ok(Self(ob.as_unbound().clone_ref(py)))
        }
    }
}

impl Sink {
    pub(crate) fn extract<'p, D>(&'p self, py: Python<'p>) -> PyResult<D>
    where
        D: FromPyObject<'p>,
    {
        self.0.extract(py)
    }
}

/// Represents a `bytewax.outputs.PartitionedOutput` from Python.
pub(crate) struct FixedPartitionedSink(Py<PyAny>);

/// Do some eager type checking.
impl<'py> FromPyObject<'py> for FixedPartitionedSink {
    fn extract_bound(ob: &Bound<'py, PyAny>) -> PyResult<Self> {
        let py = ob.py();
        let abc = py
            .import("bytewax.outputs")?
            .getattr("FixedPartitionedSink")?;
        if !ob.is_instance(&abc)? {
            Err(tracked_err::<PyTypeError>(
                "fixed partitioned sink must subclass `bytewax.outputs.FixedPartitionedSink`",
            ))
        } else {
            Ok(Self(ob.as_unbound().clone_ref(py)))
        }
    }
}

impl FixedPartitionedSink {
    fn list_parts(&self, py: Python) -> PyResult<Vec<StateKey>> {
        self.0.call_method0(py, "list_parts")?.extract(py)
    }

    fn build_part(
        &self,
        py: Python,
        step_id: &StepId,
        for_part: &StateKey,
        resume_state: Option<PyObject>,
    ) -> PyResult<StatefulPartition> {
        self.0
            .call_method1(
                py,
                "build_part",
                (step_id.clone(), for_part.clone(), resume_state),
            )?
            .extract(py)
    }

    fn build_part_assigner(&self, py: Python) -> PyResult<PartitionAssigner> {
        Ok(PartitionAssigner(
            self.0.getattr(py, "part_fn")?.extract(py)?,
        ))
    }
}

/// Represents a `bytewax.outputs.StatefulSinkPartition` in Python.
struct StatefulPartition(Py<PyAny>);

/// Do some eager type checking.
impl<'py> FromPyObject<'py> for StatefulPartition {
    fn extract_bound(ob: &Bound<'py, PyAny>) -> PyResult<Self> {
        let py = ob.py();
        let abc = py
            .import("bytewax.outputs")?
            .getattr("StatefulSinkPartition")?;
        if !ob.is_instance(&abc)? {
            Err(tracked_err::<PyTypeError>(
                "stateful sink partition must subclass `bytewax.outputs.StatefulSinkPartition`",
            ))
        } else {
            Ok(Self(ob.as_unbound().clone_ref(py)))
        }
    }
}

impl StatefulPartition {
    fn write_batch(&self, py: Python, values: Vec<PyObject>) -> PyResult<()> {
        let _ = self
            .0
            .call_method1(py, intern!(py, "write_batch"), (values,))?;
        Ok(())
    }

    fn snapshot(&self, py: Python) -> PyResult<TdPyAny> {
        Ok(self.0.call_method0(py, intern!(py, "snapshot"))?.into())
    }

    fn close(&self, py: Python) -> PyResult<()> {
        let _ = self.0.call_method0(py, "close")?;
        Ok(())
    }
}

impl Drop for StatefulPartition {
    fn drop(&mut self) {
        unwrap_any!(
            Python::with_gil(|py| self.close(py)).reraise("error closing StatefulSinkPartition")
        );
    }
}

/// This is a separate object than the bundle so we can use Python's
/// RC to clone it into the exchange closure.
struct PartitionAssigner(TdPyCallable);

impl PartitionAssigner {
    fn part_fn(&self, py: Python, key: &StateKey) -> PyResult<usize> {
        self.0.bind(py).call1((key.clone(),))?.extract()
    }
}

impl PartitionFn<StateKey> for PartitionAssigner {
    fn assign(&self, key: &StateKey) -> usize {
        // TODO: This is a hot inner GIL acquisition. This should be
        // refactored into the output operator itself, but because
        // we're piggy-backing on the pure-Timely recovery partitioned
        // IO operators, we don't have access to batches. TBH probably
        // this will go away with 2PC being worked into `stateful`
        // operator in Python.
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
        sink: FixedPartitionedSink,
        loads: &Stream<S, Snapshot>,
    ) -> PyResult<(ClockStream<S>, Stream<S, Snapshot>)>;
}

impl<S> PartitionedOutputOp<S> for Stream<S, TdPyAny>
where
    S: Scope<Timestamp = u64>,
{
    fn partitioned_output(
        &self,
        py: Python,
        step_id: StepId,
        sink: FixedPartitionedSink,
        loads: &Stream<S, Snapshot>,
    ) -> PyResult<(ClockStream<S>, Stream<S, Snapshot>)> {
        let this_worker = self.scope().w_index();

        let local_parts = sink.list_parts(py).reraise("error listing partitions")?;
        let all_parts = local_parts.into_broadcast(&self.scope(), S::Timestamp::minimum());
        let primary_updates = all_parts.assign_primaries(format!("{step_id}.assign_primaries"));

        let pf = sink.build_part_assigner(py)?;
        let routed_self = self
            .extract_key(step_id.clone())
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

        let meter = opentelemetry::global::meter("bytewax");
        let item_inp_count = meter
            .u64_counter("item_inp_count")
            .with_description("number of items this step has ingested")
            .init();
        let write_batch_histogram = meter
            .f64_histogram("out_part_write_batch_duration_seconds")
            .with_description("`write_batch` duration in seconds")
            .init();
        let snapshot_histogram = meter
            .f64_histogram("snapshot_duration_seconds")
            .with_description("`snapshot` duration in seconds")
            .init();
        let labels = vec![
            KeyValue::new("step_id", step_id.0.to_string()),
            KeyValue::new("worker_index", this_worker.0.to_string()),
        ];

        op_builder.build(move |init_caps| {
            let parts: BTreeMap<StateKey, StatefulPartition> = BTreeMap::new();
            // Which partitions were written to in this epoch. We only
            // snapshot those.
            let awoken: BTreeSet<StateKey> = BTreeSet::new();

            let mut routed_tmp = Vec::new();
            // First `StateKey` is partition, second is data
            // routing.
            type PartToInBufferMap = BTreeMap<StateKey, Vec<(StateKey, TdPyAny)>>;
            let mut items_inbuf: BTreeMap<S::Timestamp, PartToInBufferMap> = BTreeMap::new();
            let mut loads_inbuf = InBuffer::new();
            let mut ncater = EagerNotificator::new(init_caps, (parts, awoken));

            move |input_frontiers| {
                tracing::debug_span!("operator", operator = op_name).in_scope(|| {
                    routed_input.for_each(|cap, incoming| {
                        let epoch = cap.time();
                        assert!(routed_tmp.is_empty());
                        incoming.swap(&mut routed_tmp);
                        for (worker, (part, (key, value))) in routed_tmp.drain(..) {
                            assert!(worker == this_worker);
                            items_inbuf
                                .entry(*epoch)
                                .or_insert_with(BTreeMap::new)
                                .entry(part)
                                .or_insert_with(Vec::new)
                                .push((key, value));
                        }

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
                                Python::with_gil(|py| {
                                    for (part_key, items) in part_to_items {
                                        let part = parts
                                            .entry(part_key.clone())
                                            // If there's no resume data for
                                            // this partition, lazily create
                                            // it.
                                            .or_insert_with_key(|part_key| {
                                                unwrap_any!(sink
                                                    .build_part(py, &step_id, part_key, None)
                                                    .reraise("error init StatefulSink"))
                                            });

                                        let batch: Vec<_> = items
                                            .into_iter()
                                            .map(|(_k, v)| v.into_py(py))
                                            .collect();
                                        item_inp_count.add(batch.len() as u64, &labels);
                                        with_timer!(
                                            write_batch_histogram,
                                            labels,
                                            unwrap_any!(part.write_batch(py, batch))
                                        );

                                        awoken.insert(part_key);
                                    }
                                });
                            };
                        },
                        |caps, (parts, awoken)| {
                            let clock_cap = &caps[0];
                            let snaps_cap = &caps[1];
                            let epoch = clock_cap.time();

                            clock_output.activate().session(clock_cap).give(());

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
                                let state = with_timer!(
                                    snapshot_histogram,
                                    labels,
                                    unwrap_any!(Python::with_gil(|py| part
                                        .snapshot(py)
                                        .reraise("error snapshotting StatefulSink")))
                                );
                                let snap =
                                    Snapshot(step_id.clone(), part_key, StateChange::Upsert(state));
                                session.give(snap);
                            }

                            // We must reset `awake` on each epoch.
                            assert!(awoken.is_empty());

                            if let Some(loads) = loads_inbuf.remove(epoch) {
                                unwrap_any!(Python::with_gil(|py| -> PyResult<()> {
                                    // If this worker was assigned to be
                                    // primary for a partition, build it.
                                    for (worker, (part_key, change)) in loads {
                                        if worker == this_worker {
                                            match change {
                                                StateChange::Upsert(state) => {
                                                    let part = sink
                                                        .build_part(
                                                            py,
                                                            &step_id,
                                                            &part_key,
                                                            Some(state.into_py(py)),
                                                        )
                                                        .reraise("error resuming StatefulSink")?;
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

                                    Ok(())
                                }));
                            }
                        },
                    );
                });
            }
        });

        Ok((clock, snaps))
    }
}

/// Represents a `bytewax.outputs.DynamicSink` from Python.
pub(crate) struct DynamicSink(Py<PyAny>);

/// Do some eager type checking.
impl<'py> FromPyObject<'py> for DynamicSink {
    fn extract_bound(ob: &Bound<'py, PyAny>) -> PyResult<Self> {
        let py = ob.py();
        let abc = py.import("bytewax.outputs")?.getattr("DynamicSink")?;
        if !ob.is_instance(&abc)? {
            Err(tracked_err::<PyTypeError>(
                "dynamic sink must subclass `bytewax.outputs.DynamicSink`",
            ))
        } else {
            Ok(Self(ob.as_unbound().clone_ref(py)))
        }
    }
}

impl DynamicSink {
    fn build(
        self,
        py: Python,
        step_id: &StepId,
        index: WorkerIndex,
        count: WorkerCount,
    ) -> PyResult<StatelessPartition> {
        self.0
            .call_method1(py, "build", (step_id.clone(), index.0, count.0))?
            .extract(py)
    }
}

/// Represents a `bytewax.outputs.StatelessSinkPartition` in Python.
struct StatelessPartition(Py<PyAny>);

/// Do some eager type checking.
impl<'py> FromPyObject<'py> for StatelessPartition {
    fn extract_bound(ob: &Bound<'py, PyAny>) -> PyResult<Self> {
        let py = ob.py();
        let abc = py
            .import("bytewax.outputs")?
            .getattr("StatelessSinkPartition")?;
        if !ob.is_instance(&abc)? {
            Err(tracked_err::<PyTypeError>(
                "stateless sink partition must subclass `bytewax.outputs.StatelessSinkPartition`",
            ))
        } else {
            Ok(Self(ob.as_unbound().clone_ref(py)))
        }
    }
}

impl StatelessPartition {
    fn write_batch(&self, py: Python, items: Vec<PyObject>) -> PyResult<()> {
        let _ = self
            .0
            .call_method1(py, intern!(py, "write_batch"), (items,))?;
        Ok(())
    }

    fn close(&self, py: Python) -> PyResult<()> {
        let _ = self.0.call_method0(py, "close")?;
        Ok(())
    }
}

impl Drop for StatelessPartition {
    fn drop(&mut self) {
        unwrap_any!(Python::with_gil(|py| self
            .close(py)
            .reraise("error closing StatelessSinkPartition")));
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
        sink: DynamicSink,
    ) -> PyResult<ClockStream<S>>;
}

impl<S> DynamicOutputOp<S> for Stream<S, TdPyAny>
where
    S: Scope<Timestamp = u64>,
{
    fn dynamic_output(
        &self,
        py: Python,
        step_id: StepId,
        sink: DynamicSink,
    ) -> PyResult<ClockStream<S>> {
        let worker_index = self.scope().w_index();
        let worker_count = self.scope().w_count();
        let mut part = Some(sink.build(py, &step_id, worker_index, worker_count)?);

        let meter = opentelemetry::global::meter("bytewax");
        let item_inp_count = meter
            .u64_counter("item_inp_count")
            .with_description("number of items this step has ingested")
            .init();
        let write_batch_histogram = meter
            .f64_histogram("out_part_write_batch_duration_seconds")
            .with_description("`write_batch` duration in seconds")
            .init();
        let labels = vec![
            KeyValue::new("step_id", step_id.0.to_string()),
            KeyValue::new("worker_index", worker_index.0.to_string()),
        ];

        let downstream = self.unary_frontier(Pipeline, &step_id.0, |_init_cap, _info| {
            let mut tmp_incoming: Vec<TdPyAny> = Vec::new();

            move |input, output| {
                part = part.take().and_then(|sink| {
                    input.for_each(|cap, incoming| {
                        assert!(tmp_incoming.is_empty());
                        incoming.swap(&mut tmp_incoming);

                        let mut output_session = output.session(&cap);

                        unwrap_any!(Python::with_gil(|py| -> PyResult<()> {
                            let batch: Vec<PyObject> = tmp_incoming
                                .split_off(0)
                                .into_iter()
                                .map(|item| item.into_py(py))
                                .collect();
                            item_inp_count.add(batch.len() as u64, &labels);

                            with_timer!(
                                write_batch_histogram,
                                &labels,
                                sink.write_batch(py, batch)
                                    .reraise("error writing output batch")
                            )?;

                            Ok(())
                        }));

                        output_session.give(());
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
