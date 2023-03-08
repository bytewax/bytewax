//! Internal code for output.
//!
//! For a user-centric version of output, read the `bytewax.output`
//! Python module docstring. Read that first.

use crate::execution::{WorkerCount, WorkerIndex};
use crate::pyo3_extensions::{extract_state_pair, wrap_state_pair, TdPyAny, TdPyCallable};
use crate::recovery::model::*;
use crate::recovery::operators::{FlowChangeStream, Route};
use crate::unwrap_any;
use pyo3::exceptions::PyTypeError;
use pyo3::prelude::*;
use std::collections::{BTreeSet, HashMap};
use timely::dataflow::channels::pact::{Exchange, Pipeline};
use timely::dataflow::operators::generic::builder_rc::OperatorBuilder;
use timely::dataflow::operators::{FrontierNotificator, Map, Operator};
use timely::dataflow::{Scope, Stream};
use timely::progress::Antichain;

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
            Err(PyTypeError::new_err(
                "output must derive from `bytewax.outputs.Output`",
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
            Err(PyTypeError::new_err(
                "partitioned output must derive from `bytewax.outputs.PartitionedOutput`",
            ))
        } else {
            Ok(Self(ob.into()))
        }
    }
}

impl PartitionedOutput {
    /// Turn a partitioned output definition into the components the
    /// Timely operator needs to work.
    fn build(
        self,
        py: Python,
        step_id: StepId,
        worker_index: WorkerIndex,
        worker_count: WorkerCount,
        mut resume_state: StepStateBytes,
    ) -> PyResult<(StatefulBundle, PartAssigner)> {
        let keys: BTreeSet<StateKey> = self.0.call_method0(py, "list_parts")?.extract(py)?;

        let sinks = keys
            .into_iter()
        // We are using the [`StateKey`] routing hash as the way to
        // divvy up partitions to workers. This is kinda an abuse of
        // behavior, but also means we don't have to find a way to
        // propogate the correct partition:worker mappings into the
        // restore system, which would be more difficult as we have to
        // find a way to treat this kind of state key differently. I
        // might regret this.
            .filter(|key| key.is_local(worker_index, worker_count))
            .map(|key| {
                let state = resume_state
                    .remove(&key)
                    .map(StateBytes::de::<TdPyAny>)
                    .unwrap_or_else(|| py.None().into());
                tracing::info!("{worker_index:?} building output {step_id:?} sink {key:?} with resume state {state:?}");
                let sink = self
                    .0
                    .call_method1(py, "build_part", (key.clone(), state.clone_ref(py)))?
                    .extract(py)?;
                Ok((key, sink))
            }).collect::<PyResult<HashMap<StateKey, StatefulSink>>>()?;

        if !resume_state.is_empty() {
            tracing::warn!("Resume state exists for {step_id:?} for unknown partitions {:?}; changing partition counts? recovery state routing bug?", resume_state.keys());
        }

        let assign_part = self.0.getattr(py, "assign_part")?.extract(py)?;

        Ok((StatefulBundle { parts: sinks }, PartAssigner(assign_part)))
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
            Err(PyTypeError::new_err(
                "stateful sink must derive from `bytewax.outputs.StatefulSink`",
            ))
        } else {
            Ok(Self(ob.into()))
        }
    }
}

impl StatefulSink {
    fn write(&self, py: Python, item: TdPyAny) -> PyResult<()> {
        let _ = self.0.call_method1(py, "write", (item,))?;
        Ok(())
    }

    fn snapshot(&self, py: Python) -> PyResult<StateBytes> {
        let state = self.0.call_method0(py, "snapshot")?.into();
        Ok(StateBytes::ser::<TdPyAny>(&state))
    }

    fn close(&self, py: Python) -> PyResult<()> {
        let _ = self.0.call_method0(py, "close")?;
        Ok(())
    }
}

/// All partitions for an output on a worker.
struct StatefulBundle {
    parts: HashMap<StateKey, StatefulSink>,
}

impl StatefulBundle {
    fn snapshot(&self, py: Python) -> PyResult<Vec<(StateKey, StateBytes)>> {
        self.parts
            .iter()
            .map(|(key, part)| Ok((key.clone(), part.snapshot(py)?)))
            .collect()
    }

    fn close(self, py: Python) -> PyResult<()> {
        for part in self.parts.values() {
            part.close(py)?
        }
        Ok(())
    }
}

/// This is a separate object than the bundle so we can use Python's
/// RC to clone it into the exchange closure.
struct PartAssigner(TdPyCallable);

impl PartAssigner {
    /// Determine which output partition should contain this item by
    /// its key.
    fn assign_part(&self, py: Python, key: StateKey) -> PyResult<StateKey> {
        self.0.call1(py, (key,))?.extract(py)
    }

    fn clone_ref(&self, py: Python) -> Self {
        Self(self.0.clone_ref(py))
    }
}

pub(crate) trait PartitionedOutputOp<S>
where
    S: Scope,
{
    /// Write items to a partitioned output.
    ///
    /// This is a stateful operator, so the change stream must be
    /// incorporated into the recovery system, and the resume state
    /// must be routed back here.
    ///
    /// Will manage automatically distributing partition sinks. All
    /// you have to do is pass in the definition.
    fn partitioned_output(
        &self,
        py: Python,
        step_id: StepId,
        output: PartitionedOutput,
        worker_index: WorkerIndex,
        worker_count: WorkerCount,
        resume_state: StepStateBytes,
    ) -> PyResult<(Stream<S, TdPyAny>, FlowChangeStream<S>)>;
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
        worker_index: WorkerIndex,
        worker_count: WorkerCount,
        resume_state: StepStateBytes,
    ) -> PyResult<(Stream<S, TdPyAny>, FlowChangeStream<S>)> {
        let (bundle, assigner) = output.build(
            py,
            step_id.clone(),
            worker_index,
            worker_count,
            resume_state,
        )?;
        let mut bundle = Some(bundle);

        let kv_stream = self.map(extract_state_pair);

        let mut op_builder = OperatorBuilder::new(step_id.0.clone(), self.scope());

        let (mut output_wrapper, output_stream) = op_builder.new_output();
        let (mut change_wrapper, change_stream) = op_builder.new_output();

        let ex_assigner = assigner.clone_ref(py);
        let mut input_handle = op_builder.new_input_connection(
            &kv_stream,
            Exchange::new(move |(key, _value): &(StateKey, TdPyAny)| {
                let part_key = unwrap_any!(Python::with_gil(
                    |py| ex_assigner.assign_part(py, key.clone())
                ));
                part_key.route()
            }),
            // This is saying this input results in items on any
            // output.
            vec![Antichain::from_elem(0), Antichain::from_elem(0)],
        );

        op_builder.build(move |_init_caps| {
            let mut incoming_buffer: HashMap<S::Timestamp, Vec<(StateKey, TdPyAny)>> =
                HashMap::new();

            let mut tmp_incoming: Vec<(StateKey, TdPyAny)> = Vec::new();

            let mut output_ncater = FrontierNotificator::new();
            let mut change_ncater = FrontierNotificator::new();

            move |input_frontiers| {
                bundle = bundle.take().and_then(|mut bundle| {
                    input_handle.for_each(|cap, incoming| {
                        let epoch = cap.time();

                        assert!(tmp_incoming.is_empty());
                        incoming.swap(&mut tmp_incoming);

                        incoming_buffer
                            .entry(*epoch)
                            .or_insert_with(Vec::new)
                            .append(&mut tmp_incoming);

                        output_ncater.notify_at(cap.delayed_for_output(epoch, 0));
                        change_ncater.notify_at(cap.delayed_for_output(epoch, 1));
                    });

                    output_ncater.for_each(&[&input_frontiers[0]], |cap, _| {
                        unwrap_any!(Python::with_gil(|py| -> PyResult<()> {
                            let epoch = cap.time();

                            if let Some(items) = incoming_buffer.remove(epoch) {
                                let mut output_handle = output_wrapper.activate();
                                let mut output_session = output_handle.session(&cap);
                                for (key, value) in items {
                                    let part_key = assigner.assign_part(py, key.clone())?;
                                    let sink = bundle.parts.get_mut(&part_key).expect(
                                        "Item routed to non-local partition; output routing bug?",
                                    );
                                    sink.write(py, value.clone_ref(py))?;
                                    output_session.give(wrap_state_pair((key, value)));
                                }
                            }
                            Ok(())
                        }))
                    });

                    change_ncater.for_each(&[&input_frontiers[0]], |cap, _| {
                        let kchanges = unwrap_any!(Python::with_gil(|py| bundle.snapshot(py)))
                            .into_iter()
                            .map(|(key, snapshot)| {
                                KChange(FlowKey(step_id.clone(), key), Change::Upsert(snapshot))
                            });
                        change_wrapper
                            .activate()
                            .session(&cap)
                            .give_iterator(kchanges);
                    });

                    if input_frontiers.iter().all(|f| f.is_empty()) {
                        unwrap_any!(Python::with_gil(|py| bundle.close(py)));
                        None
                    } else {
                        Some(bundle)
                    }
                });
            }
        });

        Ok((output_stream, change_stream))
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
            Err(PyTypeError::new_err(
                "dynamic output must derive from `bytewax.outputs.DynamicOutput`",
            ))
        } else {
            Ok(Self(ob.into()))
        }
    }
}

impl DynamicOutput {
    fn build(self, py: Python, index: WorkerIndex, count: WorkerCount) -> PyResult<StatelessSink> {
        self.0
            .call_method1(py, "build", (index, count))?
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
            Err(PyTypeError::new_err(
                "stateless sink must derive from `bytewax.outputs.StatelessSink`",
            ))
        } else {
            Ok(Self(ob.into()))
        }
    }
}

impl StatelessSink {
    fn write(&self, py: Python, item: TdPyAny) -> PyResult<()> {
        let _ = self.0.call_method1(py, "write", (item,))?;
        Ok(())
    }

    fn close(self, py: Python) -> PyResult<()> {
        let _ = self.0.call_method0(py, "close")?;
        Ok(())
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
        worker_index: WorkerIndex,
        worker_count: WorkerCount,
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
        worker_index: WorkerIndex,
        worker_count: WorkerCount,
    ) -> PyResult<Stream<S, TdPyAny>> {
        let mut sink = Some(output.build(py, worker_index, worker_count)?);

        let output_stream = self.unary_frontier(Pipeline, &step_id.0, |_init_cap, _info| {
            let mut ncater = FrontierNotificator::new();

            let mut tmp_incoming: Vec<TdPyAny> = Vec::new();

            let mut incoming_buffer: HashMap<S::Timestamp, Vec<TdPyAny>> = HashMap::new();

            move |input, output| {
                sink = sink.take().and_then(|sink| {
                    input.for_each(|cap, incoming| {
                        let epoch = cap.time();

                        assert!(tmp_incoming.is_empty());
                        incoming.swap(&mut tmp_incoming);

                        incoming_buffer
                            .entry(*epoch)
                            .or_insert_with(Vec::new)
                            .append(&mut tmp_incoming);

                        ncater.notify_at(cap.retain());
                    });

                    ncater.for_each(&[input.frontier()], |cap, _| {
                        unwrap_any!(Python::with_gil(|py| -> PyResult<()> {
                            let epoch = cap.time();

                            let mut output_session = output.session(&cap);
                            if let Some(items) = incoming_buffer.remove(epoch) {
                                for item in items {
                                    sink.write(py, item.clone_ref(py))?;
                                    output_session.give(item);
                                }
                            }
                            Ok(())
                        }))
                    });

                    if input.frontier().is_empty() {
                        unwrap_any!(Python::with_gil(|py| sink.close(py)));
                        None
                    } else {
                        Some(sink)
                    }
                });
            }
        });

        Ok(output_stream)
    }
}
