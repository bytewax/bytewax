//! Internal code for output.
//!
//! For a user-centric version of output, read the `bytewax.output`
//! Python module docstring. Read that first.

use crate::execution::{WorkerCount, WorkerIndex};
use crate::pyo3_extensions::{extract_state_pair, TdPyAny, TdPyCallable};
use crate::recovery::model::*;
use crate::recovery::operators::{ClockStream, FlowChangeStream, Route};
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

/// Represents a `bytewax.outputs.PartOutput` from Python.
#[derive(Clone)]
pub(crate) struct PartOutput(Py<PyAny>);

/// Do some eager type checking.
impl<'source> FromPyObject<'source> for PartOutput {
    fn extract(ob: &'source PyAny) -> PyResult<Self> {
        let abc = ob
            .py()
            .import("bytewax.outputs")?
            .getattr("PartOutput")?
            .extract()?;
        if !ob.is_instance(abc)? {
            Err(PyTypeError::new_err(
                "partitioned output must derive from `bytewax.outputs.PartOutput`",
            ))
        } else {
            Ok(Self(ob.into()))
        }
    }
}

impl IntoPy<Py<PyAny>> for PartOutput {
    fn into_py(self, _py: Python<'_>) -> Py<PyAny> {
        self.0
    }
}

impl PartOutput {
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
                let sink: TdPyCallable = self
                    .0
                    .call_method1(py, "build_part", (key.clone(), state.clone_ref(py)))?
                    .extract(py)?;
                let ssink = StatefulSink { sink, state };
                Ok((key, ssink))
            }).collect::<PyResult<HashMap<StateKey, StatefulSink>>>()?;

        if !resume_state.is_empty() {
            tracing::warn!("Resume state exists for {step_id:?} for unknown partitions {:?}; changing partition counts? recovery state routing bug?", resume_state.keys());
        }

        let assign_part = self.0.getattr(py, "assign_part")?.extract(py)?;

        Ok((StatefulBundle { sinks }, PartAssigner(assign_part)))
    }
}

/// Represents a `bytewax.outputs.StatefulSink` in Python.
struct StatefulSink {
    sink: TdPyCallable,
    /// The last state seen.
    state: TdPyAny,
}

impl StatefulSink {
    /// Write the next item to this sink and save the resulting state.
    fn write(&mut self, py: Python, value: TdPyAny) -> PyResult<()> {
        let res = self.sink.call1(py, (value,))?;
        self.state = res.into();
        Ok(())
    }

    fn snapshot(&self) -> StateBytes {
        StateBytes::ser::<TdPyAny>(&self.state)
    }
}

/// All partitions for an output on a worker.
struct StatefulBundle {
    sinks: HashMap<StateKey, StatefulSink>,
}

impl StatefulBundle {
    fn snapshot(&self) -> Vec<(StateKey, StateBytes)> {
        self.sinks
            .iter()
            .map(|(key, writer)| (key.clone(), writer.snapshot()))
            .collect()
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

pub(crate) trait PartOutputOp<S>
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
    fn part_output(
        &self,
        py: Python,
        step_id: StepId,
        output: PartOutput,
        worker_index: WorkerIndex,
        worker_count: WorkerCount,
        resume_state: StepStateBytes,
    ) -> PyResult<(ClockStream<S>, FlowChangeStream<S>)>;
}

impl<S> PartOutputOp<S> for Stream<S, TdPyAny>
where
    S: Scope<Timestamp = u64>,
{
    fn part_output(
        &self,
        py: Python,
        step_id: StepId,
        output: PartOutput,
        worker_index: WorkerIndex,
        worker_count: WorkerCount,
        resume_state: StepStateBytes,
    ) -> PyResult<(ClockStream<S>, FlowChangeStream<S>)> {
        let (mut bundle, assigner) = output.build(
            py,
            step_id.clone(),
            worker_index,
            worker_count,
            resume_state,
        )?;

        let kv_stream = self.map(extract_state_pair);

        let mut op_builder = OperatorBuilder::new(step_id.0.clone(), self.scope());

        let (mut clock_wrapper, clock_stream) = op_builder.new_output();
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
            // This is saying this input results in items on either
            // output.
            vec![Antichain::from_elem(0), Antichain::from_elem(0)],
        );

        op_builder.build(move |_init_caps| {
            let mut incoming_buffer: HashMap<S::Timestamp, Vec<(StateKey, TdPyAny)>> =
                HashMap::new();

            let mut tmp_incoming: Vec<(StateKey, TdPyAny)> = Vec::new();

            let mut clock_ncater = FrontierNotificator::new();
            let mut change_ncater = FrontierNotificator::new();

            move |input_frontiers| {
                input_handle.for_each(|cap, incoming| {
                    let epoch = cap.time();

                    assert!(tmp_incoming.is_empty());
                    incoming.swap(&mut tmp_incoming);

                    incoming_buffer
                        .entry(*epoch)
                        .or_insert_with(Vec::new)
                        .append(&mut tmp_incoming);

                    clock_ncater.notify_at(cap.delayed_for_output(epoch, 0));
                    change_ncater.notify_at(cap.delayed_for_output(epoch, 1));
                });

                clock_ncater.for_each(&[&input_frontiers[0]], |cap, _| {
                    let epoch = cap.time();

                    if let Some(items) = incoming_buffer.remove(epoch) {
                        let res: PyResult<()> = Python::with_gil(|py| {
                            for (key, value) in items {
                                let part_key = assigner.assign_part(py, key)?;
                                let sink = bundle.sinks.get_mut(&part_key).expect(
                                    "Item routed to non-local partition; output routing bug?",
                                );
                                sink.write(py, value)?;
                            }
                            Ok(())
                        });
                        unwrap_any!(res);
                    }

                    clock_wrapper.activate().session(&cap).give(());
                });

                change_ncater.for_each(&[&input_frontiers[0]], |cap, _| {
                    let kchanges = bundle.snapshot().into_iter().map(|(key, snapshot)| {
                        KChange(FlowKey(step_id.clone(), key), Change::Upsert(snapshot))
                    });
                    change_wrapper
                        .activate()
                        .session(&cap)
                        .give_iterator(kchanges);
                });
            }
        });

        Ok((clock_stream, change_stream))
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

impl IntoPy<Py<PyAny>> for DynamicOutput {
    fn into_py(self, _py: Python<'_>) -> Py<PyAny> {
        self.0
    }
}

impl DynamicOutput {
    /// Turn a dynamic output definition into the components the
    /// Timely operator needs to work.
    fn build(self, py: Python) -> PyResult<StatelessSink> {
        let sink = self.0.call_method0(py, "build")?.extract(py)?;
        Ok(StatelessSink { sink })
    }
}

/// Represents a `bytewax.outputs.StatelessSink` in Python.
struct StatelessSink {
    sink: TdPyCallable,
}

impl StatelessSink {
    /// Write the next item to the sink.
    fn write(&mut self, py: Python, item: TdPyAny) -> PyResult<()> {
        let _ = self.sink.call1(py, (item,))?;
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
        output: DynamicOutput,
    ) -> PyResult<ClockStream<S>> {
        let mut sink = output.build(py)?;

        let mut tmp_incoming: Vec<TdPyAny> = Vec::new();

        let mut incoming_buffer: HashMap<S::Timestamp, Vec<TdPyAny>> = HashMap::new();

        let clock_stream =
            self.unary_notify(Pipeline, &step_id.0, None, move |input, output, ncater| {
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
                ncater.for_each(|cap, _, _| {
                    let epoch = cap.time();

                    if let Some(items) = incoming_buffer.remove(epoch) {
                        let res: PyResult<()> = Python::with_gil(|py| {
                            for item in items {
                                sink.write(py, item)?;
                            }
                            Ok(())
                        });
                        unwrap_any!(res);
                    }

                    output.session(&cap).give(());
                });
            });

        Ok(clock_stream)
    }
}
