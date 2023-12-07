//! Code implementing Bytewax's core operators.

use std::collections::hash_map::DefaultHasher;
use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::hash::BuildHasherDefault;

use chrono::DateTime;
use chrono::Utc;
use opentelemetry::KeyValue;
use pyo3::exceptions::PyTypeError;
use pyo3::intern;
use pyo3::prelude::*;
use timely::dataflow::channels::pact::Pipeline;
use timely::dataflow::operators::generic::builder_rc::OperatorBuilder;
use timely::dataflow::operators::Branch;
use timely::dataflow::operators::Concatenate;
use timely::dataflow::operators::Exchange;
use timely::dataflow::operators::Map;
use timely::dataflow::operators::ToStream;
use timely::dataflow::Scope;
use timely::dataflow::Stream;
use timely::order::TotalOrder;
use timely::progress::Antichain;
use timely::ExchangeData;

use crate::errors::PythonException;
use crate::pyo3_extensions::TdPyAny;
use crate::pyo3_extensions::TdPyCallable;
use crate::recovery::*;
use crate::timely::*;
use crate::unwrap_any;
use crate::with_timer;

pub(crate) mod fold_window;
pub(crate) mod stateful_unary;

pub(crate) trait BranchOp<S>
where
    S: Scope,
{
    fn branch(
        &self,
        py: Python,
        step_id: StepId,
        predicate: TdPyCallable,
    ) -> PyResult<(Stream<S, TdPyAny>, Stream<S, TdPyAny>)>;
}

impl<S> BranchOp<S> for Stream<S, TdPyAny>
where
    S: Scope,
{
    fn branch(
        &self,
        _py: Python,
        step_id: StepId,
        predicate: TdPyCallable,
    ) -> PyResult<(Stream<S, TdPyAny>, Stream<S, TdPyAny>)> {
        let (falses, trues) = Branch::branch(self, move |_epoch, item| {
            let item = <&PyObject>::from(item);

            unwrap_any!(Python::with_gil(|py| predicate
                .as_ref(py)
                .call1((item,))
                .reraise_with(|| format!("error calling predicate in step {step_id}"))?
                .extract::<bool>()
                .reraise_with(|| format!(
                    "return value of `predicate` in step {step_id} must be a `bool`"
                ))))
        });

        Ok((trues, falses))
    }
}

fn next_batch(out_buffer: &mut Vec<TdPyAny>, mapper: &PyAny, in_item: PyObject) -> PyResult<()> {
    let res = mapper.call1((in_item,)).reraise("error calling mapper")?;
    let iter = res.iter().reraise_with(|| {
        format!(
            "mapper must return an iterable; got a `{}` instead",
            unwrap_any!(res.get_type().name()),
        )
    })?;
    for res in iter {
        let out_item = res.reraise("error while iterating through batch")?;
        out_buffer.push(out_item.into());
    }

    Ok(())
}

pub(crate) trait FlatMapOp<S>
where
    S: Scope,
    S::Timestamp: TotalOrder,
{
    fn flat_map(
        &self,
        py: Python,
        step_id: StepId,
        mapper: TdPyCallable,
    ) -> PyResult<Stream<S, TdPyAny>>;
}

impl<S> FlatMapOp<S> for Stream<S, TdPyAny>
where
    S: Scope,
    S::Timestamp: TotalOrder,
{
    fn flat_map(
        &self,
        _py: Python,
        step_id: StepId,
        mapper: TdPyCallable,
    ) -> PyResult<Stream<S, TdPyAny>> {
        let this_worker = self.scope().w_index();

        let mut op_builder = OperatorBuilder::new(step_id.0.clone(), self.scope());

        let mut self_handle = op_builder.new_input(self, Pipeline);

        let (mut downstream_output, downstream) = op_builder.new_output();

        let meter = opentelemetry::global::meter("bytewax");
        let item_inp_count = meter
            .u64_counter("item_inp_count")
            .with_description("number of items this step has ingested")
            .init();
        let item_out_count = meter
            .u64_counter("item_out_count")
            .with_description("number of items this step has emitted")
            .init();
        let mapper_histogram = meter
            .f64_histogram("flat_map_duration_seconds")
            .with_description("`flat_map` `mapper` duration in seconds")
            .init();
        let labels = vec![
            KeyValue::new("step_id", step_id.0.to_string()),
            KeyValue::new("worker_index", this_worker.0.to_string()),
        ];

        op_builder.build(move |init_caps| {
            let mut items_inbuf = InBuffer::new();
            let mut ncater = EagerNotificator::new(init_caps, ());
            // Timely forcibly flushes buffers when using
            // `Session::give_vec` so we need to build up a buffer of
            // items to reduce the overhead of that.
            let mut out_buffer = Vec::new();

            move |input_frontiers| {
                tracing::debug_span!("operator", operator = step_id.0.clone()).in_scope(|| {
                    self_handle.buffer_notify(&mut items_inbuf, &mut ncater);

                    let mut downstream_handle = downstream_output.activate();
                    ncater.for_each(
                        input_frontiers,
                        |caps, ()| {
                            let cap = &caps[0];
                            let epoch = cap.time();

                            if let Some(items) = items_inbuf.remove(epoch) {
                                item_inp_count.add(items.len() as u64, &labels);
                                let mut downstream_session = downstream_handle.session(cap);

                                unwrap_any!(Python::with_gil(|py| -> PyResult<()> {
                                    let mapper = mapper.as_ref(py);

                                    for item in items {
                                        let pool = unsafe { py.new_pool() };
                                        let _py = pool.python();

                                        let item = PyObject::from(item);

                                        let before_size = out_buffer.len();
                                        with_timer!(
                                            mapper_histogram,
                                            labels,
                                            next_batch(&mut out_buffer, mapper, item)
                                                .reraise_with(|| {
                                                    format!(
                                                        "error calling `mapper` in step {step_id}"
                                                    )
                                                })?
                                        );
                                        let batch_size = out_buffer.len() - before_size;
                                        item_out_count.add(batch_size as u64, &labels);
                                    }

                                    downstream_session.give_vec(&mut out_buffer);

                                    Ok(())
                                }));
                            }
                        },
                        |_caps, ()| {},
                    );
                });
            }
        });

        Ok(downstream)
    }
}

pub(crate) trait InspectDebugOp<S>
where
    S: Scope,
{
    fn inspect_debug(
        &self,
        py: Python,
        step_id: StepId,
        inspector: TdPyCallable,
    ) -> PyResult<(Stream<S, TdPyAny>, ClockStream<S>)>;
}

impl<S> InspectDebugOp<S> for Stream<S, TdPyAny>
where
    S: Scope,
    S::Timestamp: IntoPy<PyObject> + TotalOrder,
{
    fn inspect_debug(
        &self,
        _py: Python,
        step_id: StepId,
        inspector: TdPyCallable,
    ) -> PyResult<(Stream<S, TdPyAny>, ClockStream<S>)> {
        let this_worker = self.scope().w_index();

        let mut op_builder = OperatorBuilder::new(step_id.0.clone(), self.scope());

        let mut self_handle = op_builder.new_input(self, Pipeline);

        let (mut downstream_output, downstream) = op_builder.new_output();
        let (mut clock_output, clock) = op_builder.new_output();

        op_builder.build(move |init_caps| {
            let mut items_inbuf = InBuffer::new();
            let mut ncater = EagerNotificator::new(init_caps, ());

            move |input_frontiers| {
                tracing::debug_span!("operator", operator = step_id.0.clone()).in_scope(|| {
                    self_handle.buffer_notify(&mut items_inbuf, &mut ncater);

                    let mut downstream_handle = downstream_output.activate();
                    let mut clock_handle = clock_output.activate();
                    ncater.for_each(
                        input_frontiers,
                        |caps, ()| {
                            let downstream_cap = &caps[0];
                            let clock_cap = &caps[1];
                            let epoch = downstream_cap.time();

                            if let Some(mut items) = items_inbuf.remove(epoch) {
                                let mut downstream_session =
                                    downstream_handle.session(downstream_cap);
                                let mut clock_session = clock_handle.session(clock_cap);

                                unwrap_any!(Python::with_gil(|py| -> PyResult<()> {
                                    let inspector = inspector.as_ref(py);

                                    for item in items.iter() {
                                        let item = <&PyObject>::from(item);

                                        inspector
                                            .call1((
                                                step_id.clone(),
                                                item,
                                                epoch.clone(),
                                                this_worker.0,
                                            ))
                                            .reraise_with(|| {
                                                format!("error calling inspector in step {step_id}")
                                            })?;
                                    }

                                    Ok(())
                                }));

                                downstream_session.give_vec(&mut items);
                                clock_session.give(());
                            }
                        },
                        |_caps, ()| {},
                    );
                });
            }
        });

        Ok((downstream, clock))
    }
}

pub(crate) trait MergeOp<S>
where
    S: Scope,
{
    fn merge(
        &self,
        py: Python,
        step_id: StepId,
        ups: Vec<Stream<S, TdPyAny>>,
    ) -> PyResult<Stream<S, TdPyAny>>;
}

impl<S> MergeOp<S> for S
where
    S: Scope,
{
    fn merge(
        &self,
        _py: Python,
        _step_id: StepId,
        ups: Vec<Stream<S, TdPyAny>>,
    ) -> PyResult<Stream<S, TdPyAny>> {
        Ok(self.concatenate(ups))
    }
}

pub(crate) trait RedistributeOp<S, D>
where
    S: Scope,
    D: ExchangeData,
{
    fn redistribute(&self, step_id: StepId) -> Stream<S, D>;
}

impl<S, D> RedistributeOp<S, D> for Stream<S, D>
where
    S: Scope,
    D: ExchangeData,
{
    fn redistribute(&self, _step_id: StepId) -> Stream<S, D> {
        self.exchange(move |_| fastrand::u64(..))
    }
}

pub(crate) trait ExtractKeyOp<S>
where
    S: Scope,
{
    fn extract_key(&self, for_step_id: StepId) -> Stream<S, (StateKey, TdPyAny)>;
}

impl<S> ExtractKeyOp<S> for Stream<S, TdPyAny>
where
    S: Scope,
{
    fn extract_key(&self, for_step_id: StepId) -> Stream<S, (StateKey, TdPyAny)> {
        self.map(move |item| {
            let item = PyObject::from(item);

            let (key, value) = unwrap_any!(Python::with_gil(|py| -> PyResult<_> {
                let item = item.as_ref(py);

                let (key, value) = item
                    .extract::<(&PyAny, PyObject)>()
                    .raise_with::<PyTypeError>(|| {
                        format!("step {for_step_id} requires `(key, value)` 2-tuple from upstream for routing; got a `{}` instead",
                            unwrap_any!(item.get_type().name()),
                        )
                    })?;

                let key = key.extract::<StateKey>().raise_with::<PyTypeError>(|| {
                    format!("step {for_step_id} requires `str` keys in `(key, value)` from upstream; got a `{}` instead",
                        unwrap_any!(key.get_type().name()),
                    )
                })?;

                Ok((key, value))
            }));

            (key, TdPyAny::from(value))
        })
    }
}

pub(crate) trait MapOp<S>
where
    S: Scope,
    S::Timestamp: TotalOrder,
{
    fn map(
        &self,
        py: Python,
        step_id: StepId,
        mapper: TdPyCallable,
    ) -> PyResult<Stream<S, TdPyAny>>;
}

impl<S> MapOp<S> for Stream<S, TdPyAny>
where
    S: Scope,
    S::Timestamp: TotalOrder,
{
    fn map(
        &self,
        _py: Python,
        step_id: StepId,
        mapper: TdPyCallable,
    ) -> PyResult<Stream<S, TdPyAny>> {
        let this_worker = self.scope().w_index();

        let mut op_builder = OperatorBuilder::new(step_id.0.clone(), self.scope());

        let mut self_handle = op_builder.new_input(self, Pipeline);

        let (mut downstream_output, downstream) = op_builder.new_output();

        let meter = opentelemetry::global::meter("bytewax");
        let item_inp_count = meter
            .u64_counter("item_inp_count")
            .with_description("number of items this step has ingested")
            .init();
        let item_out_count = meter
            .u64_counter("item_out_count")
            .with_description("number of items this step has emitted")
            .init();
        let mapper_histogram = meter
            .f64_histogram("map_duration_seconds")
            .with_description("`map` `mapper` duration in seconds")
            .init();
        let labels = vec![
            KeyValue::new("step_id", step_id.0.to_string()),
            KeyValue::new("worker_index", this_worker.0.to_string()),
        ];

        op_builder.build(move |init_caps| {
            let mut items_inbuf = InBuffer::new();
            let mut ncater = EagerNotificator::new(init_caps, ());

            move |input_frontiers| {
                tracing::debug_span!("operator", operator = step_id.0.clone()).in_scope(|| {
                    self_handle.buffer_notify(&mut items_inbuf, &mut ncater);

                    let mut downstream_handle = downstream_output.activate();
                    ncater.for_each(
                        input_frontiers,
                        |caps, ()| {
                            let cap = &caps[0];
                            let epoch = cap.time();

                            if let Some(items) = items_inbuf.remove(epoch) {
                                item_inp_count.add(items.len() as u64, &labels);
                                let mut downstream_session = downstream_handle.session(cap);

                                unwrap_any!(Python::with_gil(|py| -> PyResult<()> {
                                    let mapper = mapper.as_ref(py);

                                    for in_item in items {
                                        let pool = unsafe { py.new_pool() };
                                        let _py = pool.python();

                                        let in_item = PyObject::from(in_item);

                                        let out_item = with_timer!(
                                            mapper_histogram,
                                            labels,
                                            mapper.call1((in_item,)).reraise_with(|| {
                                                format!("error calling `mapper` in step {step_id}")
                                            })?
                                        );
                                        item_out_count.add(1, &labels);
                                        downstream_session.give(TdPyAny::from(out_item));
                                    }

                                    Ok(())
                                }));
                            }
                        },
                        |_caps, ()| {},
                    );
                });
            }
        });

        Ok(downstream)
    }
}

pub(crate) trait WrapKeyOp<S>
where
    S: Scope,
{
    fn wrap_key(&self) -> Stream<S, TdPyAny>;
}

impl<S> WrapKeyOp<S> for Stream<S, (StateKey, TdPyAny)>
where
    S: Scope,
{
    fn wrap_key(&self) -> Stream<S, TdPyAny> {
        self.map(move |(key, value)| {
            let value = PyObject::from(value);

            let item = Python::with_gil(|py| IntoPy::<PyObject>::into_py((key, value), py));

            TdPyAny::from(item)
        })
    }
}

pub(crate) trait UnaryOp<S>
where
    S: Scope<Timestamp = u64>,
{
    fn unary(
        &self,
        py: Python,
        step_id: StepId,
        builder: TdPyCallable,
        resume_epoch: ResumeEpoch,
        loads: &Stream<S, Snapshot>,
    ) -> PyResult<(Stream<S, TdPyAny>, Stream<S, Snapshot>)>;
}

struct UnaryLogic(PyObject);

/// Do some eager type checking.
impl<'source> FromPyObject<'source> for UnaryLogic {
    fn extract(ob: &'source PyAny) -> PyResult<Self> {
        let abc = ob
            .py()
            .import("bytewax.operators")?
            .getattr("UnaryLogic")?
            .extract()?;
        if !ob.is_instance(abc)? {
            Err(PyTypeError::new_err(
                "logic must subclass `bytewax.operators.UnaryLogic`",
            ))
        } else {
            Ok(Self(ob.into()))
        }
    }
}

enum IsComplete {
    Retain,
    Discard,
}

impl<'source> FromPyObject<'source> for IsComplete {
    fn extract(ob: &'source PyAny) -> PyResult<Self> {
        if ob.extract::<bool>().reraise_with(|| {
            format!(
                "`is_complete` was not a `bool`; got a `{}` instead",
                unwrap_any!(ob.get_type().name())
            )
        })? {
            Ok(IsComplete::Discard)
        } else {
            Ok(IsComplete::Retain)
        }
    }
}

impl UnaryLogic {
    fn extract_ret(res: &PyAny) -> PyResult<(Vec<PyObject>, IsComplete)> {
        let (iter, is_complete) = res.extract::<(&PyAny, &PyAny)>().reraise_with(|| {
            format!(
                "did not return a 2-tuple of `(emit, is_complete)`; got a `{}` instead",
                unwrap_any!(res.get_type().name())
            )
        })?;
        let is_complete = is_complete.extract::<IsComplete>()?;
        let emit = iter.extract::<Vec<_>>().reraise_with(|| {
            format!(
                "`emit` was not a `list`; got a `{}` instead",
                unwrap_any!(iter.get_type().name())
            )
        })?;

        Ok((emit, is_complete))
    }

    fn on_item<'py>(
        &'py self,
        py: Python<'py>,
        py_now: PyObject,
        item: PyObject,
    ) -> PyResult<(Vec<PyObject>, IsComplete)> {
        let res = self
            .0
            .as_ref(py)
            .call_method1(intern!(py, "on_item"), (py_now, item))?;
        Self::extract_ret(res).reraise("error extracting `(emit, is_complete)`")
    }

    fn on_notify<'py>(
        &'py self,
        py: Python<'py>,
        sched: DateTime<Utc>,
    ) -> PyResult<(Vec<PyObject>, IsComplete)> {
        let res = self
            .0
            .as_ref(py)
            .call_method1(intern!(py, "on_notify"), (sched,))?;
        Self::extract_ret(res).reraise("error extracting `(emit, is_complete)`")
    }

    fn on_eof<'py>(&'py self, py: Python<'py>) -> PyResult<(Vec<PyObject>, IsComplete)> {
        let res = self.0.as_ref(py).call_method0("on_eof")?;
        Self::extract_ret(res).reraise("error extracting `(emit, is_complete)`")
    }

    fn notify_at(&self, py: Python) -> PyResult<Option<DateTime<Utc>>> {
        let res = self.0.as_ref(py).call_method0(intern!(py, "notify_at"))?;
        res.extract().reraise_with(|| {
            format!(
                "did not return a `datetime`; got a `{}` instead",
                unwrap_any!(res.get_type().name())
            )
        })
    }

    fn snapshot(&self, py: Python) -> PyResult<PyObject> {
        self.0.call_method0(py, intern!(py, "snapshot"))
    }
}

impl<S> UnaryOp<S> for Stream<S, TdPyAny>
where
    S: Scope<Timestamp = u64>,
{
    fn unary(
        &self,
        _py: Python,
        step_id: StepId,
        builder: TdPyCallable,
        resume_epoch: ResumeEpoch,
        loads: &Stream<S, Snapshot>,
    ) -> PyResult<(Stream<S, TdPyAny>, Stream<S, Snapshot>)> {
        let this_worker = self.scope().w_index();

        let loads = loads.filter_snaps(step_id.clone());
        // We have a "partition" per worker. List all workers.
        let workers = self.scope().w_count().iter().to_stream(&mut self.scope());
        // TODO: Could expose this above.
        let self_pf = BuildHasherDefault::<DefaultHasher>::default();
        let loads_pf = BuildHasherDefault::<DefaultHasher>::default();
        let partd_self = self.extract_key(step_id.clone()).partition(
            format!("{step_id}.self_partition"),
            &workers,
            self_pf,
        );
        let partd_loads = loads.partition(format!("{step_id}.load_partition"), &workers, loads_pf);

        let op_name = format!("{step_id}.stateful_unary");
        let mut op_builder = OperatorBuilder::new(op_name.clone(), self.scope());

        let (mut kv_downstream_output, kv_downstream) = op_builder.new_output();
        let (mut snaps_output, snaps) = op_builder.new_output();

        let mut input_handle = op_builder.new_input_connection(
            &partd_self,
            routed_exchange(),
            vec![Antichain::from_elem(0), Antichain::from_elem(0)],
        );
        let mut loads_handle = op_builder.new_input_connection(
            &partd_loads,
            routed_exchange(),
            vec![Antichain::from_elem(0), Antichain::from_elem(0)],
        );

        let info = op_builder.operator_info();
        let activator = self.scope().activator_for(&info.address[..]);

        let meter = opentelemetry::global::meter("bytewax");
        let item_inp_count = meter
            .u64_counter("item_inp_count")
            .with_description("number of items this step has ingested")
            .init();
        let item_out_count = meter
            .u64_counter("item_out_count")
            .with_description("number of items this step has emitted")
            .init();
        let on_item_histogram = meter
            .f64_histogram("unary_on_item_duration_seconds")
            .with_description("`UnaryLogic.on_item` duration in seconds")
            .init();
        let on_notify_histogram = meter
            .f64_histogram("unary_on_notify_duration_seconds")
            .with_description("`UnaryLogic.on_notify` duration in seconds")
            .init();
        let on_eof_histogram = meter
            .f64_histogram("unary_on_eof_duration_seconds")
            .with_description("`UnaryLogic.on_eof` duration in seconds")
            .init();
        let notify_at_histogram = meter
            .f64_histogram("unary_notify_at_duration_seconds")
            .with_description("`UnaryLogic.notify_at` duration in seconds")
            .init();
        let snapshot_histogram = meter
            .f64_histogram("snapshot_duration_seconds")
            .with_description("`snapshot` duration in seconds")
            .init();
        let labels = vec![
            KeyValue::new("step_id", step_id.0.to_string()),
            KeyValue::new("worker_index", this_worker.0.to_string()),
        ];

        op_builder.build(move |mut init_caps| {
            // We have to retain separate capabilities
            // per-output. This seems to be only documented in
            // https://github.com/TimelyDataflow/timely-dataflow/pull/187
            // In reverse order because of how [`Vec::pop`] removes
            // from back.
            let mut snap_cap = init_caps.pop();
            let mut kv_downstream_cap = init_caps.pop();

            // State for each key. There is only a single state for
            // each key representing the state at the frontier epoch;
            // we only modify state carefully in epoch order once we
            // know we won't be getting any input on closed epochs.
            let mut logics: BTreeMap<StateKey, UnaryLogic> = BTreeMap::new();
            // Contains the last known return value for
            // `logic.notify_at` for each key (if any). We don't
            // snapshot this because the logic itself should contain
            // any notify times within.
            let mut sched_cache: BTreeMap<StateKey, DateTime<Utc>> = BTreeMap::new();

            // Here we have "buffers" that store items across
            // activations.

            // Persistent across activations buffer keeping track of
            // out-of-order inputs. Push in here when Timely says we
            // have new data; pull out of here in epoch order to
            // process. This spans activations and will have epochs
            // removed from it as the input frontier progresses.
            let mut inbuf = InBuffer::new();
            let mut loads_inbuf = InBuffer::new();
            // Persistent across activations buffer of what keys were
            // awoken during the most recent epoch. This is used to
            // only snapshot state of keys that could have resulted in
            // state modifications. This is drained after each epoch
            // is processed.
            let mut awoken_keys_buffer: BTreeSet<StateKey> = BTreeSet::new();

            move |input_frontiers| {
                tracing::debug_span!("operator", operator = op_name).in_scope(|| {
                    if let (Some(output_cap), Some(state_update_cap)) =
                        (kv_downstream_cap.as_mut(), snap_cap.as_mut())
                    {
                        assert!(output_cap.time() == state_update_cap.time());

                        let now = chrono::offset::Utc::now();
                        // Do this once so we don't pay the conversion
                        // penalty for every Python method invocation.
                        let py_now = Python::with_gil(|py| now.into_py(py));

                        // Buffer the inputs so we can apply them to
                        // the state cache in epoch order.
                        input_handle.for_each(|cap, incoming| {
                            let epoch = cap.time();
                            inbuf.extend(*epoch, incoming);
                        });
                        loads_handle.for_each(|cap, incoming| {
                            let epoch = cap.time();
                            loads_inbuf.extend(*epoch, incoming);
                        });

                        let last_output_epoch = *output_cap.time();
                        let frontier_epoch = input_frontiers
                            .simplify()
                            // If we're at EOF and there's no "current
                            // epoch", use the last seen epoch to
                            // still allow output. EagerNotificator
                            // does not allow this.
                            .unwrap_or(last_output_epoch);

                        // Now let's find out which epochs we should
                        // wake up the logic for.
                        let mut process_epochs: BTreeSet<S::Timestamp> = BTreeSet::new();

                        // On the last activation, we eagerly executed
                        // the frontier at that time (which may or may
                        // not still be the frontier), even though it
                        // wasn't closed. Thus, we haven't run the
                        // "epoch closed" code yet. Make sure that
                        // close code is run, since we might not have
                        // any incoming items in that epoch in this
                        // activation.
                        process_epochs.insert(last_output_epoch);

                        // Try to process all the epochs we have input
                        // for.
                        process_epochs.extend(inbuf.epochs());
                        process_epochs.extend(loads_inbuf.epochs());

                        // Filter out epochs that are not closed; the
                        // state at the beginning of those epochs are
                        // not truly known yet, so we can't apply
                        // input in those epochs yet.
                        process_epochs.retain(|e| input_frontiers.is_closed(e));

                        // Except... eagerly execute the current
                        // frontier (even though it's not closed) as
                        // long as it could actually get data. All
                        // inputs will have a flash of their frontier
                        // being 0 before the resume epoch.
                        if frontier_epoch >= resume_epoch.0 {
                            process_epochs.insert(frontier_epoch);
                        }

                        let mut kv_downstream_handle = kv_downstream_output.activate();
                        let mut snaps_handle = snaps_output.activate();
                        // For each epoch in order.
                        for epoch in process_epochs {
                            tracing::trace!("Processing epoch {epoch:?}");
                            // Since the frontier has advanced to at
                            // least this epoch (because we're going
                            // through them in order), say that we'll
                            // not be sending output at any older
                            // epochs. This also asserts "apply
                            // changes in epoch order" to the state
                            // cache.
                            output_cap.downgrade(&epoch);
                            state_update_cap.downgrade(&epoch);

                            let mut kv_downstream_session =
                                kv_downstream_handle.session(&output_cap);

                            // First, call `on_item` for all the input
                            // items.
                            if let Some(items) = inbuf.remove(&epoch) {
                                item_inp_count.add(items.len() as u64, &labels);

                                unwrap_any!(Python::with_gil(|py| -> PyResult<()> {
                                    let builder = builder.as_ref(py);

                                    for (worker, (key, value)) in items {
                                        let pool = unsafe { py.new_pool() };
                                        let py = pool.python();

                                        let value = PyObject::from(value);

                                        assert!(worker == this_worker);
                                        // Ok, let's actually run the logic code!
                                        // Pull out or build the logic for the
                                        // current key.
                                        let logic =
                                            logics.entry(key.clone()).or_insert_with(|| {
                                                unwrap_any!((|| {
                                                    builder
                                                        .call1((py_now.clone_ref(py), None::<PyObject>))?
                                                        .extract::<UnaryLogic>()
                                                })(
                                                ))
                                            });

                                        let (output, is_complete) = with_timer!(
                                            on_item_histogram,
                                            labels,
                                            logic
                                                .on_item(py, py_now.clone_ref(py), value)
                                                .reraise_with(|| format!(
                                                    "error calling `UnaryLogic.on_item` in step {step_id} for key {key}"
                                                ))?
                                        );

                                        item_out_count.add(output.len() as u64, &labels);
                                        for value in output {
                                            kv_downstream_session.give((key.clone(), TdPyAny::from(value)));
                                        }

                                        if let IsComplete::Discard = is_complete {
                                            logics.remove(&key);
                                            sched_cache.remove(&key);
                                        }

                                        awoken_keys_buffer.insert(key);
                                    }

                                    Ok(())
                                }));
                            }

                            // Then call all logic that has a due
                            // notification.
                            let notify_keys: Vec<_> = sched_cache
                                .iter()
                                .filter(|(_key, sched)| **sched <= now)
                                .map(|(key, sched)| (key.clone(), *sched))
                                .collect();
                            if !notify_keys.is_empty() {
                                unwrap_any!(Python::with_gil(|py| -> PyResult<()> {
                                    for (key, sched) in notify_keys {
                                        let pool = unsafe { py.new_pool() };
                                        let py = pool.python();

                                        // We should always have a
                                        // logic for anything in
                                        // `sched_cache`. If not, we
                                        // forgot to remove it when we
                                        // cleared the logic.
                                        let logic = logics.get(&key).unwrap();

                                        let (output, is_complete) = with_timer!(
                                            on_notify_histogram,
                                            labels,
                                            logic.on_notify(py, sched).reraise_with(|| format!(
                                                "error calling `UnaryLogic.on_notify` in {step_id} for key {key}"
                                            ))?
                                        );

                                        item_out_count.add(output.len() as u64, &labels);
                                        for value in output {
                                            kv_downstream_session.give((key.clone(), TdPyAny::from(value)));
                                        }

                                        if let IsComplete::Discard = is_complete {
                                            logics.remove(&key);
                                            sched_cache.remove(&key);
                                        } else {
                                            // Even if we don't
                                            // discard the logic, the
                                            // previous scheduled
                                            // notification only
                                            // should fire once. The
                                            // logic can re-schedule
                                            // it by still returning
                                            // it in `notify_at`.
                                            sched_cache.remove(&key);
                                        }

                                        awoken_keys_buffer.insert(key);
                                    }

                                    Ok(())
                                }));
                            }

                            // Then if EOF, call all logic that still
                            // exists.
                            if input_frontiers.is_eof() {
                                let mut discarded_keys = Vec::new();

                                unwrap_any!(Python::with_gil(|py| -> PyResult<()> {
                                    for (key, logic) in logics.iter() {
                                        let pool = unsafe { py.new_pool() };
                                        let py = pool.python();

                                        let (output, is_complete) = with_timer!(
                                            on_eof_histogram,
                                            labels,
                                            logic.on_eof(py).reraise_with(|| format!(
                                                "error calling `UnaryLogic.on_eof` in {step_id} for key {key}"
                                            ))?
                                        );

                                        item_out_count.add(output.len() as u64, &labels);
                                        for value in output {
                                            kv_downstream_session.give((key.clone(), TdPyAny::from(value)));
                                        }

                                        if let IsComplete::Discard = is_complete {
                                            discarded_keys.push(key.clone());
                                        }

                                        awoken_keys_buffer.insert(key.clone());
                                    }

                                    Ok(())
                                }));

                                for key in discarded_keys {
                                    logics.remove(&key);
                                    sched_cache.remove(&key);
                                }
                            }

                            // Then go through all awoken keys and
                            // update the next scheduled notification
                            // times.
                            if !awoken_keys_buffer.is_empty() {
                                unwrap_any!(Python::with_gil(|py| -> PyResult<()> {
                                    for key in awoken_keys_buffer.iter() {
                                        let pool = unsafe { py.new_pool() };
                                        let py = pool.python();

                                        // It's possible the logic was
                                        // discarded on a previous
                                        // activation but the epoch
                                        // hasn't ended so the key is
                                        // still in
                                        // `awoken_keys_buffer`.
                                        if let Some(logic) = logics.get(key) {
                                            let sched = with_timer!(
                                                notify_at_histogram,
                                                labels,
                                                logic.notify_at(py).reraise_with(|| {
                                                    format!("error calling `UnaryLogic.notify_at` in {step_id} for key {key}")
                                                })?
                                            );
                                            if let Some(sched) = sched {
                                                sched_cache.insert(key.clone(), sched);
                                            }
                                        }
                                    }

                                    Ok(())
                                }));
                            }

                            // Snapshot and output state changes.
                            if input_frontiers.is_closed(&epoch) {
                                // Snapshot before loads. If we have an
                                // incoming load, it means we have
                                // recovery state already at the end of
                                // the epoch.

                                let mut snaps_session = snaps_handle.session(&state_update_cap);

                                // Go through all keys awoken in this
                                // epoch. This might involve keys from the
                                // previous activation.
                                unwrap_any!(Python::with_gil(|py| -> PyResult<()> {
                                    // Finally drain
                                    // `awoken_keys_buffer` since the
                                    // epoch is over.
                                    for key in std::mem::take(&mut awoken_keys_buffer) {
                                        let pool = unsafe { py.new_pool() };
                                        let py = pool.python();

                                        let change = if let Some(logic) = logics.get(&key) {
                                            let state = with_timer!(
                                                snapshot_histogram,
                                                labels,
                                                logic.snapshot(py).reraise_with(|| {
                                                    format!("error calling `UnaryLogic.snapshot` in {step_id} for key {key}")
                                                })?
                                            );
                                            StateChange::Upsert(TdPyAny::from(state))
                                        } else {
                                            // It's ok if there's no
                                            // logic, because during
                                            // this epoch it might
                                            // have been discarded due
                                            // to one of the `on_*`
                                            // methods returning
                                            // `IsComplete::Discard`.
                                            StateChange::Discard
                                        };
                                        let snap = Snapshot(step_id.clone(), key, change);
                                        snaps_session.give(snap);
                                    }

                                    Ok(())
                                }));

                                if let Some(loads) = loads_inbuf.remove(&epoch) {
                                    unwrap_any!(Python::with_gil(|py| -> PyResult<()> {
                                        let builder = builder.as_ref(py);

                                        for (worker, (key, change)) in loads {
                                            let pool = unsafe { py.new_pool() };
                                            let py = pool.python();

                                            tracing::trace!(
                                                "Got load for {key:?} during epoch {epoch:?}"
                                            );
                                            assert!(worker == this_worker);
                                            match change {
                                                StateChange::Upsert(state) => {
                                                    let logic = builder
                                                        .call1((py_now.clone_ref(py), Some(state),))?
                                                        .extract::<UnaryLogic>()?;
                                                    if let Some(notify_at) = logic.notify_at(py)? {
                                                        sched_cache.insert(key.clone(), notify_at);
                                                    }
                                                    logics.insert(key, logic);
                                                }
                                                StateChange::Discard => {
                                                    logics.remove(&key);
                                                    sched_cache.remove(&key);
                                                }
                                            }
                                        }

                                        Ok(())
                                    }));
                                }
                            }
                        }

                        let load_frontier = &input_frontiers[1];
                        if load_frontier.is_eof() {
                            // Since we might emit downstream without any incoming
                            // items, like on window timeout, ensure we FFWD to the
                            // resume epoch.
                            init_caps.downgrade_all(&resume_epoch.0);
                        }

                        // Schedule operator activation at the soonest
                        // requested notify at for any key.
                        if let Some(next_notify_at) =
                            sched_cache.values().map(|notify_at| *notify_at - now).min()
                        {
                            activator.activate_after(
                                next_notify_at.to_std().unwrap_or(std::time::Duration::ZERO),
                            );
                        }
                    }

                    if input_frontiers.is_eof() {
                        kv_downstream_cap = None;
                        snap_cap = None;
                    }
                });
            }
        });

        let downstream = kv_downstream.wrap_key();

        Ok((downstream, snaps))
    }
}
