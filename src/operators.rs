//! Code implementing Bytewax's core operators.

use std::cell::RefCell;
use std::collections::hash_map::DefaultHasher;
use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::hash::BuildHasherDefault;
use std::rc::Rc;

use chrono::DateTime;
use chrono::Utc;
use opentelemetry::KeyValue;
use pyo3::exceptions::PyTypeError;
use pyo3::intern;
use pyo3::prelude::*;
use timely::dataflow::channels::pact::Pipeline;
use timely::dataflow::operators::generic::builder_rc::OperatorBuilder;
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

pub(crate) trait BranchOp<S>
where
    S: Scope,
{
    fn branch(
        &self,
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
        step_id: StepId,
        predicate: TdPyCallable,
    ) -> PyResult<(Stream<S, TdPyAny>, Stream<S, TdPyAny>)> {
        let mut op_builder = OperatorBuilder::new(format!("{step_id}.branch"), self.scope());

        let mut self_handle = op_builder.new_input(self, Pipeline);
        let (mut trues_output, trues) = op_builder.new_output();
        let (mut falses_output, falses) = op_builder.new_output();

        op_builder.build(move |_| {
            let mut inbuf = Vec::new();
            move |_frontiers| {
                let mut trues_handle = trues_output.activate();
                let mut falses_handle = falses_output.activate();

                Python::with_gil(|py| {
                    self_handle.for_each(|time, data| {
                        data.swap(&mut inbuf);
                        let mut trues_session = trues_handle.session(&time);
                        let mut falses_session = falses_handle.session(&time);
                        let pred = predicate.bind(py);
                        unwrap_any!(|| -> PyResult<()> {
                            for item in inbuf.drain(..) {
                                let res = pred
                                    .call1((item.bind(py),))
                                    .reraise_with(|| {
                                        format!("error calling predicate in step {step_id}")
                                    })?
                                    .extract::<bool>()
                                    .reraise_with(|| {
                                        format!(
                                        "return value of `predicate` in step {step_id} must be a `bool`"
                                    )
                                    })?;
                                if res {
                                    trues_session.give(item);
                                } else {
                                    falses_session.give(item);
                                }
                            }
                            Ok(())
                        }());
                    })
                });
            }
        });

        Ok((trues, falses))
    }
}

fn next_batch(
    outbuf: &mut Vec<TdPyAny>,
    mapper: &Bound<'_, PyAny>,
    in_batch: Vec<PyObject>,
) -> PyResult<()> {
    let res = mapper.call1((in_batch,)).reraise("error calling mapper")?;
    let iter = res.iter().reraise_with(|| {
        format!(
            "mapper must return an iterable; got a `{}` instead",
            unwrap_any!(res.get_type().name()),
        )
    })?;
    for res in iter {
        let out_item = res.reraise("error while iterating through batch")?;
        outbuf.push(out_item.into());
    }

    Ok(())
}

pub(crate) trait FlatMapBatchOp<S>
where
    S: Scope,
    S::Timestamp: TotalOrder,
{
    fn flat_map_batch(
        &self,
        py: Python,
        step_id: StepId,
        mapper: TdPyCallable,
    ) -> PyResult<Stream<S, TdPyAny>>;
}

impl<S> FlatMapBatchOp<S> for Stream<S, TdPyAny>
where
    S: Scope,
    S::Timestamp: TotalOrder,
{
    fn flat_map_batch(
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
            .f64_histogram("flat_map_batch_duration_seconds")
            .with_description("`flat_map_batch` `mapper` duration in seconds")
            .init();
        let labels = vec![
            KeyValue::new("step_id", step_id.0.to_string()),
            KeyValue::new("worker_index", this_worker.0.to_string()),
        ];

        op_builder.build(move |init_caps| {
            let mut inbuf = InBuffer::new();
            let mut ncater = EagerNotificator::new(init_caps, ());
            // Timely forcibly flushes buffers when using
            // `Session::give_vec` so we need to build up a buffer of
            // items to reduce the overhead of that. It's fine that
            // this is effectively 'static because Timely swaps this
            // out under the covers so it doesn't end up growing
            // without bound.
            let mut outbuf = Vec::new();

            move |input_frontiers| {
                tracing::debug_span!("operator", operator = step_id.0.clone()).in_scope(|| {
                    self_handle.buffer_notify(&mut inbuf, &mut ncater);

                    let mut downstream_handle = downstream_output.activate();
                    ncater.for_each(
                        input_frontiers,
                        |caps, ()| {
                            let cap = &caps[0];
                            let epoch = cap.time();

                            if let Some(batch) = inbuf.remove(epoch) {
                                item_inp_count.add(batch.len() as u64, &labels);
                                let mut downstream_session = downstream_handle.session(cap);

                                unwrap_any!(Python::with_gil(|py| -> PyResult<()> {
                                    let batch: Vec<_> =
                                        batch.into_iter().map(PyObject::from).collect();
                                    let mapper = mapper.bind(py);

                                    with_timer!(
                                        mapper_histogram,
                                        labels,
                                        next_batch(&mut outbuf, mapper, batch).reraise_with(
                                            || {
                                                format!("error calling `mapper` in step {step_id}")
                                            }
                                        )?
                                    );

                                    item_out_count.add(outbuf.len() as u64, &labels);
                                    downstream_session.give_vec(&mut outbuf);

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
                                    let inspector = inspector.bind(py);

                                    for item in items.iter() {
                                        let item = item.bind(py);

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
        let mut op_builder =
            OperatorBuilder::new(format!("{for_step_id}.extract_key"), self.scope());
        let mut self_handle = op_builder.new_input(self, Pipeline);

        let (mut downstream_output, downstream) = op_builder.new_output();

        op_builder.build(move |_| {
            let mut inbuf = Vec::new();
            move |_frontiers| {
                let mut downstream_handle = downstream_output.activate();

                Python::with_gil(|py| {
                    self_handle.for_each(|time, data| {
                        data.swap(&mut inbuf);
                        let mut downstream_session = downstream_handle.session(&time);
                        unwrap_any!(|| -> PyResult<()> {
                            for item in inbuf.drain(..) {
                                let item = PyObject::from(item);
                                let (key, value) = item
                                    .extract::<(&PyAny, PyObject)>(py)
                                    .raise_with::<PyTypeError>(|| {
                                        format!("step {for_step_id} requires `(key, value)` 2-tuple from upstream for routing; got a `{}` instead",
                                            unwrap_any!(item.bind(py).get_type().name()),
                                        )
                                    })?;

                                let key = key.extract::<StateKey>().raise_with::<PyTypeError>(|| {
                                    format!("step {for_step_id} requires `str` keys in `(key, value)` from upstream; got a `{}` instead",
                                        unwrap_any!(key.get_type().name()),
                                    )
                                })?;
                                downstream_session.give((key, TdPyAny::from(value)));
                            }
                            Ok(())
                        }());
                    });
                });
            }
        });
        downstream
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

pub(crate) trait StatefulBatchOp<S>
where
    S: Scope<Timestamp = u64>,
{
    fn stateful_batch(
        &self,
        py: Python,
        step_id: StepId,
        state: StatefulBatchState,
    ) -> PyResult<(Stream<S, TdPyAny>, Stream<S, SerializedSnapshot>)>;
}

pub(crate) struct StatefulBatchLogic(PyObject);

/// Do some eager type checking.
impl<'py> FromPyObject<'py> for StatefulBatchLogic {
    fn extract_bound(ob: &Bound<'py, PyAny>) -> PyResult<Self> {
        let py = ob.py();
        let abc = py
            .import_bound("bytewax.operators")?
            .getattr("StatefulBatchLogic")?;
        if !ob.is_instance(&abc)? {
            Err(PyTypeError::new_err(
                "logic must subclass `bytewax.operators.StatefulBatchLogic`",
            ))
        } else {
            Ok(Self(ob.to_object(py)))
        }
    }
}

pub(crate) enum IsComplete {
    Retain,
    Discard,
}

impl<'py> FromPyObject<'py> for IsComplete {
    fn extract_bound(ob: &Bound<'py, PyAny>) -> PyResult<Self> {
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

impl StatefulBatchLogic {
    fn extract_ret(res: Bound<'_, PyAny>) -> PyResult<(Vec<PyObject>, IsComplete)> {
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
}

pub(crate) struct StatefulBatchState {
    step_id: StepId,

    // Shared references to LocalStateStore and StateStoreCache
    local_state_store: Option<Rc<RefCell<LocalStateStore>>>,
    state_store_cache: Rc<RefCell<StateStoreCache>>,

    // Contains the last known return value for
    // `logic.notify_at` for each key (if any). We don't
    // snapshot this because the logic itself should contain
    // any notify times within.
    sched_cache: BTreeMap<StateKey, DateTime<Utc>>,

    // Awoken keys in this activation
    awoken_keys_this_activation: BTreeSet<StateKey>,
    awoken_keys_this_epoch_buffer: BTreeSet<StateKey>,

    /// Vec only used to temporarily hold snapshots.
    /// We keep this to avoid having to allocate a new vector
    /// each time we take snapshots.
    snaps_buf: Vec<SerializedSnapshot>,
}

impl StatefulBatchState {
    pub(crate) fn init(
        py: Python,
        step_id: StepId,
        local_state_store: Option<Rc<RefCell<LocalStateStore>>>,
        state_store_cache: Rc<RefCell<StateStoreCache>>,
        logic_builder: TdPyCallable,
    ) -> PyResult<Self> {
        let builder = move |py: Python<'_>, _key, state| {
            logic_builder
                .bind(py)
                .call1((state,))?
                .extract::<StatefulBatchLogic>()
                .map(|result| result.0)
        };
        state_store_cache
            .borrow_mut()
            .add_step(step_id.clone(), Box::new(builder));

        let mut this = Self {
            step_id,
            local_state_store,
            state_store_cache,
            sched_cache: BTreeMap::new(),
            awoken_keys_this_activation: BTreeSet::new(),
            awoken_keys_this_epoch_buffer: BTreeSet::new(),
            snaps_buf: vec![],
        };

        if let Some(lss) = this.local_state_store.as_ref() {
            let snaps = lss.borrow_mut().get_snaps(py, &this.step_id)?;
            for (state_key, state) in snaps {
                if state.is_some() {
                    this.insert(py, state_key.clone(), state);
                    if let Some(notify_at) = this.notify_at(py, &state_key)? {
                        this.schedule(state_key, notify_at);
                    }
                }
            }
        }
        Ok(this)
    }

    pub fn start_at(&self) -> ResumeEpoch {
        if let Some(lss) = self.local_state_store.as_ref() {
            lss.borrow().resume_from_epoch()
        } else {
            ResumeFrom::default().epoch()
        }
    }

    pub fn insert(&mut self, py: Python, state_key: StateKey, state: Option<PyObject>) {
        self.state_store_cache
            .borrow_mut()
            .insert(py, &self.step_id, state_key, state);
    }

    pub fn awoken(&self) -> Vec<StateKey> {
        self.awoken_keys_this_activation.iter().cloned().collect()
    }

    pub fn remove(&mut self, key: &StateKey) {
        self.state_store_cache
            .borrow_mut()
            .remove(&self.step_id, key)
            .unwrap();
        self.sched_cache.remove(key);
    }

    pub fn notify_keys(&self, now: DateTime<Utc>) -> Vec<StateKey> {
        self.sched_cache
            .iter()
            .filter(|(_key, sched)| **sched <= now)
            .map(|(key, _sched)| key.clone())
            .collect()
    }

    pub fn schedule(&mut self, key: StateKey, time: DateTime<Utc>) {
        self.sched_cache.insert(key, time);
    }

    pub fn unschedule(&mut self, key: &StateKey) {
        self.sched_cache.remove(key);
    }

    pub fn activate_after(&self, now: DateTime<Utc>) -> Option<std::time::Duration> {
        self.sched_cache
            .values()
            .map(|notify_at| {
                (*notify_at - now)
                    .to_std()
                    .unwrap_or(std::time::Duration::ZERO)
            })
            .min()
    }

    pub fn contains_key(&self, key: &StateKey) -> bool {
        self.state_store_cache
            .borrow()
            .contains_key(&self.step_id, key)
    }

    pub fn keys(&self) -> Vec<StateKey> {
        self.state_store_cache.borrow().keys(&self.step_id)
    }

    fn on_batch(
        &mut self,
        py: Python,
        key: &StateKey,
        values: Vec<PyObject>,
    ) -> PyResult<Vec<PyObject>> {
        // Mark this key as being awoken in this activation
        self.awoken_keys_this_activation.insert(key.clone());

        // Call user's code to get the result
        let res = self
            .state_store_cache
            .borrow()
            .get(&self.step_id, key)
            .unwrap()
            .bind(py)
            .call_method1(intern!(py, "on_batch"), (values,))
            .reraise_with(|| {
                format!(
                    "error calling `StatefulBatch.on_batch` in step {} for key {}",
                    self.step_id, &key
                )
            })?;

        // Do the type checking
        let (output, is_complete) = StatefulBatchLogic::extract_ret(res)
            .reraise("error extracting `(emit, is_complete)`")?;

        // Remove the state if needed.
        if matches!(is_complete, IsComplete::Discard) {
            self.remove(key);
        }

        Ok(output)
    }

    fn on_notify(&mut self, py: Python, key: &StateKey) -> PyResult<Vec<PyObject>> {
        // Mark this key as being awoken in this activation
        self.awoken_keys_this_activation.insert(key.clone());

        // Call user's code to get next scheduling time
        let res = self
            .state_store_cache
            .borrow()
            .get(&self.step_id, key)
            .unwrap()
            .bind(py)
            .call_method0(intern!(py, "on_notify"))
            .reraise_with(|| {
                format!(
                    "error calling `StatefulBatchLogic.on_notify` in {} for key {}",
                    self.step_id, key
                )
            })?;

        // Do the type checking
        let (output, is_complete) = StatefulBatchLogic::extract_ret(res)
            .reraise("error extracting `(emit, is_complete)`")?;

        // Clear the state if needed
        if matches!(is_complete, IsComplete::Discard) {
            self.remove(key);
        } else {
            // Even if we don't discard the logic, the previous
            // scheduled notification only should fire once. The
            // logic can re-schedule it by still returning it
            // in `notify_at`.
            self.unschedule(key);
        }

        Ok(output)
    }

    fn on_eof(&mut self, py: Python, key: &StateKey) -> PyResult<Vec<PyObject>> {
        // Mark this key as being awoken in this activation
        self.awoken_keys_this_activation.insert(key.clone());

        // Call user's code for the closing logic
        let res = self
            .state_store_cache
            .borrow()
            .get(&self.step_id, key)
            .unwrap()
            .bind(py)
            .call_method0("on_eof")
            .reraise_with(|| {
                format!(
                    "error calling `StatefulBatchLogic.on_eof` in {} for key {}",
                    self.step_id, key
                )
            })?;

        // Do the type checking
        let (output, is_complete) = StatefulBatchLogic::extract_ret(res)
            .reraise("error extracting `(emit, is_complete)`")?;

        // Discard and unschedule if needed.
        if let IsComplete::Discard = is_complete {
            self.remove(key);
        }

        Ok(output)
    }

    fn notify_at(&self, py: Python, key: &StateKey) -> PyResult<Option<DateTime<Utc>>> {
        if let Some(logic) = self.state_store_cache.borrow().get(&self.step_id, key) {
            let res = logic
                .bind(py)
                .call_method0(intern!(py, "notify_at"))
                .reraise_with(|| {
                    format!(
                        "error calling `StatefulBatchLogic.notify_at` in {} for key {}",
                        self.step_id, key
                    )
                })?;
            res.extract().reraise_with(|| {
                format!(
                    "did not return a `datetime`; got a `{}` instead",
                    unwrap_any!(res.get_type().name())
                )
            })
        } else {
            Ok(None)
        }
    }

    fn snapshots(
        &mut self,
        py: Python,
        epoch: u64,
        is_epoch_closed: bool,
    ) -> Vec<SerializedSnapshot> {
        // This function should be called at each activation, we then decide
        // if we take the snapshots immediately or wait for the epoch to close.
        // We do always take out keys from self.awoken_keys_this_activation into
        // self.awoken_keys_this_epoch_buffer to keep track of what we need to
        // snapshot in case we are not doing it immediately.
        self.awoken_keys_this_epoch_buffer
            .extend(std::mem::take(&mut self.awoken_keys_this_activation));

        if let Some(lss) = self.local_state_store.as_ref() {
            if lss.borrow().snapshot_mode().immediate() || is_epoch_closed {
                // Reuse the buffer vector, clone it for the local_state_store
                // then immediately empty it again for the operator.
                // This saves us a number of allocations every time this function
                // is called.
                self.snaps_buf.extend(
                    std::mem::take(&mut self.awoken_keys_this_epoch_buffer)
                        .into_iter()
                        .map(|key| {
                            // Here is where we finally take the snapshot
                            self.state_store_cache
                                .borrow()
                                .snap(py, self.step_id.clone(), key, epoch)
                                .reraise("Error snapshotting PartitionedInput")
                                .unwrap()
                        }),
                );
                lss.borrow_mut().write_snapshots(self.snaps_buf.clone());
            }
        }
        std::mem::take(&mut self.snaps_buf)
    }
}

impl<S> StatefulBatchOp<S> for Stream<S, TdPyAny>
where
    S: Scope<Timestamp = u64>,
{
    fn stateful_batch(
        &self,
        _py: Python,
        step_id: StepId,
        mut state: StatefulBatchState,
    ) -> PyResult<(Stream<S, TdPyAny>, Stream<S, SerializedSnapshot>)> {
        let this_worker = self.scope().w_index();

        // We have a "partition" per worker. List all workers.
        let workers = self.scope().w_count().iter().to_stream(&mut self.scope());
        // TODO: Could expose this above.
        let self_pf = BuildHasherDefault::<DefaultHasher>::default();
        let partd_self = self.extract_key(step_id.clone()).partition(
            format!("{step_id}.self_partition"),
            &workers,
            self_pf,
        );

        let op_name = format!("{step_id}.stateful_batch");
        let mut op_builder = OperatorBuilder::new(op_name.clone(), self.scope());

        let (mut kv_downstream_output, kv_downstream) = op_builder.new_output();
        let (mut snaps_output, snaps) = op_builder.new_output();

        let mut input_handle = op_builder.new_input_connection(
            &partd_self,
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
        let on_batch_histogram = meter
            .f64_histogram("stateful_batch_on_batch_duration_seconds")
            .with_description("`StatefulBatchLogic.on_batch` duration in seconds")
            .init();
        let on_notify_histogram = meter
            .f64_histogram("stateful_batch_on_notify_duration_seconds")
            .with_description("`StatefulBatchLogic.on_notify` duration in seconds")
            .init();
        let on_eof_histogram = meter
            .f64_histogram("stateful_batch_on_eof_duration_seconds")
            .with_description("`StatefulBatchLogic.on_eof` duration in seconds")
            .init();
        let notify_at_histogram = meter
            .f64_histogram("stateful_batch_notify_at_duration_seconds")
            .with_description("`StatefulBatchLogic.notify_at` duration in seconds")
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
            // In reverse order because of how [`Vec::pop`] removes from back.
            let mut snap_cap = init_caps.pop();
            let mut kv_downstream_cap = init_caps.pop();

            // Persistent across activations buffer keeping track of
            // out-of-order inputs. Push in here when Timely says we
            // have new data; pull out of here in epoch order to
            // process. This spans activations and will have epochs
            // removed from it as the input frontier progresses.
            let mut inbuf = InBuffer::new();

            // Persistent across activation buffer of items.
            // This is actually cleared every time we send items
            // downstream, but it will keep the maximum capacity allocated
            // so we can avoid having to allocate a new vector each time.
            // We could also use `give_iterator` rather than `give_vec`,
            // but we wouldn't have control over the size of the
            // internal buffer timely uses, and thus when it is flushed.
            let mut outbuf = Vec::new();

            move |input_frontiers| {
                let _guard = tracing::debug_span!("operator", operator = op_name).entered();

                // If the output capabilities have been dropped, do nothing here.
                if kv_downstream_cap.is_none() || snap_cap.is_none() {
                    // Make sure that if the input frontiers are closed,
                    // output capabilities are dropped too.
                    if input_frontiers.is_eof() {
                        kv_downstream_cap = None;
                        snap_cap = None;
                    }
                    return;
                }

                // We can unwrap here since we just checked.
                let output_cap = kv_downstream_cap.as_mut().unwrap();
                let state_update_cap = snap_cap.as_mut().unwrap();

                // Start the processing logic. Both outputs should be in sync.
                assert!(output_cap.time() == state_update_cap.time());

                let now = chrono::offset::Utc::now();

                // Buffer the inputs so we can apply them to the state cache in epoch order.
                input_handle.for_each(|cap, incoming| {
                    let epoch = cap.time();
                    inbuf.extend(*epoch, incoming);
                });

                let last_output_epoch = *output_cap.time();
                let frontier_epoch = input_frontiers
                    .simplify()
                    // If we're at EOF and there's no "current epoch", use the last seen
                    // epoch to still allow output. EagerNotificator does not allow this.
                    .unwrap_or(last_output_epoch);

                // Now let's find out which epochs we should wake up the logic for.
                let mut process_epochs: BTreeSet<S::Timestamp> = BTreeSet::new();

                // On the last activation, we eagerly executed the frontier at that time
                // (which may or may not still be the frontier), even though it wasn't
                // closed. Thus, we haven't run the "epoch closed" code yet. Make sure
                // that close code is run, since we might not have any incoming items in
                // that epoch in this activation.
                process_epochs.insert(last_output_epoch);

                // Try to process all the epochs we have input for.
                process_epochs.extend(inbuf.epochs());

                // Filter out epochs that are not closed; the state at the beginning of
                // those epochs are not truly known yet, so we can't apply input in those
                // epochs yet.
                process_epochs.retain(|e| input_frontiers.is_closed(e));

                // Except... eagerly execute the current frontier (even though it's not
                // closed) as long as it could actually get data. All inputs will have a
                // flash of their frontier being 0 before the resume epoch.
                if frontier_epoch >= state.start_at().0 {
                    process_epochs.insert(frontier_epoch);
                }

                let mut kv_downstream_handle = kv_downstream_output.activate();
                let mut snaps_handle = snaps_output.activate();

                // For each epoch in order.
                for epoch in process_epochs {
                    tracing::trace!("Processing epoch {epoch:?}");

                    // Since the frontier has advanced to at least this epoch (because
                    // we're going through them in order), say that we'll not be sending
                    // output at any older epochs. This also asserts "apply changes in
                    // epoch order" to the state cache.
                    output_cap.downgrade(&epoch);
                    state_update_cap.downgrade(&epoch);

                    let mut kv_downstream_session = kv_downstream_handle.session(&output_cap);

                    // First, call `on_batch` for all the input items.
                    if let Some(items) = inbuf.remove(&epoch) {
                        item_inp_count.add(items.len() as u64, &labels);

                        // Check that the worker is the right one for each item,
                        // then convert it to PyObject and save it into a temporary map.
                        let mut keyed_items: BTreeMap<StateKey, Vec<PyObject>> = BTreeMap::new();
                        for (worker, (key, value)) in items {
                            assert!(worker == this_worker);
                            keyed_items
                                .entry(key)
                                .or_default()
                                .push(PyObject::from(value));
                        }

                        // Ok, now let's actually run the logic code for each item!
                        Python::with_gil(|py| {
                            for (key, values) in keyed_items {
                                // Build the logic if it wasn't in the state yet.
                                if !state.contains_key(&key) {
                                    state.insert(py, key.clone(), None);
                                }

                                // Run the logic's on_batch function
                                let output = with_timer!(
                                    on_batch_histogram,
                                    labels,
                                    unwrap_any!(state.on_batch(py, &key, values))
                                );

                                // Update metrics
                                item_out_count.add(output.len() as u64, &labels);

                                // And send the output downstream.
                                // Reuse the already allocated buffer,
                                // then let timely empty it in `give_vec`.
                                outbuf.extend(
                                    output
                                        .into_iter()
                                        .map(|value| (key.clone(), TdPyAny::from(value))),
                                );
                                kv_downstream_session.give_vec(&mut outbuf);
                            }
                        });
                    }

                    // Then call all logic that has a due notification.
                    Python::with_gil(|py| {
                        for key in state.notify_keys(now) {
                            let output = with_timer!(
                                on_notify_histogram,
                                labels,
                                unwrap_any!(state.on_notify(py, &key))
                            );

                            // Update metrics
                            item_out_count.add(output.len() as u64, &labels);

                            // Send the output downstream.
                            // Reuse the already allocated buffer,
                            // then let timely empty it in `give_vec`.
                            outbuf.extend(
                                output
                                    .into_iter()
                                    .map(|value| (key.clone(), TdPyAny::from(value))),
                            );
                            kv_downstream_session.give_vec(&mut outbuf);
                        }
                    });

                    // Then if EOF, call all logic that still exists.
                    if input_frontiers.is_eof() {
                        Python::with_gil(|py| {
                            for key in state.keys() {
                                let output = with_timer!(
                                    on_eof_histogram,
                                    labels,
                                    unwrap_any!(state.on_eof(py, &key))
                                );

                                // Update metrics.
                                item_out_count.add(output.len() as u64, &labels);

                                // Send items downstream.
                                // Reuse the already allocated buffer,
                                // then let timely empty it in `give_vec`.
                                outbuf.extend(
                                    output
                                        .into_iter()
                                        .map(|value| (key.clone(), TdPyAny::from(value))),
                                );
                                kv_downstream_session.give_vec(&mut outbuf);
                            }
                        });
                    }

                    // Then go through all awoken keys and update the next scheduled
                    // notification times.
                    Python::with_gil(|py| {
                        for key in state.awoken() {
                            if let Some(sched) = with_timer!(
                                notify_at_histogram,
                                labels,
                                unwrap_any!(state.notify_at(py, &key))
                            ) {
                                state.schedule(key.clone(), sched);
                            }
                        }
                    });

                    // Snapshot and output state changes.
                    let is_epoch_closed = input_frontiers.is_closed(&epoch);
                    let mut snapshots = Python::with_gil(|py| {
                        with_timer!(
                            snapshot_histogram,
                            labels,
                            state.snapshots(py, epoch, is_epoch_closed)
                        )
                    });

                    if !snapshots.is_empty() {
                        let mut snaps_session = snaps_handle.session(&state_update_cap);
                        snaps_session.give_vec(&mut snapshots);
                    }
                }

                // Schedule operator activation at the soonest requested notify at for any key.
                if let Some(activate_after) = state.activate_after(now) {
                    activator.activate_after(activate_after);
                }

                if input_frontiers.is_eof() {
                    kv_downstream_cap = None;
                    snap_cap = None;
                }
            }
        });

        let downstream = kv_downstream.wrap_key();

        Ok((downstream, snaps))
    }
}
