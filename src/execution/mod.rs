#![allow(non_snake_case)]

use pyo3::exceptions::PyStopIteration;
use pyo3::iter::IterNextOutput;
use send_wrapper::SendWrapper;
use std::collections::HashMap;

use pyo3::exceptions::{PyRuntimeError, PyValueError};
use pyo3::prelude::*;
use timely::communication::allocator::zero_copy::initialize::CommsGuard as TCommsGuard;
use timely::communication::allocator::GenericBuilder as TBuilder;
use timely::dataflow::InputHandle;
use timely::dataflow::ProbeHandle;

use crate::dataflow::Dataflow;
use crate::pyo3_extensions::*;

type TWorker = timely::worker::Worker<timely::communication::allocator::generic::Generic>;

/// Encapsulates the process of pulling data out of the input Python
/// iterator and feeding it into Timely.
///
/// This will be called in the worker's "main" loop to feed data in.
struct Pump {
    pull_from_pyiter: TdPyIterator,
    pyiter_is_empty: bool,
    push_to_timely: InputHandle<u64, TdPyAny>,
}

impl Pump {
    fn new(pull_from_pyiter: TdPyIterator, push_to_timely: InputHandle<u64, TdPyAny>) -> Self {
        Self {
            pull_from_pyiter,
            pyiter_is_empty: false,
            push_to_timely,
        }
    }

    /// Take a single data element and timestamp and feed it into the
    /// dataflow.
    fn pump(&mut self, py: Python) -> PyResult<()> {
        let mut pull_from_pyiter = self.pull_from_pyiter.0.as_ref(py);
        if let Some(epoch_item_pytuple) = pull_from_pyiter.next() {
            let (epoch, item) = epoch_item_pytuple?.extract()?;
            self.push_to_timely.advance_to(epoch);
            self.push_to_timely.send(item);
        } else {
            self.pyiter_is_empty = true;
        }
        Ok(())
    }

    fn input_remains(&self) -> bool {
        !self.pyiter_is_empty
    }
}

/// Coroutine representing a worker thread's "main" loop.
///
/// Can be `await`ed from an async function, passed to `asyncio.run()`
/// to block and run, or stepped through via `send()`.
///
/// Created via `WorkerBuilder.build()` in the final execution
/// thread. Do not use from other threads.
#[pyclass(unsendable)]
struct WorkerCoro {
    /// None if this worker panic'd and is poisoned and thus should
    /// not be used again. Use SendWrapper because of py.allow_threads
    /// requiring Send, not because we actually are sending between
    /// threads; could be dropped once
    /// https://github.com/PyO3/pyo3/issues/2141 is done.
    timely_worker: Option<SendWrapper<TWorker>>,
    input_pumps: Vec<Pump>,
    output_probes: Vec<ProbeHandle<u64>>,
    should_stop: TdPyCallable,
}

impl WorkerCoro {
    /// Compile a Bytewax dataflow into a Timely dataflow.
    fn compile(
        py: Python,
        mut timely_worker: SendWrapper<TWorker>,
        flow: &Dataflow,
        input_builder: TdPyCallable,
        output_builder: TdPyCallable,
        should_stop: TdPyCallable,
    ) -> PyResult<Self> {
        use timely::dataflow::operators::aggregation::*;
        use timely::dataflow::operators::*;

        use crate::dataflow::Step;
        use crate::operators::*;

        let worker_index = timely_worker.index();
        let worker_count = timely_worker.peers();

        let worker_input: TdPyIterator = input_builder
            .call1(py, (worker_index, worker_count))
            .unwrap()
            .extract(py)
            .unwrap();
        let mut timely_input = InputHandle::new();

        let worker_output: TdPyCallable = output_builder
            .call1(py, (worker_index, worker_count))
            .unwrap()
            .extract(py)
            .unwrap();
        let mut output_probes = Vec::new();

        timely_worker.dataflow(|scope| {
            let mut stream = timely_input.to_stream(scope);
            let steps = &flow.steps;
            for step in steps {
                match step {
                    Step::Map { mapper } => {
                        // All these closure lifetimes are static, so tell
                        // Python's GC that there's another pointer to the
                        // mapping function that's going to hang around
                        // for a while when it's moved into the closure.
                        let mapper = mapper.clone_ref(py);
                        stream = stream.map(move |item| map(&mapper, item));
                    }
                    Step::FlatMap { mapper } => {
                        let mapper = mapper.clone_ref(py);
                        stream = stream.flat_map(move |item| flat_map(&mapper, item));
                    }
                    Step::Filter { predicate } => {
                        let predicate = predicate.clone_ref(py);
                        stream = stream.filter(move |item| filter(&predicate, item));
                    }
                    Step::Inspect { inspector } => {
                        let inspector = inspector.clone_ref(py);
                        stream = stream.inspect(move |item| inspect(&inspector, item));
                    }
                    Step::InspectEpoch { inspector } => {
                        let inspector = inspector.clone_ref(py);
                        stream = stream.inspect_time(move |epoch, item| {
                            inspect_epoch(&inspector, epoch, item)
                        });
                    }
                    Step::Reduce {
                        reducer,
                        is_complete,
                    } => {
                        let reducer = reducer.clone_ref(py);
                        let is_complete = is_complete.clone_ref(py);
                        stream = stream.map(lift_2tuple).state_machine(
                            move |key, value, aggregator: &mut Option<TdPyAny>| {
                                reduce(&reducer, &is_complete, aggregator, key, value)
                            },
                            hash,
                        );
                    }
                    Step::ReduceEpoch { reducer } => {
                        let reducer = reducer.clone_ref(py);
                        stream = stream.map(lift_2tuple).aggregate(
                            move |key, value, aggregator: &mut Option<TdPyAny>| {
                                reduce_epoch(&reducer, aggregator, key, value);
                            },
                            move |key, aggregator: Option<TdPyAny>| {
                                // Aggregator will only exist for keys
                                // that exist, so it will have been filled
                                // into Some(value) above.
                                wrap_2tuple((key, aggregator.unwrap()))
                            },
                            hash,
                        );
                    }
                    Step::ReduceEpochLocal { reducer } => {
                        let reducer = reducer.clone_ref(py);
                        stream = stream
                            .map(lift_2tuple)
                            .accumulate(
                                HashMap::new(),
                                move |aggregators, all_key_value_in_epoch| {
                                    reduce_epoch_local(
                                        &reducer,
                                        aggregators,
                                        &all_key_value_in_epoch,
                                    );
                                },
                            )
                            .flat_map(|aggregators| aggregators.into_iter().map(wrap_2tuple));
                    }
                    Step::StatefulMap { builder, mapper } => {
                        let builder = builder.clone_ref(py);
                        let mapper = mapper.clone_ref(py);
                        stream = stream.map(lift_2tuple).state_machine(
                            move |key, value, maybe_uninit_state: &mut Option<TdPyAny>| {
                                let state =
                                    maybe_uninit_state.get_or_insert_with(|| build(&builder));
                                stateful_map(&mapper, state, key, value)
                            },
                            hash,
                        );
                    }
                    Step::Capture {} => {
                        let worker_output = worker_output.clone_ref(py);
                        let mut output_probe = ProbeHandle::new();
                        stream
                            .inspect_time(move |epoch, item| capture(&worker_output, epoch, item))
                            .probe_with(&mut output_probe);
                        output_probes.push(output_probe);
                    }
                }
            }

            if output_probes.is_empty() {
                return Err(PyValueError::new_err(
                    "Dataflow needs to contain at least one capture",
                ));
            }

            Ok(())
        })?;

        let pump = Pump::new(worker_input, timely_input);
        let worker = Self {
            timely_worker: Some(timely_worker),
            input_pumps: vec![pump],
            output_probes,
            should_stop,
        };
        Ok(worker)
    }
}

#[pymethods]
impl WorkerCoro {
    /// "Main" loop of this worker thread.
    ///
    /// 1. Pump `Pump`s (get input data from Python into Timely).
    ///
    /// 2. Dispose of empty `Pump`s which indicate there's no more
    /// input data.
    ///
    /// 3. Step the worker to do whatever work it can. This will
    /// execute operators.
    ///
    /// 4. Dispose of empty probes which indicates that all output has
    /// been emitted from that caputre operator.
    ///
    /// 5. Determine if the worker should exit.
    fn send(&mut self, py: Python, _value: Py<PyAny>) -> PyResult<()> {
        let timely_worker = self.timely_worker.as_mut().ok_or(PyRuntimeError::new_err(
            "Dataflow previously threw unhandled exception; can't be used again",
        ))?;

        for pump in self.input_pumps.iter_mut() {
            pump.pump(py)?;
        }
        self.input_pumps = self
            .input_pumps
            .drain(..)
            .filter(Pump::input_remains)
            .collect();

        // SAFETY: TWorker must outlive the catch_unwind boundary to
        // allow async resume. But TWorker is implemented in Timely
        // using some things that are not UnwindSafe. Thus after a
        // panic, throw away the TWorker via assigning None so logic
        // safety can be enforced by never being able to use the
        // possibly-corrupt TWorker again.
        std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            py.allow_threads(|| {
                timely_worker.step();
            })
        }))
        .map_err(|err| {
            // Never allow calls to this possibly-corrupted by
            // panic TWorker again.
            self.timely_worker = None;

            if let Some(pyerr) = err.downcast_ref::<PyErr>() {
                pyerr.clone_ref(py)
            } else {
                PyRuntimeError::new_err("Panic in Rust code")
            }
        })?;

        self.output_probes = self
            .output_probes
            .drain(..)
            .filter(|probe| !probe.done())
            .collect();

        let should_stop = self.should_stop.call0(py)?.extract(py)?;
        let work_complete = self.input_pumps.is_empty() && self.output_probes.is_empty();

        if should_stop || work_complete {
            Err(PyStopIteration::new_err(()))
        } else {
            Ok(())
        }
    }

    /// Run code on shutdown of worker.
    fn close(&mut self) -> PyResult<()> {
        let timely_worker = self.timely_worker.as_mut().ok_or(PyRuntimeError::new_err(
            "Dataflow previously threw unhandled exception; can't be used again",
        ))?;

        for dataflow_id in timely_worker.installed_dataflows() {
            timely_worker.drop_dataflow(dataflow_id);
        }
        Ok(())
    }

    fn throw(&mut self, _typ: &PyAny, _value: &PyAny, _traceback: &PyAny) -> PyResult<()> {
        let _timely_worker = self.timely_worker.as_mut().ok_or(PyRuntimeError::new_err(
            "Dataflow previously threw unhandled exception; can't be used again",
        ))?;

        Ok(())
    }

    // Shims to allow this coroutine to be `await`ed upon.

    fn __await__(self_: PyRef<Self>) -> PyRef<Self> {
        self_
    }

    fn __next__(&mut self, py: Python) -> PyResult<IterNextOutput<(), ()>> {
        match self.send(py, ().into_py(py)) {
            Ok(ok) => Ok(IterNextOutput::Yield(ok)),
            Err(err) if err.is_instance::<PyStopIteration>(py) => Ok(IterNextOutput::Return(())),
            Err(err) => Err(err),
        }
    }

    fn __iter__(self_: PyRef<Self>) -> PyRef<Self> {
        self_
    }
}

/// Contains handles to Timely's background communication threads in a
/// cluster setting.
///
/// You must call `join()` on this after the workers have been
/// shutdown.
#[pyclass(unsendable)]
struct CommsThreads {
    guard: Option<Box<TCommsGuard>>,
}

impl CommsThreads {
    fn new(guard: Box<TCommsGuard>) -> Self {
        Self { guard: Some(guard) }
    }
}

#[pymethods]
impl CommsThreads {
    /// Block until background communication threads have flushed and
    /// exited.
    fn join(&mut self) -> PyResult<()> {
        self.guard.take();
        Ok(())
    }
}

/// Used to configure and build a worker thread.
///
/// See the static methods on this class for how to build workers for
/// the various contexts (e.g. single thread, cluster).
#[pyclass]
struct WorkerBuilder {
    /// Consumed once built into a coroutine.
    timely_builder: Option<TBuilder>,
}

impl WorkerBuilder {
    fn new(timely_builder: TBuilder) -> Self {
        Self {
            timely_builder: Some(timely_builder),
        }
    }
}

#[pymethods]
impl WorkerBuilder {
    /// Create a new builder which will produce a worker which runs in
    /// the current thread.
    ///
    /// Returns: A single builder.
    #[staticmethod]
    fn sync() -> Self {
        Self::new(TBuilder::Thread(
            timely::communication::allocator::thread::ThreadBuilder {},
        ))
    }

    /// Create a set of builders which correspond to the worker
    /// threads on this process in a Bytewax cluster.
    ///
    /// Will start up background communication threads that will need
    /// to be joined after the worker is done.
    ///
    /// Args:
    ///
    ///     addresses: List of host/port addresses for all processes
    ///         in this cluster (including this one).
    ///         
    ///     proc_id: Index of this process in cluster; starts from 0.
    ///     
    ///     worker_count_per_proc: Number of worker threads to start
    ///         on this process.
    ///
    /// Returns:
    ///
    ///     builders: One buidler for each worker thread on this
    ///         process.
    ///
    ///     comms_guard: Handle to background communication
    ///         threads. Call `join()` on this after worker is complete.
    #[staticmethod]
    fn cluster(
        py: Python,
        addresses: Option<Vec<String>>,
        proc_id: usize,
        worker_count_per_proc: usize,
    ) -> PyResult<(Vec<Self>, CommsThreads)> {
        let addresses = addresses.unwrap_or_default();

        let (timely_builders, comms_guard) = py
            .allow_threads(|| {
                timely::CommunicationConfig::Cluster {
                    threads: worker_count_per_proc,
                    process: proc_id,
                    addresses,
                    report: false,
                    log_fn: Box::new(|_| None),
                }
                .try_build()
            })
            .map_err(PyRuntimeError::new_err)?;
        let comms_guard = comms_guard
            .downcast::<TCommsGuard>()
            .map_err(|_| PyRuntimeError::new_err("Timely did not give Bytewax a CommsGuard"))?;

        let builders = timely_builders.into_iter().map(Self::new).collect();
        Ok((builders, CommsThreads::new(comms_guard)))
    }

    /// Once this `WorkerBuilder` has been sent to the thread it
    /// should run in, convert it into an actual worker which will run
    /// the provided `Dataflow` and IO.
    ///
    /// Args:
    ///
    ///     flow: Dataflow to run.
    ///     
    ///     input_builder: Returns input that each worker thread
    ///         should process.
    ///     
    ///     output_builder: Returns a callback function for each
    ///         worker thread, called with `(epoch, item)` whenever and
    ///         item passes by a capture operator on this process.
    ///     
    ///     should_stop: Returns if this worker should gracefully
    ///         shutdown soon.
    ///
    /// Returns: Worker coroutine, ready to run.
    fn build(
        &mut self,
        py: Python,
        flow: &Dataflow,
        input_builder: TdPyCallable,
        output_builder: TdPyCallable,
        should_stop: TdPyCallable,
    ) -> PyResult<WorkerCoro> {
        use timely::communication::allocator::AllocateBuilder;

        let timely_builder = self.timely_builder.take().ok_or(PyRuntimeError::new_err(
            "Can't reuse WorkerConfig; each should turn into a single worker thread",
        ))?;

        let allocator = py.allow_threads(|| SendWrapper::new(timely_builder.build()));

        let timely_worker = SendWrapper::new(TWorker::new(
            timely::WorkerConfig::default(),
            allocator.take(),
        ));
        WorkerCoro::compile(
            py,
            timely_worker,
            flow,
            input_builder,
            output_builder,
            should_stop,
        )
    }
}

pub(crate) fn register(_py: Python, m: &PyModule) -> PyResult<()> {
    m.add_class::<WorkerCoro>()?;
    m.add_class::<WorkerBuilder>()?;
    m.add_class::<CommsThreads>()?;
    Ok(())
}
