extern crate rand;

use pyo3::exceptions::PyKeyboardInterrupt;
use pyo3::exceptions::PyTypeError;
use pyo3::prelude::*;
use pyo3::types::*;
use serde::ser::Error;
use std::collections::hash_map::DefaultHasher;
use std::fmt;
use std::hash::Hash;
use std::hash::Hasher;
use std::ops::Deref;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::thread;
use std::time::Duration;
use timely::WorkerConfig;

use timely::dataflow::operators::aggregation::*;
use timely::dataflow::operators::*;
use timely::dataflow::*;

pub(crate) mod webserver;

#[macro_use]
pub(crate) mod macros;

/// Represents a Python object flowing through a Timely dataflow.
///
/// A newtype for [`Py`]<[`PyAny`]> so we can
/// extend it with traits that Timely needs. See
/// <https://github.com/Ixrec/rust-orphan-rules> for why we need a
/// newtype and what they are.
#[derive(Clone, FromPyObject)]
struct TdPyAny(Py<PyAny>);

/// Have access to all [`Py`] methods.
impl Deref for TdPyAny {
    type Target = Py<PyAny>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

/// Allows use in some Rust to Python conversions.
// I Don't really get the difference between ToPyObject and IntoPy
// yet.
impl ToPyObject for TdPyAny {
    fn to_object(&self, py: Python) -> Py<PyAny> {
        self.0.clone_ref(py)
    }
}

/// Allow passing to Python function calls without explicit
/// conversion.
impl IntoPy<PyObject> for TdPyAny {
    fn into_py(self, _py: Python) -> Py<PyAny> {
        self.0
    }
}

/// Allow passing a reference to Python function calls without
/// explicit conversion.
impl IntoPy<PyObject> for &TdPyAny {
    fn into_py(self, _py: Python) -> Py<PyAny> {
        self.clone_ref(_py)
    }
}

/// Conveniently re-wrap Python objects for use in Timely.
impl From<&PyAny> for TdPyAny {
    fn from(x: &PyAny) -> Self {
        Self(x.into())
    }
}

/// Conveniently re-wrap Python objects for use in Timely.
impl From<Py<PyAny>> for TdPyAny {
    fn from(x: Py<PyAny>) -> Self {
        Self(x)
    }
}

/// Serialize Python objects flowing through Timely that cross
/// process bounds as pickled bytes.
impl serde::Serialize for TdPyAny {
    // We can't do better than isolating the Result<_, PyErr> part and
    // the explicitly converting.  1. `?` automatically trys to
    // convert using From<ReturnedError> for OuterError. But orphan
    // rule means we can't implement it since we don't own either py
    // or serde error types.  2. Using the newtype trick isn't worth
    // it, since you'd have to either wrap all the PyErr when they're
    // generated, or you implement From twice, once to get MyPyErr and
    // once to get serde::Err. And then you're calling .into()
    // explicitly since the last line isn't a `?` anyway.
    //
    // There's the separate problem if we could even implement the
    // Froms. We aren't allowed to "capture" generic types in an inner
    // `impl<S>` https://doc.rust-lang.org/error-index.html#E0401 and
    // we can't move them top-level since we don't know the concrete
    // type of S, and you're not allowed to have unconstrained generic
    // parameters https://doc.rust-lang.org/error-index.html#E0207 .
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let bytes: Result<Vec<u8>, PyErr> = Python::with_gil(|py| {
            let x = self.as_ref(py);
            let pickle = py.import("pickle")?;
            let bytes = pickle.call_method1("dumps", (x,))?.extract()?;
            Ok(bytes)
        });
        serializer.serialize_bytes(bytes.map_err(S::Error::custom)?.as_slice())
    }
}

struct PickleVisitor;
impl<'de> serde::de::Visitor<'de> for PickleVisitor {
    type Value = TdPyAny;

    fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        formatter.write_str("a pickled byte array")
    }

    fn visit_bytes<E>(self, bytes: &[u8]) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        let x: Result<TdPyAny, PyErr> = Python::with_gil(|py| {
            let pickle = py.import("pickle")?;
            let x = pickle.call_method1("loads", (bytes,))?.into();
            Ok(x)
        });
        x.map_err(E::custom)
    }
}

/// Deserialize Python objects flowing through Timely that cross
/// process bounds from pickled bytes.
impl<'de> serde::Deserialize<'de> for TdPyAny {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        deserializer.deserialize_bytes(PickleVisitor)
    }
}

/// Re-use Python's value semantics in Rust code.
///
/// Timely requires this whenever it internally "groups by".
impl PartialEq for TdPyAny {
    fn eq(&self, other: &Self) -> bool {
        use pyo3::class::basic::CompareOp;

        Python::with_gil(|py| {
            // Don't use Py.eq or PyAny.eq since it only checks
            // pointer identity.
            let self_ = self.as_ref(py);
            let other = other.as_ref(py);
            with_traceback!(py, self_.rich_compare(other, CompareOp::Eq)?.is_true())
        })
    }
}

/// Possibly a footgun.
///
/// Timely internally stores values in [`HashMap`] which require keys
/// to be [`Eq`] so we have to implement this (or somehow write our
/// own hash maps that could ignore this), but we can't actually
/// guarantee that any Python type will actually have a correct sense
/// of total equality. It's possible broken `__eq__` implementations
/// will cause mysterious behavior.
impl Eq for TdPyAny {}

/// Re-use Python's hashing semantics in Rust.
///
/// Timely requires this whenever it exchanges or accumulates data in
/// hash maps.
impl Hash for TdPyAny {
    fn hash<H: Hasher>(&self, state: &mut H) {
        Python::with_gil(|py| {
            let self_ = self.as_ref(py);
            let hash = with_traceback!(py, self_.hash());
            state.write_isize(hash);
        });
    }
}

/// A Python iterator that only gets the GIL when calling .next() and
/// automatically wraps in [`TdPyAny`].
///
/// Otherwise the GIL would be held for the entire life of the iterator.
#[derive(Clone)]
struct TdPyIterator(Py<PyIterator>);

/// Have PyO3 do type checking to ensure we only make from iterable
/// objects.
impl<'source> FromPyObject<'source> for TdPyIterator {
    fn extract(ob: &'source PyAny) -> PyResult<Self> {
        Ok(ob.iter()?.into())
    }
}

/// Conveniently re-wrap PyO3 objects.
impl From<&PyIterator> for TdPyIterator {
    fn from(x: &PyIterator) -> Self {
        Self(x.into())
    }
}

/// Restricted Rust interface that only makes sense for iterators.
///
/// Just pass through to [`Py`].
///
/// This is mostly empty because [`Iterator`] does the heavy lifting
/// for defining the iterator API.
impl TdPyIterator {
    fn clone_ref(&self, py: Python) -> Self {
        Self(self.0.clone_ref(py))
    }
}

impl Iterator for TdPyIterator {
    type Item = TdPyAny;

    fn next(&mut self) -> Option<Self::Item> {
        Python::with_gil(|py| {
            let mut iter = self.0.as_ref(py);
            iter.next().map(|r| with_traceback!(py, r).into())
        })
    }
}

/// A Python object that is callable.
struct TdPyCallable(Py<PyAny>);

/// Have PyO3 do type checking to ensure we only make from callable
/// objects.
impl<'source> FromPyObject<'source> for TdPyCallable {
    fn extract(ob: &'source PyAny) -> PyResult<Self> {
        if ob.is_callable() {
            Ok(Self(ob.into()))
        } else {
            let msg = if let Ok(type_name) = ob.get_type().name() {
                format!("'{type_name}' object is not callable")
            } else {
                "object is not callable".to_string()
            };
            Err(PyTypeError::new_err(msg))
        }
    }
}

/// Restricted Rust interface that only makes sense on callable
/// objects.
///
/// Just pass through to [`Py`].
impl TdPyCallable {
    fn clone_ref(&self, py: Python) -> Self {
        Self(self.0.clone_ref(py))
    }

    fn call0(&self, py: Python) -> PyResult<Py<PyAny>> {
        self.0.call0(py)
    }

    fn call1(&self, py: Python, args: impl IntoPy<Py<PyTuple>>) -> PyResult<Py<PyAny>> {
        self.0.call1(py, args)
    }
}

/// Styles of input for a Bytewax dataflow. Built as part of
/// calling [`Dataflow::new`].
///
/// PyO3 will automatically generate one of these given the Python
/// object is an iterator or a callable.
#[derive(FromPyObject)]
enum Input {
    /// A dataflow that uses a Singleton as its input will receive
    /// data from a single Python `Iterator[Tuple[int, Any]]` where
    /// the `int` is the timestamp for that element and `Any` is the
    /// data element itself.
    ///
    /// Only worker `0` will run this iterator and then we'll
    /// immediately distribute the items to every worker randomly.
    // Have PyO3 ignore that this is a named field and just try to
    // make a TdPyIterator out of the argument it's trying to turn
    // into this.
    #[pyo3(transparent)]
    Singleton { worker_0s_input: TdPyIterator },

    /// A dataflow that uses a Partitioned input will have each worker
    /// generate its own input from a builder function `def
    /// build(worker_index: int, total_workers: int) ->
    /// Iterable[Tuple[int, Any]]` where `int` is the timestamp for
    /// that element and `Any` is the data element itself.
    ///
    /// Each worker will call this function independently so they
    /// should return disjoint data.
    #[pyo3(transparent)]
    Partitioned {
        per_worker_input_builder: TdPyCallable,
    },
}

/// The definition of one step in a Bytewax dataflow graph.
///
/// This isn't actually used during execution, just during building.
///
/// See
/// <https://docs.rs/timely/latest/timely/dataflow/operators/index.html>
/// for Timely's operators. We try to keep the same semantics here.
enum Step {
    Map(TdPyCallable),
    FlatMap(TdPyCallable),
    Exchange(TdPyCallable),
    Filter(TdPyCallable),
    Inspect(TdPyCallable),
    InspectEpoch(TdPyCallable),
    Accumulate {
        build_new_aggregator: TdPyCallable,
        reducer: TdPyCallable,
    },
    StateMachine {
        build_new_aggregator: TdPyCallable,
        reducer: TdPyCallable,
    },
    Aggregate {
        build_new_aggregator: TdPyCallable,
        reducer: TdPyCallable,
    },
}

/// The definition of Bytewax dataflow graph.
///
/// This isn't actually used during execution, just during building.
///
/// TODO: Right now this is just a linear dataflow only.
#[pyclass]
struct Dataflow {
    input: Input,
    steps: Vec<Step>,
}

#[pymethods]
impl Dataflow {
    #[new]
    fn new(input: Input) -> Self {
        Self {
            input: input,
            steps: Vec::new(),
        }
    }

    fn map(&mut self, f: TdPyCallable) {
        self.steps.push(Step::Map(f));
    }

    fn flat_map(&mut self, f: TdPyCallable) {
        self.steps.push(Step::FlatMap(f));
    }

    fn exchange(&mut self, f: TdPyCallable) {
        self.steps.push(Step::Exchange(f));
    }

    fn filter(&mut self, f: TdPyCallable) {
        self.steps.push(Step::Filter(f));
    }

    fn inspect(&mut self, f: TdPyCallable) {
        self.steps.push(Step::Inspect(f));
    }

    fn inspect_epoch(&mut self, f: TdPyCallable) {
        self.steps.push(Step::InspectEpoch(f));
    }

    fn accumulate(&mut self, build_new_aggregator: TdPyCallable, reducer: TdPyCallable) {
        self.steps.push(Step::Accumulate {
            build_new_aggregator,
            reducer,
        });
    }

    fn state_machine(&mut self, build_new_aggregator: TdPyCallable, reducer: TdPyCallable) {
        self.steps.push(Step::StateMachine {
            build_new_aggregator,
            reducer,
        });
    }

    fn aggregate(&mut self, build_new_aggregator: TdPyCallable, reducer: TdPyCallable) {
        self.steps.push(Step::Aggregate {
            build_new_aggregator,
            reducer,
        });
    }
}

// These are all shims which map the Timely Rust API into equivalent
// calls to Python functions through PyO3.

fn build(f: &TdPyCallable) -> TdPyAny {
    Python::with_gil(|py| with_traceback!(py, f.call0(py)).into())
}

fn map(f: &TdPyCallable, x: TdPyAny) -> TdPyAny {
    Python::with_gil(|py| with_traceback!(py, f.call1(py, (x,))).into())
}

fn flat_map(f: &TdPyCallable, x: TdPyAny) -> TdPyIterator {
    Python::with_gil(|py| with_traceback!(py, f.call1(py, (x,))?.extract(py)))
}

fn exchange(f: &TdPyCallable, x: &TdPyAny) -> u64 {
    // need to specify signed int to prevent python from converting negative int to unsigned
    Python::with_gil(|py| {
        with_traceback!(py, f.call1(py, (x,))?.extract::<i64>(py).map(|x| x as u64))
    })
}

fn filter(f: &TdPyCallable, x: &TdPyAny) -> bool {
    Python::with_gil(|py| with_traceback!(py, f.call1(py, (x,))?.extract(py)))
}

fn inspect(f: &TdPyCallable, x: &TdPyAny) {
    Python::with_gil(|py| {
        with_traceback!(py, f.call1(py, (x,)));
    });
}

fn inspect_epoch(f: &TdPyCallable, epoch: &u64, x: &TdPyAny) {
    Python::with_gil(|py| {
        with_traceback!(py, f.call1(py, (*epoch, x)));
    });
}

fn accumulate(f: &TdPyCallable, aggregator: &mut TdPyAny, all_xs_in_epoch: &Vec<TdPyAny>) {
    Python::with_gil(|py| {
        let all_xs_in_epoch = all_xs_in_epoch.clone();
        *aggregator =
            with_traceback!(py, f.call1(py, (aggregator.clone_ref(py), all_xs_in_epoch))).into();
    });
}

fn hash(key: &TdPyAny) -> u64 {
    let mut hasher = DefaultHasher::new();
    key.hash(&mut hasher);
    hasher.finish()
}

fn state_machine(
    f: &TdPyCallable,
    aggregator: &mut TdPyAny,
    key: &TdPyAny,
    value: TdPyAny,
) -> (bool, TdPyIterator) {
    Python::with_gil(|py| {
        let (updated_aggregator, output_xs): (TdPyAny, TdPyIterator) = with_traceback!(
            py,
            f.call1(py, (aggregator.clone_ref(py), key, value))?
                .extract(py)
        );
        let should_discard_aggregator = updated_aggregator.is_none(py);
        *aggregator = updated_aggregator;
        (should_discard_aggregator, output_xs)
    })
}

/// Turn a Python 2-tuple into a Rust 2-tuple.
fn lift_2tuple(key_value_pytuple: TdPyAny) -> (TdPyAny, TdPyAny) {
    Python::with_gil(|py| with_traceback!(py, key_value_pytuple.as_ref(py).extract()))
}

fn aggregate(f: &TdPyCallable, aggregator: &mut TdPyAny, key: &TdPyAny, value: TdPyAny) {
    Python::with_gil(|py| {
        let updated_aggregator: TdPyAny = with_traceback!(
            py,
            f.call1(py, (aggregator.clone_ref(py), key, value))?
                .extract(py)
        );
        *aggregator = updated_aggregator;
    });
}

fn emit_2tuple(key: TdPyAny, final_aggregator: TdPyAny) -> TdPyAny {
    Python::with_gil(|py| (key, final_aggregator).to_object(py).into())
}

// End of shim functions.

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
    fn pump(&mut self) {
        Python::with_gil(|py| {
            let mut pull_from_pyiter = self.pull_from_pyiter.0.as_ref(py);
            if let Some(epoch_x_pytuple) = pull_from_pyiter.next() {
                let (epoch, x) = with_traceback!(py, epoch_x_pytuple?.extract());
                self.push_to_timely.advance_to(epoch);
                self.push_to_timely.send(x);
            } else {
                self.pyiter_is_empty = true;
            }
        });
    }

    fn input_remains(&self) -> bool {
        !self.pyiter_is_empty
    }
}

fn build_dataflow<A>(
    timely_worker: &mut timely::worker::Worker<A>,
    py: Python,
    dataflow: &Dataflow,
) -> (Option<Pump>, ProbeHandle<u64>)
where
    A: timely::communication::Allocate,
{
    let this_worker_index = timely_worker.index();
    let total_worker_count = timely_worker.peers();
    timely_worker.dataflow(|scope| {
        let mut timely_input = InputHandle::new();
        let mut end_of_steps_probe = ProbeHandle::new();
        let mut stream = timely_input.to_stream(scope);

        if let Input::Singleton { .. } = dataflow.input {
            stream = stream.exchange(|_| rand::random());
        }

        let steps = &dataflow.steps;
        for step in steps {
            match step {
                Step::Map(f) => {
                    // All these closure lifetimes are static, so tell
                    // Python's GC that there's another pointer to the
                    // mapping function that's going to hang around
                    // for a while when it's moved into the closure.
                    let f = f.clone_ref(py);
                    stream = stream.map(move |x| map(&f, x));
                }
                Step::FlatMap(f) => {
                    let f = f.clone_ref(py);
                    stream = stream.flat_map(move |x| flat_map(&f, x));
                }
                Step::Exchange(f) => {
                    let f = f.clone_ref(py);
                    stream = stream.exchange(move |x| exchange(&f, x));
                }
                Step::Filter(f) => {
                    let f = f.clone_ref(py);
                    stream = stream.filter(move |x| filter(&f, x));
                }
                Step::Inspect(f) => {
                    let f = f.clone_ref(py);
                    stream = stream.inspect(move |x| inspect(&f, x));
                }
                Step::InspectEpoch(f) => {
                    let f = f.clone_ref(py);
                    stream = stream.inspect_time(move |epoch, x| inspect_epoch(&f, epoch, x));
                }
                Step::Accumulate {
                    build_new_aggregator,
                    reducer,
                } => {
                    let build_new_aggregator = build_new_aggregator.clone_ref(py);
                    let reducer = reducer.clone_ref(py);
                    stream = stream
                        .accumulate(None, move |maybe_uninit_aggregator, all_xs_in_epoch| {
                            let aggregator = maybe_uninit_aggregator
                                .get_or_insert_with(|| build(&build_new_aggregator));
                            accumulate(&reducer, aggregator, &all_xs_in_epoch);
                        })
                        // Unwrap the Option<TdPyAny> used above just
                        // to signal a new aggregator.
                        .flat_map(|maybe_uninit_aggregator| maybe_uninit_aggregator);
                }
                Step::StateMachine {
                    build_new_aggregator,
                    reducer,
                } => {
                    let build_new_aggregator = build_new_aggregator.clone_ref(py);
                    let reducer = reducer.clone_ref(py);
                    stream = stream.map(lift_2tuple).state_machine(
                        move |key, value, maybe_uninit_aggregator: &mut Option<TdPyAny>| {
                            let aggregator = maybe_uninit_aggregator
                                .get_or_insert_with(|| build(&build_new_aggregator));
                            state_machine(&reducer, aggregator, key, value)
                        },
                        hash,
                    );
                }
                Step::Aggregate {
                    build_new_aggregator,
                    reducer,
                } => {
                    let build_for_fold = build_new_aggregator.clone_ref(py);
                    let build_for_emit = build_new_aggregator.clone_ref(py);
                    let reducer = reducer.clone_ref(py);
                    stream = stream.map(lift_2tuple).aggregate(
                        move |key, value, maybe_uninit_aggregator: &mut Option<TdPyAny>| {
                            let aggregator = maybe_uninit_aggregator
                                .get_or_insert_with(|| build(&build_for_fold));
                            aggregate(&reducer, aggregator, key, value);
                        },
                        move |key, maybe_uninit_aggregator: Option<TdPyAny>| {
                            let aggregator =
                                maybe_uninit_aggregator.unwrap_or_else(|| build(&build_for_emit));
                            emit_2tuple(key, aggregator)
                        },
                        hash,
                    );
                }
            }
        }
        stream.probe_with(&mut end_of_steps_probe);

        let pump: Option<Pump> = match &dataflow.input {
            Input::Singleton { worker_0s_input } => {
                if this_worker_index == 0 {
                    let worker_0s_input = worker_0s_input.clone_ref(py);
                    Some(Pump::new(worker_0s_input, timely_input))
                } else {
                    None
                }
            }
            Input::Partitioned {
                per_worker_input_builder,
            } => {
                let this_builders_input_iter = per_worker_input_builder
                    .call1(py, (this_worker_index, total_worker_count))
                    .unwrap()
                    .extract(py)
                    .unwrap();
                Some(Pump::new(this_builders_input_iter, timely_input))
            }
        };

        (pump, end_of_steps_probe)
    })
}

/// "Main loop" of a Timely worker thread.
///
/// 1. Pump [`Pump`]s (get input data from Python into Timely).
///
/// 2. Dispose of empty [`Pump`]s and Probes which indicate there's no
/// data left to process in that dataflow.
///
/// 3. Call [timely::worker::Worker::step] to tell Timely to do
/// whatever work it can.
fn worker_main<A>(
    mut pumps_with_input_remaining: Vec<Pump>,
    mut probes_with_timestamps_inflight: Vec<ProbeHandle<u64>>,
    interrupt_flag: &Arc<AtomicBool>,
    worker: &mut timely::worker::Worker<A>,
) where
    A: timely::communication::Allocate,
{
    while (!pumps_with_input_remaining.is_empty() || !probes_with_timestamps_inflight.is_empty())
        && !interrupt_flag.load(Ordering::Relaxed)
    {
        if !pumps_with_input_remaining.is_empty() {
            let mut updated_pumps = Vec::new();
            for mut pump in pumps_with_input_remaining.into_iter() {
                pump.pump();
                if pump.input_remains() {
                    updated_pumps.push(pump);
                }
            }
            pumps_with_input_remaining = updated_pumps;
        }
        if !probes_with_timestamps_inflight.is_empty() {
            let mut updated_probes = Vec::new();
            for probe in probes_with_timestamps_inflight.into_iter() {
                if !probe.done() {
                    updated_probes.push(probe);
                }
            }
            probes_with_timestamps_inflight = updated_probes;
        }

        worker.step();
    }
}

/// Main Bytewax class that handles defining and executing
/// dataflows.
///
/// Create a new dataflow for a stream of input with
/// [`Dataflow`], add steps to it using the methods within
/// (e.g. `map`), then build and run it using [`build_and_run`].
#[pyclass]
struct Executor {
    dataflows: Vec<Py<Dataflow>>,
}

#[pymethods]
impl Executor {
    #[new]
    fn new() -> Self {
        Executor {
            dataflows: Vec::new(),
        }
    }

    /// Create a new empty dataflow blueprint that you can add steps
    /// to.
    #[pyo3(name = "Dataflow")] // Cuz it's sort of the class constructor.
    fn dataflow(&mut self, py: Python, input: Input) -> PyResult<Py<Dataflow>> {
        // For some reason we can only return Rust-owned objects in
        // #[new], which is totally unlike the API of PyList::new(py,
        // ...) which returns something already on the Python
        // heap. This means we need to send it to Python right away to
        // have a Py pointer we can clone into the vec and return now.
        let dataflow = Py::new(py, Dataflow::new(input))?;
        self.dataflows.push(dataflow.clone());
        Ok(dataflow)
    }

    /// Build all of the previously-defined dataflow blueprints and
    /// then run them.
    ///
    /// Will start up the number of workers specified on the command
    /// line.
    fn build_and_run(self_: Py<Self>, py: Python) -> PyResult<()> {
        // We can't use Python::check_signals() in the worker threads
        // since according to
        // https://docs.python.org/3/c-api/exceptions.html#c.PyErr_CheckSignals
        // it does nothing unless called on the main thread! But that
        // thread is blocked!
        let interrupt_flag = Arc::new(AtomicBool::new(false));
        let interrupt_flag_for_ctrlc_thread = interrupt_flag.clone();
        // Worker threads have to be gracefully shutdown, but since
        // the main Python thread blocks below waiting for the workers
        // to join(), we have to spin up another thread to listen for
        // interrupts.
        ctrlc::set_handler(move || {
            interrupt_flag_for_ctrlc_thread.store(true, Ordering::Relaxed);
        })
        .map_err(|e| pyo3::exceptions::PyException::new_err(e.to_string()))?;

        let interrupt_flag_for_worker_threads = interrupt_flag.clone();
        let guards = timely::execute_from_args(std::env::args(), move |worker| {
            let mut all_pumps = Vec::new();
            let mut all_probes = Vec::new();

            Python::with_gil(|py| {
                // These borrows should be safe because nobody else
                // will have a mut ref. Although they might have
                // shared refs.
                let self_ = self_.as_ref(py).borrow();
                for dataflow in &self_.dataflows {
                    let dataflow = dataflow.as_ref(py).borrow();
                    let (pump, probe) = build_dataflow(worker, py, &dataflow);
                    all_pumps.extend(pump);
                    all_probes.push(probe);
                }
            });

            worker_main(
                all_pumps,
                all_probes,
                &interrupt_flag_for_worker_threads,
                worker,
            );
        })
        .map_err(pyo3::exceptions::PyException::new_err)?;

        py.allow_threads(|| {
            // TODO: Disabled until there is something to show.
            // tokio::runtime::Builder::new_multi_thread()
            //     .enable_all()
            //     .build()
            //     .unwrap()
            //     .block_on(webserver::start());
            //
            // This is
            // blocking. https://docs.python.org/3/library/signal.html#execution-of-python-signal-handlers
            // says that Python-side signal handlers will only be
            // called on the main thread, so nothing ever gets
            // triggered by ctrl-c while we're blocked here.
            guards.join();
        });

        // Pass through if we got interrupted.
        if interrupt_flag.load(Ordering::Relaxed) {
            Err(PyKeyboardInterrupt::new_err(""))
        } else {
            Ok(())
        }
        // TODO: My understanding is POSIX says you can only have one
        // signal callback per process. So by setting our custom one
        // above, we're clobbering Python's default one. So if someone
        // caught this KeyboardInterrupt and then tried to continue on
        // in the main thread, ctrl-c would do nothing there. Not sure
        // how to revert to default. Also FWIW, Python's Thread.join()
        // *can* be interrupted by ctrl-c, so maybe there's some way
        // to make this work...
    }

    /// Build all of the previously-defined dataflow blueprints and
    /// then run them on the number of threads specified.
    ///
    /// Unlike `build_and_run()`, this function does not take command-line
    /// arguments to control the number of workers or threads.
    fn execute_directly(self_: Py<Self>, py: Python, threads: usize) -> PyResult<()> {
        let interrupt_flag = Arc::new(AtomicBool::new(false)); // Unused

        // Instead of taking command line arguments, run a workflow in a single thread
        let (builders, other) = timely::CommunicationConfig::Process(threads).try_build().unwrap();
        let guards = timely::execute::execute_from(
            builders,
            other,
            WorkerConfig::default(),
            move |worker| {
                let mut all_pumps = Vec::new();
                let mut all_probes = Vec::new();

                Python::with_gil(|py| {
                    let self_ = self_.as_ref(py).borrow();
                    for dataflow in &self_.dataflows {
                        let dataflow = dataflow.as_ref(py).borrow();
                        let (pump, probe) = build_dataflow(worker, py, &dataflow);
                        all_pumps.extend(pump);
                        all_probes.push(probe);
                    }
                });

                worker_main(all_pumps, all_probes, &interrupt_flag, worker);
            },
        )
        .map_err(pyo3::exceptions::PyException::new_err)?;

        py.allow_threads(|| {
            guards.join();
        });
        Ok(())
    }
}

#[pyfunction]
fn sleep_keep_gil(_py: Python, secs: u64) -> () {
    thread::sleep(Duration::from_secs(secs));
}

#[pyfunction]
fn sleep_release_gil(_py: Python, secs: u64) -> () {
    _py.allow_threads(|| {
        thread::sleep(Duration::from_secs(secs));
    });
}

#[pymodule]
#[pyo3(name = "bytewax")]
fn mod_tiny_dancer(_py: Python, m: &PyModule) -> PyResult<()> {
    m.add_class::<Dataflow>()?;
    m.add_class::<Executor>()?;

    m.add_function(wrap_pyfunction!(sleep_keep_gil, m)?)?;
    m.add_function(wrap_pyfunction!(sleep_release_gil, m)?)?;

    Ok(())
}
