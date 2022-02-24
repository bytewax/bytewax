use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3::types::*;

use std::collections::HashMap;
use timely::dataflow::*;

use timely::dataflow::operators::aggregation::*;
use timely::dataflow::operators::*;

use crate::operators::{
    capture, filter, flat_map, inspect, inspect_epoch, map, reduce, reduce_epoch,
    reduce_epoch_local, stateful_map,
};
use crate::{
    build, hash, lift_2tuple, wrap_2tuple, Dataflow, Pump, TdPyAny, TdPyCallable, TdPyIterator,
};

/// The definition of one step in a Bytewax dataflow graph.
///
/// This isn't actually used during execution, just during building.
///
/// See
/// <https://docs.rs/timely/latest/timely/dataflow/operators/index.html>
/// for Timely's operators. We try to keep the same semantics here.
pub(crate) enum Step {
    Map {
        mapper: TdPyCallable,
    },
    FlatMap {
        mapper: TdPyCallable,
    },
    Filter {
        predicate: TdPyCallable,
    },
    Inspect {
        inspector: TdPyCallable,
    },
    InspectEpoch {
        inspector: TdPyCallable,
    },
    Reduce {
        reducer: TdPyCallable,
        is_complete: TdPyCallable,
    },
    ReduceEpoch {
        reducer: TdPyCallable,
    },
    ReduceEpochLocal {
        reducer: TdPyCallable,
    },
    StatefulMap {
        builder: TdPyCallable,
        mapper: TdPyCallable,
    },
    Capture {},
}

/// Decode Steps from Python (name, funcs...) tuples.
///
/// Required for pickling.
impl<'source> FromPyObject<'source> for Step {
    fn extract(obj: &'source PyAny) -> PyResult<Self> {
        let tuple: &PySequence = obj.downcast()?;

        if let Ok(("Map", mapper)) = tuple.extract() {
            return Ok(Self::Map { mapper });
        }
        if let Ok(("FlatMap", mapper)) = tuple.extract() {
            return Ok(Self::FlatMap { mapper });
        }
        if let Ok(("Filter", predicate)) = tuple.extract() {
            return Ok(Self::Filter { predicate });
        }
        if let Ok(("Inspect", inspector)) = tuple.extract() {
            return Ok(Self::Inspect { inspector });
        }
        if let Ok(("InspectEpoch", inspector)) = tuple.extract() {
            return Ok(Self::InspectEpoch { inspector });
        }
        if let Ok(("Reduce", reducer, is_complete)) = tuple.extract() {
            return Ok(Self::Reduce {
                reducer,
                is_complete,
            });
        }
        if let Ok(("ReduceEpoch", reducer)) = tuple.extract() {
            return Ok(Self::ReduceEpoch { reducer });
        }
        if let Ok(("ReduceEpochLocal", reducer)) = tuple.extract() {
            return Ok(Self::ReduceEpochLocal { reducer });
        }
        if let Ok(("StatefulMap", builder, mapper)) = tuple.extract() {
            return Ok(Self::StatefulMap { builder, mapper });
        }
        if let Ok(("Capture",)) = tuple.extract() {
            return Ok(Self::Capture {});
        }

        Err(PyValueError::new_err(format!(
            "bad python repr when unpickling Step: {obj:?}"
        )))
    }
}

/// Represent Steps in Python as (name, funcs...) tuples.
///
/// Required for pickling.
impl ToPyObject for Step {
    fn to_object(&self, py: Python) -> Py<PyAny> {
        match self {
            Self::Map { mapper } => ("Map", mapper).to_object(py),
            Self::FlatMap { mapper } => ("FlatMap", mapper).to_object(py),
            Self::Filter { predicate } => ("Filter", predicate).to_object(py),
            Self::Inspect { inspector } => ("Inspect", inspector).to_object(py),
            Self::InspectEpoch { inspector } => ("InspectEpoch", inspector).to_object(py),
            Self::Reduce {
                reducer,
                is_complete,
            } => ("Reduce", reducer, is_complete).to_object(py),
            Self::ReduceEpoch { reducer } => ("ReduceEpoch", reducer).to_object(py),
            Self::ReduceEpochLocal { reducer } => ("ReduceEpochLocal", reducer).to_object(py),
            Self::StatefulMap { builder, mapper } => ("StatefulMap", builder, mapper).to_object(py),
            Self::Capture {} => ("Capture",).to_object(py),
        }
        .into()
    }
}

pub(crate) fn build_dataflow<A>(
    timely_worker: &mut timely::worker::Worker<A>,
    py: Python,
    flow: &Dataflow,
    input_builder: TdPyCallable,
    output_builder: TdPyCallable,
) -> Result<(Pump, ProbeHandle<u64>), String>
where
    A: timely::communication::Allocate,
{
    let worker_index = timely_worker.index();
    let worker_count = timely_worker.peers();
    timely_worker.dataflow(|scope| {
        let mut timely_input = InputHandle::new();
        let mut end_of_steps_probe = ProbeHandle::new();
        let mut has_capture = false;
        let mut stream = timely_input.to_stream(scope);

        let worker_input: TdPyIterator = input_builder
            .call1(py, (worker_index, worker_count))
            .unwrap()
            .extract(py)
            .unwrap();
        let worker_output: TdPyCallable = output_builder
            .call1(py, (worker_index, worker_count))
            .unwrap()
            .extract(py)
            .unwrap();

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
                    stream = stream
                        .inspect_time(move |epoch, item| inspect_epoch(&inspector, epoch, item));
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
                                reduce_epoch_local(&reducer, aggregators, &all_key_value_in_epoch);
                            },
                        )
                        .flat_map(|aggregators| aggregators.into_iter().map(wrap_2tuple));
                }
                Step::StatefulMap { builder, mapper } => {
                    let builder = builder.clone_ref(py);
                    let mapper = mapper.clone_ref(py);
                    stream = stream.map(lift_2tuple).state_machine(
                        move |key, value, maybe_uninit_state: &mut Option<TdPyAny>| {
                            let state = maybe_uninit_state.get_or_insert_with(|| build(&builder));
                            stateful_map(&mapper, state, key, value)
                        },
                        hash,
                    );
                }
                Step::Capture {} => {
                    let worker_output = worker_output.clone_ref(py);
                    stream
                        .inspect_time(move |epoch, item| capture(&worker_output, epoch, item))
                        .probe_with(&mut end_of_steps_probe);
                    has_capture = true;
                }
            }
        }

        if has_capture {
            let pump = Pump::new(worker_input, timely_input);
            Ok((pump, end_of_steps_probe))
        } else {
            Err("Dataflow needs to contain at least one capture".into())
        }
    })
}
