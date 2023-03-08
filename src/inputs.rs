//! Internal code for input systems.
//!
//! For a user-centric version of input, read the `bytewax.inputs`
//! Python module docstring. Read that first.
//!
//! [`PartitionedInput`] defines an input source. [`PartBundle`] is
//! what is passed to the source operators defined in
//! [`crate::execution::epoch`].
//!
//! Each [`PartIter`] is keyed by a [`StateKey`] so that the recovery
//! system can round trip the state data back to
//! [`PartitionedInput::build_parts`] and be provided to the correct
//! builder.
//!
//! The one extra quirk here is that input is completely decoupled
//! from epoch generation. See [`crate::execution`] for how Timely
//! sources are generated and epochs assigned. The only goal of the
//! input system is "what's the next item for this input?"

use std::collections::{BTreeSet, HashMap, VecDeque};
use std::task::Poll;

use crate::execution::{WorkerCount, WorkerIndex};
use crate::pyo3_extensions::TdPyAny;
use crate::recovery::model::{StateBytes, StateKey, StepId, StepStateBytes};
use crate::recovery::operators::Route;
use crate::unwrap_any;
use pyo3::exceptions::PyTypeError;
use pyo3::prelude::*;
use pyo3::types::PyIterator;

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
                "input must derive from `bytewax.inputs.PartitionedInput`",
            ))
        } else {
            Ok(Self(ob.into()))
        }
    }
}

impl IntoPy<Py<PyAny>> for PartitionedInput {
    fn into_py(self, _py: Python<'_>) -> Py<PyAny> {
        self.0
    }
}

/// The total number of partitions in an input source.
///
/// Not just on this worker.
pub(crate) struct TotalPartCount(pub(crate) usize);

impl PartitionedInput {
    /// Build all partitions for this input for this worker.
    pub(crate) fn build_parts(
        &self,
        py: Python,
        step_id: StepId,
        index: WorkerIndex,
        worker_count: WorkerCount,
        mut resume_state: StepStateBytes,
    ) -> PyResult<(PartBundle, TotalPartCount)> {
        let keys: BTreeSet<StateKey> = self.0.call_method0(py, "list_parts")?.extract(py)?;

        let part_count = TotalPartCount(keys.len());

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
            .map(|key| {
                let state = resume_state
                    .remove(&key)
                    .map(StateBytes::de::<TdPyAny>)
                    .unwrap_or_else(|| py.None().into());
                tracing::info!("{index:?} building input {step_id:?} partition {key:?} with resume state {state:?}");
                let iter: Py<PyIterator> = self
                    .0
                    .call_method1(py, "build_part", (key.clone(), state.clone_ref(py)))?
                    .as_ref(py)
                    .iter()?
                    .into();
                let part = PartIter { iter, state };
                Ok((key, part))
            }).collect::<PyResult<HashMap<StateKey, PartIter>>>()?;

        if !resume_state.is_empty() {
            tracing::warn!("Resume state exists for {step_id:?} for unknown partitions {:?}; changing partition counts? recovery state routing bug?", resume_state.keys());
        }

        Ok((PartBundle::new(parts), part_count))
    }
}

/// A single input partition.
pub(crate) struct PartIter {
    iter: Py<PyIterator>,
    /// The last state seen.
    state: TdPyAny,
}

impl PartIter {
    /// Get the next item from this partition, if any, and save the
    /// state.
    pub(crate) fn next(&mut self) -> Poll<Option<TdPyAny>> {
        Python::with_gil(|py| {
            let mut iter = self.iter.as_ref(py);
            match iter.next() {
                None => Poll::Ready(None),
                Some(res) => {
                    let res = unwrap_any!(res);
                    if res.is_none() {
                        Poll::Pending
                    } else {
                        let (state, item): (TdPyAny, TdPyAny) = res.extract()
                            .expect("`PartIter` did not yield either `(state, item)` 2-tuple of new item and recovery state or `None` to signify no new items yet");
                        self.state = state.into();
                        Poll::Ready(Some(item))
                    }
                }
            }
        })
    }

    pub(crate) fn snapshot(&self) -> StateBytes {
        StateBytes::ser::<TdPyAny>(&self.state)
    }
}

/// All partitions for this input on a worker.
pub(crate) struct PartBundle {
    /// Partitions in order.
    ///
    /// This will be cycled through to read from all partitions.
    parts: VecDeque<(StateKey, PartIter)>,
    /// Partitions which are EOF.
    ///
    /// This is saved to ensure we snapshot their state even after
    /// EOF.
    eofd: Vec<(StateKey, PartIter)>,
}

impl PartBundle {
    fn new(parts: HashMap<StateKey, PartIter>) -> Self {
        let len = parts.len();
        Self {
            parts: parts.into_iter().collect(),
            eofd: Vec::with_capacity(len),
        }
    }

    /// Get the next item for this input, rotating through partitions.
    pub(crate) fn next(&mut self) -> Poll<Option<TdPyAny>> {
        if let Some((key, mut part)) = self.parts.pop_front() {
            match part.next() {
                Poll::Pending => {
                    self.parts.push_back((key, part));
                    Poll::Pending
                }
                Poll::Ready(None) => {
                    tracing::trace!("Partition {key:?} reached EOF");
                    self.eofd.push((key, part));
                    Poll::Pending
                }
                Poll::Ready(Some(next)) => {
                    self.parts.push_back((key, part));
                    Poll::Ready(Some(next))
                }
            }
        } else {
            Poll::Ready(None)
        }
    }

    pub(crate) fn snapshot(&self) -> Vec<(StateKey, StateBytes)> {
        let mut snaps = Vec::with_capacity(self.parts.len() + self.eofd.len());
        snaps.extend(
            self.parts
                .iter()
                .map(|(key, part)| (key.clone(), part.snapshot())),
        );
        snaps.extend(
            self.eofd
                .iter()
                .map(|(key, part)| (key.clone(), part.snapshot())),
        );
        snaps
    }
}
