//! Internal code for windowing.
//!
//! For a user-centric version of windowing, read the `bytewax.window`
//! Python module docstring. Read that first.
//!
//! Architecture
//! ------------
//!
//! Windowing is based around two core traits and one generic core
//! operator: [`Clock`], [`Windower`], and
//! [`StatefulWindowUnary::stateful_window_unary`].
//!
//! All user-facing windowing operators like
//! [`crate::dataflow::Dataflow::reduce_window`] are built on top of
//! [`StatefulWindowUnary::stateful_window_unary`] so we only have to
//! write the window management code once.
//!
//! To implement a new user-facing windowing operator, implement
//! [`WindowLogic`] and pass that to
//! [`StatefulWindowUnary::stateful_window_unary`].
//!
//! That operator itself is based upon
//! [`StatefulUnary::stateful_unary`], which provides a generic
//! abstraction to the recovery system. We get recovery for "free" as
//! long as we play by the rules of that operator.
//!
//! This system follows our standard pattern of having parallel Python
//! config objects and Rust impl structs for each trait of behavior we
//! want. E.g. [`SystemClockConfig`] represents a token in Python for
//! how to create a [`SystemClock`].

use std::collections::HashMap;
use std::fmt::Debug;
use std::marker::PhantomData;
use std::rc::Rc;
use std::task::Poll;

use chrono::prelude::*;
use pyo3::exceptions::PyTypeError;
use pyo3::prelude::*;
use pyo3::types::PyDict;
use serde::Deserialize;
use serde::Serialize;
use timely::dataflow::Scope;
use timely::dataflow::Stream;
use timely::Data;
use timely::ExchangeData;

use crate::errors::tracked_err;
use crate::operators::stateful_unary::*;
use crate::pyo3_extensions::PyConfigClass;
use crate::pyo3_extensions::TdPyAny;
use crate::unwrap_any;

pub(crate) mod clock;
pub(crate) mod session_window;
pub(crate) mod sliding_window;
pub(crate) mod tumbling_window;

use self::session_window::SessionWindow;
use self::sliding_window::SlidingWindow;
use self::tumbling_window::TumblingWindow;
use clock::{event_time_clock::EventClockConfig, system_clock::SystemClockConfig, ClockConfig};

/// Base class for a windower config.
///
/// This describes the type of windows you would like.
///
/// Use a specific subclass of this that matches the window definition
/// you'd like to use.
#[pyclass(module = "bytewax.window", subclass)]
pub(crate) struct WindowConfig;

#[pymethods]
impl WindowConfig {
    #[new]
    fn new() -> Self {
        Self {}
    }
}

type Builder = Box<dyn Fn(Option<TdPyAny>) -> Box<dyn Windower>>;

pub(crate) trait WindowBuilder {
    fn build(&self, py: Python) -> PyResult<Builder>;
}

impl PyConfigClass<Box<dyn WindowBuilder>> for Py<WindowConfig> {
    fn downcast(&self, py: Python) -> PyResult<Box<dyn WindowBuilder>> {
        if let Ok(conf) = self.extract::<TumblingWindow>(py) {
            Ok(Box::new(conf))
        } else if let Ok(conf) = self.extract::<SlidingWindow>(py) {
            Ok(Box::new(conf))
        } else if let Ok(conf) = self.extract::<SessionWindow>(py) {
            Ok(Box::new(conf))
        } else {
            let pytype = self.as_ref(py).get_type();
            Err(tracked_err::<PyTypeError>(&format!(
                "Unknown window_config type: {pytype}"
            )))
        }
    }
}

impl WindowBuilder for Py<WindowConfig> {
    fn build(&self, py: Python) -> PyResult<Builder> {
        self.downcast(py)?.build(py)
    }
}

/// Defines the sense of time for a windowing operator.
pub(crate) trait Clock<V> {
    /// Return the current time of the stream.
    ///
    /// There should be no more items with times before the
    /// watermark. If there unexpectedly is, those items are marked as
    /// late and routed separately.
    ///
    /// This will be called with each value in arrival order, but also
    /// might be called between values and should use the internal
    /// state to still return the watermark.
    ///
    /// This can mutate the [`ClockState`] on every call to ensure
    /// future calls are accurate.
    fn watermark(&mut self, next_value: &Poll<Option<V>>) -> DateTime<Utc>;

    /// Get the time for an item.
    ///
    /// This can mutate the [`ClockState`] if noting that an item has
    /// arrived should advance the clock or something.
    fn time_for(&mut self, value: &V) -> DateTime<Utc>;

    /// Snapshot the internal state of this clock.
    ///
    /// Serialize any and all state necessary to re-construct this
    /// clock exactly how it is in
    /// [`StatefulWindowUnary::stateful_window_unary`]'s
    /// `logic_builder`.
    fn snapshot(&self) -> TdPyAny;
}

/// Unique ID for a window coming from a single [`Windower`] impl.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize, FromPyObject)]
pub(crate) struct WindowKey(i64);

impl ToPyObject for WindowKey {
    fn to_object(&self, py: Python<'_>) -> PyObject {
        self.0.to_object(py)
    }
}

impl IntoPy<PyObject> for WindowKey {
    fn into_py(self, py: Python<'_>) -> PyObject {
        self.0.into_py(py)
    }
}

/// An error that can occur when windowing an item.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) enum InsertError {
    /// The inserted item was late for a window.
    Late(WindowKey),
}

/// Defines a kind of time-based windower.
///
/// This should keep internal state of which windows are open and
/// accepting values.
///
/// A separate instance of this is created for each key in the
/// stateful stream. There is no way to interact across keys.
pub(crate) trait Windower {
    /// Attempt to insert an incoming value into a window, creating it
    /// if necessary.
    ///
    /// If the current item is "late" for all windows, return
    /// [`WindowError::Late`].
    ///
    /// This should update any internal state.
    fn insert(
        &mut self,
        watermark: &DateTime<Utc>,
        item_time: &DateTime<Utc>,
    ) -> Vec<Result<WindowKey, InsertError>>;

    /// Look at the current watermark, determine which windows are now
    /// closed, return them, and remove them from internal state.
    fn drain_closed(&mut self, watermark: &DateTime<Utc>) -> Vec<WindowKey>;

    /// Is this windower currently tracking any windows?
    fn is_empty(&self) -> bool;

    /// Return the system time estimate of the next window close, if
    /// any.
    fn next_close(&self) -> Option<DateTime<Utc>>;

    /// Snapshot the internal state of this windower.
    ///
    /// Serialize any and all state necessary to re-construct this
    /// windower exactly how it is in
    /// [`StatefulWindowUnary::stateful_window_unary`]'s
    /// `logic_builder`.
    fn snapshot(&self) -> TdPyAny;
}

/// Possible errors emitted downstream from windowing operators.
#[derive(Clone, Serialize, Deserialize)]
pub(crate) enum WindowError<V> {
    /// This item was late for a window.
    Late(V),
}

/// Impl this trait to create a windowing operator.
///
/// A separate instance of this will be create for each window a
/// [`Windower`] creates. There is no way to interact across windows
/// or keys.
pub(crate) trait WindowLogic<V, R, I>
where
    V: Data,
    I: IntoIterator<Item = R>,
{
    /// Logic to run when this the window sees a new value.
    ///
    /// `next_value` has the same semantics as
    /// [`std::iter::Iterator::next`]: An incoming value or [`None`]
    /// if the window for this key has just closed.
    ///
    /// This must return any values to be emitted downstream. You
    /// probably only want to emit values when `next_value` is
    /// [`None`], signaling the window has closed, but you might want
    /// something more generic. They will be automatically paired with
    /// the key in the output stream.
    fn with_next(&mut self, next_value: Option<(V, DateTime<Utc>)>) -> I;

    /// Snapshot the internal state of this logic.
    ///
    /// Serialize any and all state necessary to re-construct this
    /// window exactly how it is in
    /// [`StatefulWindowUnary::stateful_window_unary`]'s
    /// `logic_builder`.
    fn snapshot(&self) -> TdPyAny;
}

/// Implement [`WindowLogic`] in terms of [`StatefulLogic`], bridging
/// the gap between a windowing operator and the underlying stateful
/// operator.
struct WindowStatefulLogic<V, R, I, L, LB>
where
    V: Data,
    I: IntoIterator<Item = R>,
    L: WindowLogic<V, R, I>,
    LB: Fn(Option<TdPyAny>) -> L,
{
    clock: Box<dyn Clock<V>>,
    windower: Box<dyn Windower>,
    current_state: HashMap<WindowKey, L>,
    /// This has to be an [`Rc`] because multiple keys all need to use
    /// the same builder and we don't know how many there'll be.
    logic_builder: Rc<LB>,
    out_pdata: PhantomData<R>,
    out_iter_pdata: PhantomData<I>,
}

impl<V, R, I, L, LB> WindowStatefulLogic<V, R, I, L, LB>
where
    V: Data,
    I: IntoIterator<Item = R>,
    L: WindowLogic<V, R, I>,
    LB: Fn(Option<TdPyAny>) -> L,
{
    fn builder<
        CB: Fn(Option<TdPyAny>) -> Box<dyn Clock<V>> + 'static, // Clock builder
        WB: Fn(Option<TdPyAny>) -> Box<dyn Windower> + 'static, // Windower builder
    >(
        clock_builder: CB,
        windower_builder: WB,
        logic_builder: LB,
    ) -> impl Fn(Option<TdPyAny>) -> Self {
        let logic_builder = Rc::new(logic_builder);

        move |resume_state| {
            let (clock_state, windower_state, logic_states) = if let Some(state) = resume_state {
                unwrap_any!(Python::with_gil(|py| -> PyResult<_> {
                    let state = state.as_ref(py);
                    let clock_state = state.get_item("clock")?.into();
                    let windower_state = state.get_item("windower")?.into();
                    let logic_states: HashMap<WindowKey, TdPyAny> =
                        state.get_item("logic")?.extract()?;
                    Ok((Some(clock_state), Some(windower_state), Some(logic_states)))
                }))
            } else {
                (None, None, None)
            };

            let clock = clock_builder(clock_state);
            let windower = windower_builder(windower_state);
            let current_state = logic_states
                .unwrap_or_default()
                .into_iter()
                .map(|(key, state)| (key, logic_builder(Some(state))))
                .collect();
            let logic_builder = logic_builder.clone();

            Self {
                clock,
                windower,
                current_state,
                logic_builder,
                out_pdata: PhantomData,
                out_iter_pdata: PhantomData,
            }
        }
    }
}

impl<V, R, I, L, LB> StatefulLogic<V, Result<R, WindowError<V>>, Vec<Result<R, WindowError<V>>>>
    for WindowStatefulLogic<V, R, I, L, LB>
where
    V: Data + Debug,
    I: IntoIterator<Item = R>,
    L: WindowLogic<V, R, I>,
    LB: Fn(Option<TdPyAny>) -> L,
{
    fn on_awake(&mut self, next_value: Poll<Option<V>>) -> Vec<Result<R, WindowError<V>>> {
        let mut output = Vec::new();

        let watermark = self.clock.watermark(&next_value);
        tracing::trace!("Watermark at {watermark:?}");

        if let Poll::Ready(Some(value)) = next_value {
            let item_time = self.clock.time_for(&value);
            tracing::trace!("{value:?} has time {item_time:?}");

            for window_result in self.windower.insert(&watermark, &item_time) {
                let value = value.clone();
                match window_result {
                    Err(InsertError::Late(window_key)) => {
                        tracing::trace!("{value:?} late for {window_key:?}");
                        output.push(Err(WindowError::Late(value)));
                    }
                    Ok(window_key) => {
                        tracing::trace!("{value:?} in {window_key:?}");
                        let logic = self
                            .current_state
                            .entry(window_key)
                            .or_insert_with(|| (self.logic_builder)(None));
                        let window_output = logic.with_next(Some((value, item_time)));
                        output.extend(window_output.into_iter().map(Ok));
                    }
                }
            }
        }

        for closed_window in self.windower.drain_closed(&watermark) {
            let mut logic = self
                .current_state
                .remove(&closed_window)
                .expect("No logic for closed window");

            let window_output = logic.with_next(None);
            output.extend(window_output.into_iter().map(Ok));
        }

        output
    }

    fn fate(&self) -> LogicFate {
        if self.windower.is_empty() {
            LogicFate::Discard
        } else {
            LogicFate::Retain
        }
    }

    fn next_awake(&self) -> Option<DateTime<Utc>> {
        let next_awake = self.windower.next_close();
        tracing::trace!("Next awake {next_awake:?}");
        next_awake
    }

    fn snapshot(&self) -> TdPyAny {
        unwrap_any!(Python::with_gil(|py| -> PyResult<TdPyAny> {
            let state = PyDict::new(py);
            state.set_item("clock", self.clock.snapshot())?;
            state.set_item("windower", self.windower.snapshot())?;
            let logic_states: HashMap<WindowKey, TdPyAny> = self
                .current_state
                .iter()
                .map(|(key, logic)| (*key, logic.snapshot()))
                .collect();
            state.set_item("logic", logic_states)?;
            let state: PyObject = state.into();
            Ok(state.into())
        }))
    }
}

/// Extension trait for [`Stream`].
pub(crate) trait StatefulWindowUnary<S, V>
where
    S: Scope,
    V: ExchangeData,
{
    /// Create a new generic windowing operator.
    ///
    /// This is the core Timely operator that all Bytewax windowing
    /// operators are implemented in terms of. We do this so we only
    /// have to implement the detailed windowing code once.
    ///
    /// # Input
    ///
    /// Because this is a stateful operator, the input must be a
    /// stream of `(key, value)` 2-tuples. They will automatically be
    /// routed to the same worker and windowing state by key.
    ///
    /// This means that windows are distinct per-key and there is no
    /// interaction between them.
    ///
    /// Currently we permanently allocate windowing state per-key; it
    /// is never freed; if you have a lot of fleeting keys, perhaps
    /// re-think your key space.
    ///
    /// # Logic Builder
    ///
    /// This is a closure that should build a new instance of your
    /// logic for a window, given the last snapshot of its state for
    /// that window. You should implement the deserialization from
    /// [`StateBytes`] in this builder; it should be the reverse of
    /// your [`WindowLogic::snapshot`].
    ///
    /// See [`WindowLogic`] for the semantics of the logic.
    ///
    /// This will be called periodically as new windows are created.
    ///
    /// # Output
    ///
    /// The output will be a stream of `(key, value)` output
    /// 2-tuples. Values emitted by [`WindowLogic::exec`] will
    /// automatically be paired with the key in the output stream.
    #[allow(clippy::type_complexity)]
    fn stateful_window_unary<R, I, CB, WB, L, LB>(
        &self,
        step_id: StepId,
        clock_builder: CB,
        windower_builder: WB,
        logic_builder: LB,
        resume_epoch: ResumeEpoch,
        loads: &Stream<S, Snapshot>,
    ) -> (
        Stream<S, (StateKey, Result<R, WindowError<V>>)>,
        Stream<S, Snapshot>,
    )
    where
        R: Data,                                                // Output item type
        I: IntoIterator<Item = R> + 'static,                    // Iterator of output items
        CB: Fn(Option<TdPyAny>) -> Box<dyn Clock<V>> + 'static, // Clock builder
        WB: Fn(Option<TdPyAny>) -> Box<dyn Windower> + 'static, // Windower builder
        L: WindowLogic<V, R, I> + 'static,                      // Logic
        LB: Fn(Option<TdPyAny>) -> L + 'static                  // Logic builder
    ;
}

impl<S, V> StatefulWindowUnary<S, V> for Stream<S, (StateKey, V)>
where
    S: Scope<Timestamp = u64>,
    V: ExchangeData + Debug,
{
    fn stateful_window_unary<R, I, CB, WB, L, LB>(
        &self,
        step_id: StepId,
        clock_builder: CB,
        windower_builder: WB,
        logic_builder: LB,
        resume_epoch: ResumeEpoch,
        loads: &Stream<S, Snapshot>,
    ) -> (
        Stream<S, (StateKey, Result<R, WindowError<V>>)>,
        Stream<S, Snapshot>,
    )
    where
        R: Data,                                                // Output item type
        I: IntoIterator<Item = R> + 'static,                    // Iterator of output items
        CB: Fn(Option<TdPyAny>) -> Box<dyn Clock<V>> + 'static, // Clock builder
        WB: Fn(Option<TdPyAny>) -> Box<dyn Windower> + 'static, // Windower builder
        L: WindowLogic<V, R, I> + 'static,                      // Logic
        LB: Fn(Option<TdPyAny>) -> L + 'static,                 // Logic builder
    {
        self.stateful_unary(
            step_id,
            WindowStatefulLogic::builder(clock_builder, windower_builder, logic_builder),
            resume_epoch,
            loads,
        )
    }
}

pub(crate) fn register(_py: Python, m: &PyModule) -> PyResult<()> {
    m.add_class::<ClockConfig>()?;
    m.add_class::<EventClockConfig>()?;
    m.add_class::<SystemClockConfig>()?;
    m.add_class::<WindowConfig>()?;
    m.add_class::<TumblingWindow>()?;
    m.add_class::<SlidingWindow>()?;
    m.add_class::<SessionWindow>()?;
    Ok(())
}
