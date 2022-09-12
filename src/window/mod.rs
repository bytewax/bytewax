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
use crate::pyo3_extensions::TdPyAny;
use crate::recovery::{EpochData, StateBytes, StateKey};
use crate::recovery::{StateUpdate, StepId};
use crate::recovery::{StatefulLogic, StatefulUnary};
use crate::StringResult;
use chrono::{DateTime, Duration, Utc};
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fmt::Debug;
use std::marker::PhantomData;
use std::rc::Rc;
use std::task::Poll;
use timely::dataflow::{Scope, Stream};
use timely::{Data, ExchangeData};

pub(crate) mod event_time_clock;
pub(crate) mod system_clock;
pub(crate) mod testing_clock;
pub(crate) mod tumbling_window;

use self::event_time_clock::EventClockConfig;
use self::system_clock::SystemClockConfig;
use self::testing_clock::{PyTestingClock, TestingClockConfig};
use self::tumbling_window::{TumblingWindowConfig, TumblingWindower};

/// Base class for a clock config.
///
/// This describes how a windowing operator should determine the
/// current time and the time for each element.
///
/// Use a specific subclass of this that matches the time definition
/// you'd like to use.
#[pyclass(module = "bytewax.window", subclass)]
#[pyo3(text_signature = "()")]
pub(crate) struct ClockConfig;

impl ClockConfig {
    /// Create an "empty" [`Self`] just for use in `__getnewargs__`.
    #[allow(dead_code)]
    pub(crate) fn pickle_new(py: Python) -> Py<Self> {
        PyCell::new(py, ClockConfig {}).unwrap().into()
    }
}

#[pymethods]
impl ClockConfig {
    #[new]
    fn new() -> Self {
        Self {}
    }

    /// Pickle as a tuple.
    fn __getstate__(&self) -> (&str,) {
        ("ClockConfig",)
    }

    /// Unpickle from tuple of arguments.
    fn __setstate__(&mut self, state: &PyAny) -> PyResult<()> {
        if let Ok(("ClockConfig",)) = state.extract() {
            Ok(())
        } else {
            Err(PyValueError::new_err(format!(
                "bad pickle contents for ClockConfig: {state:?}"
            )))
        }
    }
}

/// A type representing a function that takes an optional serialized
/// state and returns a Clock.
type Builder<V> = Box<dyn Fn(Option<StateBytes>) -> Box<dyn Clock<V>>>;

/// The `builder` function consumes a ClockConfig to build a Clock.
pub(crate) trait ClockBuilder<V> {
    fn builder(self) -> Builder<V>;
}

pub(crate) fn build_clock_builder(
    clock_config: &PyCell<ClockConfig>,
) -> StringResult<Builder<TdPyAny>> {
    // System clock
    if let Ok(conf) = clock_config.extract::<SystemClockConfig>() {
        Ok(conf.builder())
    // Testing clock
    } else if let Ok(conf) = clock_config.extract::<TestingClockConfig>() {
        Ok(conf.builder())
    // Event clock
    } else if let Ok(conf) = clock_config.extract::<EventClockConfig>() {
        Ok(conf.builder())
    // Anything else
    } else {
        Err(format!(
            "Unknown clock_config type: {}",
            clock_config.get_type()
        ))
    }
}

/// Base class for a windower config.
///
/// This describes the type of windows you would like.
///
/// Use a specific subclass of this that matches the window definition
/// you'd like to use.
#[pyclass(module = "bytewax.window", subclass)]
#[pyo3(text_signature = "()")]
pub(crate) struct WindowConfig;

impl WindowConfig {
    /// Create an "empty" [`Self`] just for use in `__getnewargs__`.
    #[allow(dead_code)]
    pub(crate) fn pickle_new(py: Python) -> Py<Self> {
        PyCell::new(py, WindowConfig {}).unwrap().into()
    }
}

#[pymethods]
impl WindowConfig {
    #[new]
    fn new() -> Self {
        Self {}
    }

    /// Pickle as a tuple.
    fn __getstate__(&self) -> (&str,) {
        ("WindowConfig",)
    }

    /// Unpickle from tuple of arguments.
    fn __setstate__(&mut self, state: &PyAny) -> PyResult<()> {
        if let Ok(("WindowConfig",)) = state.extract() {
            Ok(())
        } else {
            Err(PyValueError::new_err(format!(
                "bad pickle contents for WindowConfig: {state:?}"
            )))
        }
    }
}

pub(crate) fn build_windower_builder(
    py: Python,
    window_config: Py<WindowConfig>,
) -> StringResult<Box<dyn Fn(Option<StateBytes>) -> Box<dyn Windower>>> {
    let window_config = window_config.as_ref(py);

    if let Ok(tumbling_window_config) = window_config.downcast::<PyCell<TumblingWindowConfig>>() {
        let tumbling_window_config = tumbling_window_config.borrow();

        let length = tumbling_window_config.length;
        let start_at = tumbling_window_config.start_at.unwrap_or_else(Utc::now);

        let builder = TumblingWindower::builder(length, start_at);
        Ok(Box::new(builder))
    } else {
        Err(format!(
            "Unknown window_config type: {}",
            window_config.get_type(),
        ))
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
    fn snapshot(&self) -> StateBytes;
}

/// Unique ID for a window coming from a single [`Windower`] impl.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub(crate) struct WindowKey(i64);

/// An error that can occur when windowing an item.
#[derive(Clone, Copy, Debug, Serialize, Deserialize)]
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
        item_time: DateTime<Utc>,
    ) -> Vec<Result<WindowKey, InsertError>>;

    /// Look at the current watermark, determine which windows are now
    /// closed, return them, and remove them from internal state.
    fn drain_closed(&mut self, watermark: &DateTime<Utc>) -> Vec<WindowKey>;

    /// Return how much system time should pass before the windowing
    /// operator should be activated again, even if there is no
    /// incoming values.
    ///
    /// Use this to signal to Timely's execution when windows will be
    /// closing so we can close them without data.
    fn activate_after(&mut self, watermark: &DateTime<Utc>) -> Option<Duration>;

    /// Snapshot the internal state of this windower.
    ///
    /// Serialize any and all state necessary to re-construct this
    /// windower exactly how it is in
    /// [`StatefulWindowUnary::stateful_window_unary`]'s
    /// `logic_builder`.
    fn snapshot(&self) -> StateBytes;
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
pub(crate) trait WindowLogic<V, R, I: IntoIterator<Item = R>> {
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
    fn exec(&mut self, next_value: Option<V>) -> I;

    /// Snapshot the internal state of this logic.
    ///
    /// Serialize any and all state necessary to re-construct this
    /// window exactly how it is in
    /// [`StatefulWindowUnary::stateful_window_unary`]'s
    /// `logic_builder`.
    fn snapshot(&self) -> StateBytes;
}

/// Implement [`WindowLogic`] in terms of [`StatefulLogic`], bridging
/// the gap between a windowing operator and the underlying stateful
/// operator.
struct WindowStatefulLogic<V, R, I, L, LB>
where
    I: IntoIterator<Item = R>,
    L: WindowLogic<V, R, I>,
    LB: Fn(Option<StateBytes>) -> L,
{
    clock: Box<dyn Clock<V>>,
    windower: Box<dyn Windower>,
    logic_cache: HashMap<WindowKey, L>,
    /// This has to be an [`Rc`] because multiple keys all need to use
    /// the same builder and we don't know how many there'll be.
    logic_builder: Rc<LB>,
    out_pdata: PhantomData<R>,
    out_iter_pdata: PhantomData<I>,
}

impl<V, R, I, L, LB> WindowStatefulLogic<V, R, I, L, LB>
where
    I: IntoIterator<Item = R>,
    L: WindowLogic<V, R, I>,
    LB: Fn(Option<StateBytes>) -> L,
{
    fn builder<
        CB: Fn(Option<StateBytes>) -> Box<dyn Clock<V>> + 'static, // Clock builder
        WB: Fn(Option<StateBytes>) -> Box<dyn Windower> + 'static, // Windower builder
    >(
        clock_builder: CB,
        windower_builder: WB,
        logic_builder: LB,
    ) -> impl Fn(Option<StateBytes>) -> Self {
        let logic_builder = Rc::new(logic_builder);

        move |resume_state_bytes| {
            let resume_state_bytes: Option<(
                StateBytes,
                StateBytes,
                HashMap<WindowKey, StateBytes>,
            )> = resume_state_bytes
                .map(StateBytes::de::<(StateBytes, StateBytes, HashMap<WindowKey, StateBytes>)>);
            let (clock_resume_state_bytes, windower_resume_state_bytes, key_to_resume_state_bytes) =
                match resume_state_bytes {
                    Some((c, w, l)) => (Some(c), Some(w), Some(l)),
                    None => (None, None, None),
                };

            let clock = clock_builder(clock_resume_state_bytes);
            let windower = windower_builder(windower_resume_state_bytes);
            let logic_cache = key_to_resume_state_bytes
                .unwrap_or_default()
                .into_iter()
                .map(|(window_key, logic_resume_state_bytes)| {
                    (window_key, logic_builder(Some(logic_resume_state_bytes)))
                })
                .collect();
            let logic_builder = logic_builder.clone();

            Self {
                clock,
                windower,
                logic_cache,
                logic_builder,
                out_pdata: PhantomData,
                out_iter_pdata: PhantomData,
            }
        }
    }
}

impl<V: Clone, R, I, L, LB>
    StatefulLogic<V, Result<R, WindowError<V>>, Vec<Result<R, WindowError<V>>>>
    for WindowStatefulLogic<V, R, I, L, LB>
where
    I: IntoIterator<Item = R>,
    L: WindowLogic<V, R, I>,
    LB: Fn(Option<StateBytes>) -> L,
{
    fn exec(
        &mut self,
        next_value: Poll<Option<V>>,
    ) -> (Vec<Result<R, WindowError<V>>>, Option<std::time::Duration>) {
        let mut output = Vec::new();

        let watermark = self.clock.watermark(&next_value);

        if let Poll::Ready(Some(value)) = next_value {
            let item_time = self.clock.time_for(&value);

            for window_result in self.windower.insert(&watermark, item_time) {
                let value = value.clone();
                match window_result {
                    Err(InsertError::Late(_window_key)) => {
                        output.push(Err(WindowError::Late(value)));
                    }
                    Ok(window_key) => {
                        let logic = self
                            .logic_cache
                            .entry(window_key)
                            .or_insert_with(|| (self.logic_builder)(None));
                        let window_output = logic.exec(Some(value));
                        output.extend(window_output.into_iter().map(Ok));
                    }
                }
            }
        }

        for window_key in self.windower.drain_closed(&watermark) {
            let mut logic = self
                .logic_cache
                .remove(&window_key)
                .expect("No logic for closed window");

            let window_output = logic.exec(None);
            output.extend(window_output.into_iter().map(Ok));
        }

        let activate_after = self
            .windower
            .activate_after(&watermark)
            // [`chrono::Duration`] supports negative
            // durations but [`std::time::Duration`] does not,
            // so clamp to 0.
            .map(|d| d.to_std().unwrap_or(std::time::Duration::ZERO));

        (output, activate_after)
    }

    // TODO: Set a timeout on destroying window state per key?
    /// Note that we never return [`StateUpdate::Reset`] so once a key
    /// is seen, we permanently allocate state for the window system.
    fn snapshot(&self) -> StateUpdate {
        let window_to_logic_resume_state_bytes: HashMap<WindowKey, StateBytes> = self
            .logic_cache
            .iter()
            .map(|(window_key, logic)| (*window_key, logic.snapshot()))
            .collect();
        let state = (
            self.clock.snapshot(),
            self.windower.snapshot(),
            window_to_logic_resume_state_bytes,
        );
        StateUpdate::Upsert(StateBytes::ser::<(
            StateBytes,
            StateBytes,
            HashMap<WindowKey, StateBytes>,
        )>(&state))
    }
}

/// Extension trait for [`Stream`].
pub(crate) trait StatefulWindowUnary<S: Scope, V: ExchangeData> {
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
    fn stateful_window_unary<
        R: Data,                                                   // Output item type
        I: IntoIterator<Item = R> + 'static,                       // Iterator of output items
        CB: Fn(Option<StateBytes>) -> Box<dyn Clock<V>> + 'static, // Clock builder
        WB: Fn(Option<StateBytes>) -> Box<dyn Windower> + 'static, // Windower builder
        L: WindowLogic<V, R, I> + 'static,                         // Logic
        LB: Fn(Option<StateBytes>) -> L + 'static,                 // Logic builder
    >(
        &self,
        step_id: StepId,
        clock_builder: CB,
        windower_builder: WB,
        logic_builder: LB,
        resume_state: HashMap<StateKey, StateBytes>,
    ) -> (
        Stream<S, (StateKey, Result<R, WindowError<V>>)>,
        Stream<S, EpochData>,
    );
}

impl<S, V: ExchangeData + Debug> StatefulWindowUnary<S, V> for Stream<S, (StateKey, V)>
where
    S: Scope<Timestamp = u64>,
{
    fn stateful_window_unary<
        R: Data,                                                   // Output item type
        I: IntoIterator<Item = R> + 'static,                       // Iterator of output items
        CB: Fn(Option<StateBytes>) -> Box<dyn Clock<V>> + 'static, // Clock builder
        WB: Fn(Option<StateBytes>) -> Box<dyn Windower> + 'static, // Windower builder
        L: WindowLogic<V, R, I> + 'static,                         // Logic
        LB: Fn(Option<StateBytes>) -> L + 'static,                 // Logic builder
    >(
        &self,
        step_id: StepId,
        clock_builder: CB,
        windower_builder: WB,
        logic_builder: LB,
        resume_state: HashMap<StateKey, StateBytes>,
    ) -> (
        Stream<S, (StateKey, Result<R, WindowError<V>>)>,
        Stream<S, EpochData>,
    ) {
        self.stateful_unary(
            step_id,
            WindowStatefulLogic::builder(clock_builder, windower_builder, logic_builder),
            resume_state,
        )
    }
}

pub(crate) fn register(_py: Python, m: &PyModule) -> PyResult<()> {
    m.add_class::<ClockConfig>()?;
    m.add_class::<EventClockConfig>()?;
    m.add_class::<TestingClockConfig>()?;
    m.add_class::<PyTestingClock>()?;
    m.add_class::<SystemClockConfig>()?;
    m.add_class::<WindowConfig>()?;
    m.add_class::<TumblingWindowConfig>()?;
    Ok(())
}
