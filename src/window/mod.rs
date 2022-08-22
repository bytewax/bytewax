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

use crate::recovery::{StateBytes, StateKey};
use crate::recovery::{StateUpdate, StepId};
use crate::recovery::{StatefulLogic, StatefulUnary};
use chrono::{Duration, NaiveDateTime};
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

/// Use to simulate system time in tests. Increment "now" after each
/// item.
///
/// If the dataflow has no more input, all windows are closed.
///
/// The watermark uses the most recent "now".
///
/// Args:
///
///     item_incr (datetime.timedelta): Amount to increment "now"
///         after each item.
///
///     start_at (datetime.datetime): Initial "now" / time of first
///         item. If you set this and use a window config
///
/// Returns:
///
///     Config object. Pass this as the `clock_config` parameter to
///     your windowing operator.
#[pyclass(module="bytewax.window", extends=ClockConfig)]
#[pyo3(text_signature = "(item_incr)")]
struct TestingClockConfig {
    #[pyo3(get)]
    item_incr: pyo3_chrono::Duration,
    #[pyo3(get)]
    start_at: pyo3_chrono::NaiveDateTime,
}

#[pymethods]
impl TestingClockConfig {
    /// Tell pytest to ignore this class, even though it starts with
    /// the name "Test".
    #[allow(non_upper_case_globals)]
    #[classattr]
    const __test__: bool = false;

    #[new]
    #[args(item_incr, start_at)]
    fn new(
        item_incr: pyo3_chrono::Duration,
        start_at: pyo3_chrono::NaiveDateTime,
    ) -> (Self, ClockConfig) {
        (
            Self {
                item_incr,
                start_at,
            },
            ClockConfig {},
        )
    }

    /// Pickle as a tuple.
    fn __getstate__(&self) -> (&str, pyo3_chrono::Duration, pyo3_chrono::NaiveDateTime) {
        (
            "TestingClockConfig",
            self.item_incr.clone(),
            self.start_at.clone(),
        )
    }

    /// Egregious hack see [`SqliteRecoveryConfig::__getnewargs__`].
    fn __getnewargs__(&self) -> (pyo3_chrono::Duration, pyo3_chrono::NaiveDateTime) {
        (
            pyo3_chrono::Duration(Duration::zero()),
            pyo3_chrono::NaiveDateTime(chrono::naive::MAX_DATETIME),
        )
    }

    /// Unpickle from tuple of arguments.
    fn __setstate__(&mut self, state: &PyAny) -> PyResult<()> {
        if let Ok(("TestingClockConfig", item_incr, start_at)) = state.extract() {
            self.item_incr = item_incr;
            self.start_at = start_at;
            Ok(())
        } else {
            Err(PyValueError::new_err(format!(
                "bad pickle contents for TestingClockConfig: {state:?}"
            )))
        }
    }
}

/// Use the system time inside the windowing operator to determine
/// times.
///
/// If the dataflow has no more input, all windows are closed.
///
/// Returns:
///
///     Config object. Pass this as the `clock_config` parameter to
///     your windowing operator.
#[pyclass(module="bytewax.window", extends=ClockConfig)]
struct SystemClockConfig {}

#[pymethods]
impl SystemClockConfig {
    #[new]
    #[args()]
    fn new() -> (Self, ClockConfig) {
        (Self {}, ClockConfig {})
    }

    /// Pickle as a tuple.
    fn __getstate__(&self) -> (&str,) {
        ("SystemClockConfig",)
    }

    /// Unpickle from tuple of arguments.
    fn __setstate__(&mut self, state: &PyAny) -> PyResult<()> {
        if let Ok(("SystemClockConfig",)) = state.extract() {
            Ok(())
        } else {
            Err(PyValueError::new_err(format!(
                "bad pickle contents for SystemClockConfig: {state:?}"
            )))
        }
    }
}

pub(crate) fn build_clock_builder<V: 'static>(
    py: Python,
    clock_config: Py<ClockConfig>,
) -> Result<Box<dyn Fn(Option<StateBytes>) -> Box<dyn Clock<V>>>, String> {
    let clock_config = clock_config.as_ref(py);

    if let Ok(testing_clock_config) = clock_config.downcast::<PyCell<TestingClockConfig>>() {
        let testing_clock_config = testing_clock_config.borrow();

        let item_incr = testing_clock_config.item_incr.0;
        let start_at = testing_clock_config.start_at.0;

        let builder = TestingClock::builder(item_incr, start_at);
        Ok(Box::new(builder))
    } else if let Ok(system_clock_config) = clock_config.downcast::<PyCell<SystemClockConfig>>() {
        let _system_clock_config = system_clock_config.borrow();

        let builder = SystemClock::builder();
        Ok(Box::new(builder))
    } else {
        Err(format!(
            "Unknown clock_config type: {}",
            clock_config.get_type(),
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

/// Tumbling windows of fixed duration.
///
/// Args:
///
///     length (datetime.timedelta): Length of window.
///
///     start_at (datetime.datetime): Instant of the first window. You
///         can use this to align all windows to an hour,
///         e.g. Defaults to system time of dataflow start.
///
/// Returns:
///
///     Config object. Pass this as the `window_config` parameter to
///     your windowing operator.
#[pyclass(module="bytewax.window", extends=WindowConfig)]
struct TumblingWindowConfig {
    #[pyo3(get)]
    length: pyo3_chrono::Duration,
    #[pyo3(get)]
    start_at: Option<pyo3_chrono::NaiveDateTime>,
}

#[pymethods]
impl TumblingWindowConfig {
    #[new]
    #[args(length, start_at = "None")]
    fn new(
        length: pyo3_chrono::Duration,
        start_at: Option<pyo3_chrono::NaiveDateTime>,
    ) -> (Self, WindowConfig) {
        (Self { length, start_at }, WindowConfig {})
    }

    /// Pickle as a tuple.
    fn __getstate__(
        &self,
    ) -> (
        &str,
        pyo3_chrono::Duration,
        Option<pyo3_chrono::NaiveDateTime>,
    ) {
        ("TumblingWindowConfig", self.length, self.start_at)
    }

    /// Egregious hack see [`SqliteRecoveryConfig::__getnewargs__`].
    fn __getnewargs__(&self) -> (pyo3_chrono::Duration, Option<pyo3_chrono::NaiveDateTime>) {
        (pyo3_chrono::Duration(Duration::zero()), None)
    }

    /// Unpickle from tuple of arguments.
    fn __setstate__(&mut self, state: &PyAny) -> PyResult<()> {
        if let Ok(("TumblingWindowConfig", length, start_at)) = state.extract() {
            self.length = length;
            self.start_at = start_at;
            Ok(())
        } else {
            Err(PyValueError::new_err(format!(
                "bad pickle contents for TumblingWindowConfig: {state:?}"
            )))
        }
    }
}

pub(crate) fn build_windower_builder(
    py: Python,
    window_config: Py<WindowConfig>,
) -> Result<Box<dyn Fn(Option<StateBytes>) -> Box<dyn Windower>>, String> {
    let window_config = window_config.as_ref(py);

    if let Ok(tumbling_window_config) = window_config.downcast::<PyCell<TumblingWindowConfig>>() {
        let tumbling_window_config = tumbling_window_config.borrow();

        let length = tumbling_window_config.length.0;
        let start_at = tumbling_window_config
            .start_at
            .map(|t| t.0)
            .unwrap_or(chrono::offset::Local::now().naive_local());

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
    fn watermark(&mut self, next_value: &Poll<Option<V>>) -> NaiveDateTime;

    /// Get the time for an item.
    ///
    /// This can mutate the [`ClockState`] if noting that an item has
    /// arrived should advance the clock or something.
    fn time_for(&mut self, value: &V) -> NaiveDateTime;

    /// Snapshot the internal state of this clock.
    ///
    /// Serialize any and all state necessary to re-construct this
    /// clock exactly how it is in
    /// [`StatefulWindowUnary::stateful_window_unary`]'s
    /// `logic_builder`.
    fn snapshot(&self) -> StateBytes;
}

/// Simulate system time for tests. Increment "now" after each item.
struct TestingClock {
    item_incr: Duration,
    current_time: NaiveDateTime,
}

impl TestingClock {
    fn builder<V>(
        item_incr: Duration,
        start_at: NaiveDateTime,
    ) -> impl Fn(Option<StateBytes>) -> Box<dyn Clock<V>> {
        move |resume_state_bytes: Option<StateBytes>| {
            let current_time = resume_state_bytes.map(StateBytes::de).unwrap_or(start_at);

            Box::new(Self {
                item_incr,
                current_time,
            })
        }
    }
}

impl<V> Clock<V> for TestingClock {
    fn watermark(&mut self, next_value: &Poll<Option<V>>) -> NaiveDateTime {
        match next_value {
            // If there will be no more values, close out all windows.
            Poll::Ready(None) => chrono::naive::MAX_DATETIME,
            _ => self.current_time,
        }
    }

    fn time_for(&mut self, _item: &V) -> NaiveDateTime {
        let item_time = self.current_time.clone();
        self.current_time += self.item_incr;
        item_time
    }

    fn snapshot(&self) -> StateBytes {
        StateBytes::ser(&self.current_time)
    }
}

#[test]
fn test_testing_clock() {
    use chrono::{NaiveDate, NaiveTime};

    let mut clock = TestingClock {
        item_incr: Duration::seconds(1),
        current_time: NaiveDateTime::new(
            NaiveDate::from_ymd(2022, 1, 1),
            NaiveTime::from_hms_milli(0, 0, 0, 0),
        ),
    };
    assert_eq!(
        clock.time_for(&"x"),
        NaiveDateTime::new(
            NaiveDate::from_ymd(2022, 1, 1),
            NaiveTime::from_hms_milli(0, 0, 0, 0)
        )
    );
    assert_eq!(
        clock.time_for(&"y"),
        NaiveDateTime::new(
            NaiveDate::from_ymd(2022, 1, 1),
            NaiveTime::from_hms_milli(0, 0, 1, 0)
        )
    );
    assert_eq!(
        clock.time_for(&"z"),
        NaiveDateTime::new(
            NaiveDate::from_ymd(2022, 1, 1),
            NaiveTime::from_hms_milli(0, 0, 2, 0)
        )
    );
}

/// Use the current system time.
struct SystemClock {}

impl SystemClock {
    fn builder<V>() -> impl Fn(Option<StateBytes>) -> Box<dyn Clock<V>> {
        move |_resume_state_bytes| Box::new(Self {})
    }
}

impl<V> Clock<V> for SystemClock {
    fn watermark(&mut self, next_value: &Poll<Option<V>>) -> NaiveDateTime {
        match next_value {
            // If there will be no more values, close out all windows.
            Poll::Ready(None) => chrono::naive::MAX_DATETIME,
            _ => chrono::offset::Local::now().naive_local(),
        }
    }

    fn time_for(&mut self, item: &V) -> NaiveDateTime {
        let next_value = Poll::Ready(Some(item));
        self.watermark(&next_value)
    }

    fn snapshot(&self) -> StateBytes {
        StateBytes::ser(&())
    }
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
        watermark: &NaiveDateTime,
        item_time: NaiveDateTime,
    ) -> Vec<Result<WindowKey, InsertError>>;

    /// Look at the current watermark, determine which windows are now
    /// closed, return them, and remove them from internal state.
    fn drain_closed(&mut self, watermark: &NaiveDateTime) -> Vec<WindowKey>;

    /// Return how much system time should pass before the windowing
    /// operator should be activated again, even if there is no
    /// incoming values.
    ///
    /// Use this to signal to Timely's execution when windows will be
    /// closing so we can close them without data.
    fn activate_after(&mut self, watermark: &NaiveDateTime) -> Option<Duration>;

    /// Snapshot the internal state of this windower.
    ///
    /// Serialize any and all state necessary to re-construct this
    /// windower exactly how it is in
    /// [`StatefulWindowUnary::stateful_window_unary`]'s
    /// `logic_builder`.
    fn snapshot(&self) -> StateBytes;
}

/// Use fixed-length tumbling windows aligned to a start time.
struct TumblingWindower {
    length: Duration,
    start_at: NaiveDateTime,
    close_times: HashMap<WindowKey, NaiveDateTime>,
}

impl TumblingWindower {
    fn builder(
        length: Duration,
        start_at: NaiveDateTime,
    ) -> impl Fn(Option<StateBytes>) -> Box<dyn Windower> {
        move |resume_state_bytes| {
            let close_times = resume_state_bytes.map(StateBytes::de).unwrap_or_default();

            Box::new(Self {
                length,
                start_at,
                close_times,
            })
        }
    }
}

impl Windower for TumblingWindower {
    fn insert(
        &mut self,
        watermark: &NaiveDateTime,
        item_time: NaiveDateTime,
    ) -> Vec<Result<WindowKey, InsertError>> {
        let since_start_at = item_time - self.start_at;
        let window_count = since_start_at.num_milliseconds() / self.length.num_milliseconds();

        let key = WindowKey(window_count);
        let close_at = self.start_at + self.length * (window_count as i32 + 1);

        if &close_at < watermark {
            vec![Err(InsertError::Late(key))]
        } else {
            self.close_times.insert(key.clone(), close_at);
            vec![Ok(key)]
        }
    }

    fn drain_closed(&mut self, watermark: &NaiveDateTime) -> Vec<WindowKey> {
        // TODO: Gosh I really want [`HashMap::drain_filter`].
        let mut future_close_times = HashMap::new();
        let mut closed_ids = Vec::new();

        for (id, close_at) in self.close_times.iter() {
            if close_at < &watermark {
                closed_ids.push(id.clone());
            } else {
                future_close_times.insert(id.clone(), close_at.clone());
            }
        }

        self.close_times = future_close_times;
        closed_ids
    }

    fn activate_after(&mut self, watermark: &NaiveDateTime) -> Option<Duration> {
        let watermark = watermark.clone();
        self.close_times
            .values()
            .cloned()
            .map(|t| t - watermark)
            .min()
    }

    fn snapshot(&self) -> StateBytes {
        StateBytes::ser(&self.close_times)
    }
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
    key_to_resume_state_bytes: HashMap<WindowKey, StateBytes>,
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
            )> = resume_state_bytes.map(|state_bytes| state_bytes.de());
            let (clock_resume_state_bytes, windower_resume_state_bytes, key_to_resume_state_bytes) =
                match resume_state_bytes {
                    Some((c, w, l)) => (Some(c), Some(w), Some(l)),
                    None => (None, None, None),
                };

            let clock = clock_builder(clock_resume_state_bytes);
            let windower = windower_builder(windower_resume_state_bytes);
            let logic_cache = HashMap::new();
            let logic_builder = logic_builder.clone();
            let key_to_resume_state_bytes = key_to_resume_state_bytes.unwrap_or_default();

            Self {
                clock,
                windower,
                logic_cache,
                logic_builder,
                key_to_resume_state_bytes,
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
                            .or_insert_with_key(|key| {
                                // Remove so we only use the resume
                                // state once.
                                let resume_state_bytes = self.key_to_resume_state_bytes.remove(key);
                                (self.logic_builder)(resume_state_bytes)
                            });
                        let window_output = logic.exec(Some(value));
                        output.extend(window_output.into_iter().map(Ok));
                    }
                }
            }
        }

        for window_key in self.windower.drain_closed(&watermark) {
            if let Some(mut logic) = self.logic_cache.remove(&window_key) {
                let window_output = logic.exec(None);
                output.extend(window_output.into_iter().map(Ok));
            }
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
            .map(|(window_key, logic)| (window_key.clone(), logic.snapshot()))
            .collect();
        let state = (
            self.clock.snapshot(),
            self.windower.snapshot(),
            window_to_logic_resume_state_bytes,
        );
        StateUpdate::Upsert(StateBytes::ser(&state))
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
        Stream<S, (StateKey, (StepId, StateUpdate))>,
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
        Stream<S, (StateKey, (StepId, StateUpdate))>,
    ) {
        self.stateful_unary(
            step_id.clone(),
            WindowStatefulLogic::builder(clock_builder, windower_builder, logic_builder),
            resume_state,
        )
    }
}

pub(crate) fn register(_py: Python, m: &PyModule) -> PyResult<()> {
    m.add_class::<ClockConfig>()?;
    m.add_class::<TestingClockConfig>()?;
    m.add_class::<SystemClockConfig>()?;
    m.add_class::<WindowConfig>()?;
    m.add_class::<TumblingWindowConfig>()?;
    Ok(())
}
