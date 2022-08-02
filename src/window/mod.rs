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
//! That operator itself is based upon
//! [`crate::operators::StatefulUnary`], which provides a generic
//! abstraction to the recovery system. We get recovery for "free" as
//! long as we play by the rules of that operator.
//!
//! This system follows our standard pattern of having parallel Python
//! config objects and Rust impl structs for each trait of behavior we
//! want. E.g. [`SystemClockConfig`] represents a token in Python for
//! how to create a [`SystemClock`].

use crate::dataflow::StepId;
use crate::operators::{StatefulUnary, StatefulUnaryLogicReturn};
use crate::pyo3_extensions::{StateKey, TdPyAny, TdPyCallable};
use crate::recovery::StateBackup;
use crate::with_traceback;
use chrono::{Duration, NaiveDateTime};
use log::debug;
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fmt::Debug;
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
/// Returns:
///
///     Config object. Pass this as the `clock_config` parameter to
///     your windowing operator.
#[pyclass(module="bytewax.window", extends=ClockConfig)]
#[pyo3(text_signature = "(item_incr)")]
struct TestingClockConfig {
    #[pyo3(get)]
    item_incr: pyo3_chrono::Duration,
}

#[pymethods]
impl TestingClockConfig {
    /// Tell pytest to ignore this class, even though it starts with
    /// the name "Test".
    #[allow(non_upper_case_globals)]
    #[classattr]
    const __test__: bool = false;

    #[new]
    #[args(item_incr)]
    fn new(item_incr: pyo3_chrono::Duration) -> (Self, ClockConfig) {
        (Self { item_incr }, ClockConfig {})
    }

    /// Pickle as a tuple.
    fn __getstate__(&self) -> (&str, pyo3_chrono::Duration) {
        ("TestingClockConfig", self.item_incr.clone())
    }

    /// Egregious hack see [`SqliteRecoveryConfig::__getnewargs__`].
    fn __getnewargs__(&self) -> (pyo3_chrono::Duration,) {
        (pyo3_chrono::Duration(Duration::zero()),)
    }

    /// Unpickle from tuple of arguments.
    fn __setstate__(&mut self, state: &PyAny) -> PyResult<()> {
        if let Ok(("TestingClockConfig", item_incr)) = state.extract() {
            self.item_incr = item_incr;
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

pub(crate) fn build_clock<V>(
    py: Python,
    clock_config: Py<ClockConfig>,
) -> Result<Box<dyn Clock<V>>, String> {
    let clock_config = clock_config.as_ref(py);

    if let Ok(testing_clock_config) = clock_config.downcast::<PyCell<TestingClockConfig>>() {
        let testing_clock_config = testing_clock_config.borrow();

        let item_incr = testing_clock_config.item_incr.0;

        let clock = TestingClock::new(item_incr);

        Ok(Box::new(clock))
    } else if let Ok(system_clock_config) = clock_config.downcast::<PyCell<SystemClockConfig>>() {
        let _system_clock_config = system_clock_config.borrow();

        let clock = SystemClock::new();

        Ok(Box::new(clock))
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

/// Tumbling windows of fixed duration.
///
/// Args:
///
///     length (datetime.timedelta): Length of window.
///
///     start_at (datetime.datetime): Instant of the first window. You
///         can use this to align all windows to an hour, e.g.
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

pub(crate) fn build_windower(
    py: Python,
    window_config: Py<WindowConfig>,
) -> Result<Box<dyn Windower>, String> {
    let window_config = window_config.as_ref(py);

    if let Ok(tumbling_window_config) = window_config.downcast::<PyCell<TumblingWindowConfig>>() {
        let tumbling_window_config = tumbling_window_config.borrow();

        let length = tumbling_window_config.length.0;
        let start_at = tumbling_window_config
            .start_at
            .map(|t| t.0)
            .unwrap_or(chrono::offset::Local::now().naive_local());

        let windower = TumblingWindower::new(length, start_at);

        Ok(Box::new(windower))
    } else {
        Err(format!(
            "Unknown window_config type: {}",
            window_config.get_type(),
        ))
    }
}

/// Any persistent state that a [`Clock`] might need.
///
/// A clock does not need to use or set all fields here. It should use
/// whatever it needs internally in the class. This won't be modified
/// outside of the current clock impl.
///
/// This is stored in the recovery system.
///
/// Add new stuff here that newer [`Clock`]s might need.
#[derive(Clone, Default, Debug, Serialize, Deserialize)]
pub(crate) struct ClockState {
    /// The time assigned to the last item seen.
    last_item_time: Option<NaiveDateTime>,
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
    fn watermark(&self, state: &mut ClockState, value: &Poll<Option<V>>) -> NaiveDateTime;

    /// Get the time for an item.
    ///
    /// This can mutate the [`ClockState`] if noting that an item has
    /// arrived should advance the clock or something.
    fn time_for(&self, state: &mut ClockState, value: &V) -> NaiveDateTime;
}

/// Simulate system time for tests. Increment "now" after each item.
struct TestingClock {
    item_incr: Duration,
}

impl TestingClock {
    fn new(item_incr: Duration) -> Self {
        Self { item_incr }
    }

    fn last_item_time(&self, state: &mut ClockState) -> NaiveDateTime {
        state
            .last_item_time
            .get_or_insert_with(|| chrono::offset::Local::now().naive_local())
            .clone()
    }
}

impl<V> Clock<V> for TestingClock {
    fn watermark(&self, state: &mut ClockState, item: &Poll<Option<V>>) -> NaiveDateTime {
        match item {
            // If there will be no more values, close out all windows.
            Poll::Ready(None) => chrono::naive::MAX_DATETIME,
            _ => self.last_item_time(state),
        }
    }

    fn time_for(&self, state: &mut ClockState, _item: &V) -> NaiveDateTime {
        let last_item_time = self.last_item_time(state);
        state.last_item_time = Some(last_item_time + self.item_incr);
        last_item_time
    }
}

/// Use the current system time.
struct SystemClock {}

impl SystemClock {
    fn new() -> Self {
        Self {}
    }

    fn now(&self) -> NaiveDateTime {
        chrono::offset::Local::now().naive_local()
    }
}

impl<V> Clock<V> for SystemClock {
    fn watermark(&self, _state: &mut ClockState, item: &Poll<Option<V>>) -> NaiveDateTime {
        match item {
            // If there will be no more values, close out all windows.
            Poll::Ready(None) => chrono::naive::MAX_DATETIME,
            _ => self.now(),
        }
    }

    fn time_for(&self, _state: &mut ClockState, _item: &V) -> NaiveDateTime {
        self.now()
    }
}

#[derive(Clone, PartialEq, Eq, Hash, Serialize, Deserialize, Debug)]
pub(crate) struct WindowId(i64);

/// Any persistent state a [`Windower`] might need.
///
/// This is stored in the recovery system.
///
/// Add new stuff here that newer [`Windower`]s might need.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub(crate) struct WindowerState {
    close_times: HashMap<WindowId, NaiveDateTime>,
}

#[derive(Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
struct Window {
    id: WindowId,
    close: NaiveDateTime,
}

/// An error that can occur when windowing an item.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub(crate) enum InsertError {
    /// The inserted item was late for a window.
    Late(WindowId),
}

/// Defines a kind of time-based windows.
///
/// All relevant data needed to perform the inner methods must be
/// stored in the [`WindowerState`].
pub(crate) trait Windower {
    /// Attempt to insert an incoming value into a window, creating it
    /// if necessary.
    ///
    /// If the current item is "late" for all windows, return
    /// [`WindowError::Late`].
    ///
    /// This must update the [`WindowerState`].
    fn insert(
        &self,
        state: &mut WindowerState,
        watermark: &NaiveDateTime,
        item_time: NaiveDateTime,
    ) -> Vec<Result<WindowId, InsertError>>;

    /// Look at the current watermark, determine which windows are now
    /// closed, return them, and remove them from the state.
    ///
    /// This must update the [`WindowerState`].
    fn drain_closed(&self, state: &mut WindowerState, watermark: &NaiveDateTime) -> Vec<WindowId>;

    /// Return how much system time should pass before the windowing
    /// operator should be activated again, even if there is no
    /// incoming values.
    ///
    /// Use this to signal to Timely's execution when windows will be
    /// closing so we can close them without data.
    fn activate_after(
        &self,
        state: &mut WindowerState,
        watermark: &NaiveDateTime,
    ) -> Option<Duration>;
}

/// Use fixed-length tumbling windows aligned to a start time.
struct TumblingWindower {
    length: Duration,
    start_at: NaiveDateTime,
}

impl TumblingWindower {
    fn new(length: Duration, start_at: NaiveDateTime) -> Self {
        Self { length, start_at }
    }
}

impl Windower for TumblingWindower {
    fn insert(
        &self,
        state: &mut WindowerState,
        watermark: &NaiveDateTime,
        item_time: NaiveDateTime,
    ) -> Vec<Result<WindowId, InsertError>> {
        let since_start_at = item_time - self.start_at;
        let window_count = since_start_at.num_milliseconds() / self.length.num_milliseconds();

        let id = WindowId(window_count);
        let close_at = self.start_at + self.length * (window_count as i32 + 1);

        if &close_at < watermark {
            vec![Err(InsertError::Late(id))]
        } else {
            state.close_times.insert(id.clone(), close_at);
            vec![Ok(id)]
        }
    }

    fn drain_closed(&self, state: &mut WindowerState, watermark: &NaiveDateTime) -> Vec<WindowId> {
        // TODO: Gosh I really want [`HashMap::drain_filter`].
        let mut future_close_times = HashMap::new();
        let mut closed_ids = Vec::new();

        for (id, close_at) in state.close_times.iter() {
            if close_at < &watermark {
                closed_ids.push(id.clone());
            } else {
                future_close_times.insert(id.clone(), close_at.clone());
            }
        }

        state.close_times = future_close_times;
        closed_ids
    }

    fn activate_after(
        &self,
        state: &mut WindowerState,
        watermark: &NaiveDateTime,
    ) -> Option<Duration> {
        let watermark = watermark.clone();
        state
            .close_times
            .values()
            .cloned()
            .map(|t| t - watermark)
            .min()
    }
}
/// The persistent state per-window that
/// [`StatefulWindowUnary::stateful_window_unary`] will need.
///
/// The return types of the logic function in
/// [`StatefulWindowUnary::stateful_window_unary`] determine what
/// values we'll need to store here.
///
/// This is stored in the recovery system.
#[derive(Clone, Serialize, Deserialize)]
struct LogicState<D> {
    window_states: HashMap<WindowId, D>,
}

impl<D> Default for LogicState<D> {
    fn default() -> Self {
        Self {
            window_states: HashMap::new(),
        }
    }
}

/// Possible errors emitted downstream from windowing operators.
#[derive(Clone, Serialize, Deserialize)]
pub(crate) enum WindowError<V> {
    /// This item was late for a window.
    Late(V),
}

/// Extension trait for [`Stream`].
pub(crate) trait StatefulWindowUnary<S: Scope, V: ExchangeData> {
    /// Create a new generic windowing operator.
    ///
    /// This is the core Timely operator that all Bytewax windowing
    /// operators are implemented in terms of. We do this so we only
    /// have to implement the detailed windowing code once.
    ///
    /// # Logic
    ///
    /// `logic` takes three arguments and is called on each incoming
    /// item and when the window closes:
    ///
    /// - The relevant key.
    ///
    /// - The last state for that key and window. This might be
    /// [`None`] if there wasn't any.
    ///
    /// - An incoming value or [`None`] if the window for this key has
    /// just closed. Think of this as the same protocol as
    /// [`std::iter::Iterator::next`].
    ///
    /// `logic` must return a 2-tuple of:
    ///
    /// - The updated state for the key, or [`None`] if the state
    /// should be discarded.
    ///
    /// - Values to be emitted downstream. You probably only want to
    /// emit values when the incoming value is [`None`], signaling the
    /// window has closed, but you might want something more
    /// generic. They will be automatically paired with the key in the
    /// output stream.
    ///
    /// # Output
    ///
    /// The output will be a stream of `(key, value)` output
    /// 2-tuples. Values emitted by `logic` will automatically be
    /// paired with the key in the output stream.
    fn stateful_window_unary<
        R: Data,                                                            // Output item type
        D: ExchangeData,           // Per-key per-window state
        I: IntoIterator<Item = R>, // Iterator of output items
        L: Fn(&StateKey, Option<D>, Option<V>) -> (Option<D>, I) + 'static, // Logic
        H: Fn(&StateKey) -> u64 + 'static, // Hash function of key to worker
    >(
        &self,
        step_id: StepId,
        clock: Box<dyn Clock<V>>,
        windower: Box<dyn Windower>,
        logic: L,
        hasher: H,
        state_loading_stream: &Stream<S, StateBackup<S::Timestamp, Vec<u8>>>,
        state_backup_streams: &mut Vec<Stream<S, StateBackup<S::Timestamp, Vec<u8>>>>,
    ) -> Stream<S, (StateKey, Result<R, WindowError<V>>)>;
}

impl<S, V: ExchangeData + Debug> StatefulWindowUnary<S, V> for Stream<S, (StateKey, V)>
where
    S: Scope<Timestamp = u64>,
{
    fn stateful_window_unary<
        R: Data,                                                            // Output item type
        D: ExchangeData,           // Per-key per-window state
        I: IntoIterator<Item = R>, // Iterator of output items
        L: Fn(&StateKey, Option<D>, Option<V>) -> (Option<D>, I) + 'static, // Logic
        H: Fn(&StateKey) -> u64 + 'static, // Hash function of key to worker
    >(
        &self,
        step_id: StepId,
        clock: Box<dyn Clock<V>>,
        windower: Box<dyn Windower>,
        logic: L,
        hasher: H,
        state_loading_stream: &Stream<S, StateBackup<S::Timestamp, Vec<u8>>>,
        state_backup_streams: &mut Vec<Stream<S, StateBackup<S::Timestamp, Vec<u8>>>>,
    ) -> Stream<S, (StateKey, Result<R, WindowError<V>>)> {
        self.stateful_unary(
            step_id,
            move |key, state: Option<(ClockState, WindowerState, LogicState<D>)>, value| {
                let mut state = state.unwrap_or_default();
                let (clock_state, windower_state, logic_state) = &mut state;

                let mut output = Vec::new();

                let watermark = clock.watermark(clock_state, &value);

                if let Poll::Ready(Some(value)) = value {
                    let item_time = clock.time_for(clock_state, &value);

                    for window_result in windower.insert(windower_state, &watermark, item_time) {
                        let value = value.clone();
                        match window_result {
                            Err(InsertError::Late(_id)) => {
                                output.push(Err(WindowError::Late(value)));
                            }
                            Ok(id) => {
                                let window_state = logic_state.window_states.remove(&id);

                                let (updated_window_state, window_output) =
                                    logic(&key, window_state, Some(value));

                                match updated_window_state {
                                    Some(state) => logic_state.window_states.insert(id, state),
                                    None => logic_state.window_states.remove(&id),
                                };

                                output.extend(window_output.into_iter().map(Ok));
                            }
                        }
                    }
                }

                for id in windower.drain_closed(windower_state, &watermark) {
                    let window_state = logic_state.window_states.remove(&id);

                    let (updated_window_state, window_output) = logic(&key, window_state, None);

                    if let Some(_) = updated_window_state {
                        panic!("Stateful window logic did not return `None` on window close; state would be lost");
                    }
                    logic_state.window_states.remove(&id);

                    output.extend(window_output.into_iter().map(Ok));
                }

                let activate_after = windower
                    .activate_after(windower_state, &watermark)
                    .map(|d| d.to_std().unwrap_or(std::time::Duration::ZERO));

                StatefulUnaryLogicReturn { updated_state: Some(state), output, activate_after }
            },
            hasher,
            state_loading_stream,
            state_backup_streams,
        )
    }
}

/// Shim function for [`crate::dataflow::Dataflow::reduce_window`].
pub(crate) fn reduce_window(
    reducer: &TdPyCallable,
    key: &StateKey,
    acc: Option<TdPyAny>,
    next_value: Option<TdPyAny>,
) -> (Option<TdPyAny>, impl IntoIterator<Item = TdPyAny>) {
    match next_value {
        Some(value) => {
            Python::with_gil(|py| {
                let updated_acc: TdPyAny = match acc {
                    // If there's no previous state for this key, use
                    // the current value.
                    None => value,
                    Some(acc) => {
                        let updated_acc = with_traceback!(
                            py,
                            reducer.call1(py, (acc.clone_ref(py), value.clone_ref(py)))
                        )
                        .into();
                        debug!("reduce_window for key={key:?}: reducer={reducer:?}(acc={acc:?}, value={value:?}) -> updated_acc={updated_acc:?}",);

                        updated_acc
                    }
                };

                (Some(updated_acc), None)
            })
        }
        None => (None, acc),
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
