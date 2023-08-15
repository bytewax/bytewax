use chrono::{DateTime, Duration, Utc};
use pyo3::{prelude::*, types::PyDict};

use crate::{
    add_pymethods,
    pyo3_extensions::TdPyAny,
    unwrap_any,
    window::{session_window::session::Session, WindowConfig},
};

use super::{InsertError, WindowBuilder, WindowKey, Windower};

/// Session windowing with a fixed inactivity gap.
/// Each time a new item is received, it is added to the latest
/// window if the time since the latest event is < gap.
/// Otherwise a new window is created that starts at current clock's time.
///
///  Args:
///    gap (datetime.timedelta):
///      Gap of inactivity before considering a session closed. The
///      gap should not be negative.
///
/// Returns:
///   Config object. Pass this as the `window_config` parameter to
///   your windowing operator.
#[pyclass(module="bytewax.window", extends=WindowConfig)]
#[derive(Clone)]
pub(crate) struct SessionWindow {
    #[pyo3(get)]
    pub(crate) gap: Duration,
}

impl WindowBuilder for SessionWindow {
    fn build(&self, _py: pyo3::Python) -> PyResult<super::Builder> {
        Ok(Box::new(SessionWindower::builder(self.gap)))
    }
}

add_pymethods!(
    SessionWindow,
    parent: WindowConfig,
    signature: (gap),
    args {
        gap: Duration => Duration::zero()
    }
);

pub(crate) struct SessionWindower {
    /// How long to wait before cosidering
    /// the session closed.
    gap: Duration,

    /// State.
    /// A list of sessions that should always be ordered
    /// by session.start_time.
    sessions: Vec<session::Session>,

    /// Keep track of the latest window key we issued,
    /// so we can generate the next avoiding collisions.
    max_key: WindowKey,
}

impl SessionWindower {
    pub(crate) fn builder(gap: Duration) -> impl Fn(Option<TdPyAny>) -> Box<dyn Windower> {
        move |resume_snapshot| {
            assert!(
                gap.num_milliseconds() > 0,
                "gap in WindowSession cannot be negative"
            );
            let (sessions, max_key) = resume_snapshot
                .map(|state| -> (Vec<Session>, WindowKey) {
                    unwrap_any!(Python::with_gil(
                        |py| -> PyResult<(Vec<Session>, WindowKey)> {
                            let state = state.as_ref(py);
                            let session_snaps: Vec<TdPyAny> =
                                state.get_item("sessions")?.extract()?;
                            let max_key = state.get_item("max_key")?.extract()?;
                            let sessions = session_snaps
                                .into_iter()
                                .map(|ss| Session::from_snap(py, ss))
                                .collect::<PyResult<Vec<Session>>>()?;
                            Ok((sessions, max_key))
                        }
                    ))
                })
                .unwrap_or_else(|| (vec![], WindowKey(0)));
            Box::new(Self {
                gap,
                sessions,
                max_key,
            })
        }
    }

    fn add_session(&mut self, item_time: &DateTime<Utc>) {
        let key = self.next_key();
        let session = session::Session::new(key, *item_time);
        self.sessions.push(session);
        // Don't forget to sort, since we might have added a session
        // with a start_time that's before the latest one.
        self.sessions.sort_unstable();
    }

    fn merge_sessions(&mut self) {
        // If a session's start_time is inside the previous session,
        // merge them into the earlier one.
        // This assumes sessions are ordered by start_time.
        self.sessions = self
            .sessions
            // Take ownership of each session, so that
            // we can consume them for the merge.
            .drain(..)
            .fold(vec![], |mut sessions, session| {
                // If this is the first iteration, put the session
                // in the accumulator and go to next iteration.
                if sessions.is_empty() {
                    sessions.push(session);
                    return sessions;
                }

                // Now get the previous session...
                let prev_session = sessions.pop().unwrap();

                // If prev_session contains session.start(),
                // we merge them, otherwise we put both back as they are
                if prev_session.contains(session.start(), &self.gap) {
                    sessions.push(prev_session.merge_with(session));
                } else {
                    sessions.extend_from_slice(&[prev_session, session]);
                }
                sessions
            });
    }

    fn next_key(&mut self) -> WindowKey {
        self.max_key.0 += 1;
        self.max_key
    }
}

#[test]
fn test_merge_sessions() {
    let mut windower = SessionWindower {
        gap: Duration::seconds(5),
        sessions: vec![],
        max_key: WindowKey(0),
    };
    // First add a session at time 0
    let start_time = Utc::now();
    windower.add_session(&start_time);
    // Then add a session at time +6
    windower.add_session(&(start_time + Duration::seconds(6)));
    // Now add a session at time +1, which
    // should be merged with the first one.
    windower.add_session(&(start_time + Duration::seconds(1)));
    assert_eq!(windower.sessions.len(), 3);
    windower.merge_sessions();
    // Now we should only have 2 sessions.
    assert_eq!(windower.sessions.len(), 2);
    // The first one should have been merged with the one we added last.
    assert_eq!(
        windower.sessions[0].current_close_time(&Duration::zero()),
        start_time + Duration::seconds(1)
    );
}

impl Windower for SessionWindower {
    fn insert(
        &mut self,
        watermark: &DateTime<Utc>,
        item_time: &DateTime<Utc>,
    ) -> Vec<Result<WindowKey, InsertError>> {
        // If the item time is before the watermark we mark it late
        // before creating a new session.
        if *item_time < *watermark {
            // We need to pass a window_key to the "Late" error, but in
            // this case we can't really say what session the item should
            // have belong to, because it's presence might have changed
            // sessions that we already dropped.
            // So I'm just going to assign `self.max_key` here as a placeholder.
            return vec![Err(InsertError::Late(self.max_key))];
        }

        // Following Flink's behavior, we just create a new session at every event
        self.add_session(item_time);
        // Then merge all the sessions that can be merged.
        self.merge_sessions();

        // And finally try to insert the item in every session.
        // Since each item should only belong to one session,
        // we just try to find the first one that matches.
        self.sessions
            .iter_mut()
            .find_map(|session| {
                session
                    .try_insert(item_time, &self.gap)
                    .then(|| vec![Ok(session.key())])
            })
            // Here we panic if we don't find a session for an item,
            // since the only case where a session might not exist
            // is if the item was late (from before the watermark),
            // in which case we should have returned earlier.
            .expect("SessionWindower is not generating consistent sessions")
    }

    fn drain_closed(&mut self, watermark: &DateTime<Utc>) -> Vec<WindowKey> {
        // Vec::drain_filter would be nice here.
        let mut keys = vec![];
        self.sessions.retain(|s| {
            let is_open = s.is_open_at(watermark, &self.gap);
            if !is_open {
                keys.push(s.key());
            }
            is_open
        });
        keys
    }

    fn is_empty(&self) -> bool {
        self.sessions.is_empty()
    }

    fn next_close(&self) -> Option<DateTime<Utc>> {
        self.sessions
            .last()
            .map(|s| s.current_close_time(&self.gap))
    }

    fn snapshot(&self) -> TdPyAny {
        unwrap_any!(Python::with_gil(|py| -> PyResult<_> {
            let state = PyDict::new(py);
            let sessions = self
                .sessions
                .iter()
                .map(|s| s.snapshot(py))
                .collect::<PyResult<Vec<TdPyAny>>>()?;
            state.set_item("sessions", sessions.into_py(py))?;
            state.set_item("max_key", self.max_key.into_py(py))?;
            let state: PyObject = state.into_py(py);
            Ok(state.into())
        }))
    }
}

#[test]
fn test_insert() {
    let mut windower = SessionWindower {
        gap: Duration::seconds(5),
        sessions: vec![],
        max_key: WindowKey(0),
    };
    // First add a session at time 0
    let start_time = Utc::now();
    let watermark = start_time;

    // This should just create a session and return ok for the item.
    let res = windower.insert(&watermark, &start_time);
    assert_eq!(res.len(), 1);
    assert!(res[0].is_ok());
    let first_window = res[0].unwrap();

    // Now we try to insert a late item
    let res = windower.insert(&watermark, &(watermark - Duration::seconds(1)));
    assert_eq!(res.len(), 1);
    assert!(res[0].is_err());

    // Now we insert an item that is out of the previous session
    let res = windower.insert(&watermark, &(start_time + Duration::seconds(6)));
    assert_eq!(res.len(), 1);
    assert!(res[0].is_ok());
    let second_window = res[0].unwrap();
    // And it should have created a new window
    assert_ne!(first_window, second_window);

    // Finally insert an item that should be included in the latest window
    let res = windower.insert(&watermark, &(start_time + Duration::seconds(7)));
    assert_eq!(res.len(), 1);
    assert!(res[0].is_ok());
    let third_window = res[0].unwrap();
    assert_eq!(second_window, third_window);
}

/// The Session struct is inside it's own module to avoid having the ability
/// to access fields directly from outside, which might brake some of the assumptions we make:
/// - self.key can never change
/// - self.start_time can never change
/// - self.latest_event_time has to be valid, which is done through Session::try_insert
mod session {
    use chrono::{DateTime, Duration, Utc};
    use pyo3::{prelude::*, types::PyDict};

    use crate::{pyo3_extensions::TdPyAny, window::WindowKey};

    /// This struct represents a Session.
    #[derive(Clone, Debug, Eq)]
    pub(crate) struct Session {
        key: WindowKey,
        start: DateTime<Utc>,
        latest_event_time: DateTime<Utc>,
    }

    impl Session {
        pub fn new(key: WindowKey, start: DateTime<Utc>) -> Self {
            Self {
                key,
                start,
                latest_event_time: start,
            }
        }

        pub fn from_snap(py: Python, state: TdPyAny) -> PyResult<Self> {
            let state = state.as_ref(py);
            let key = state.get_item("key")?.extract()?;
            let start = state.get_item("start")?.extract()?;
            let latest_event_time = state.get_item("latest_event_time")?.extract()?;

            Ok(Self {
                key,
                start,
                latest_event_time,
            })
        }

        pub fn start(&self) -> &DateTime<Utc> {
            &self.start
        }

        pub fn key(&self) -> WindowKey {
            self.key
        }

        /// Returns the current close time. This changes
        /// when a more recent event is added to the session.
        pub fn current_close_time(&self, gap: &Duration) -> DateTime<Utc> {
            self.latest_event_time + *gap
        }

        /// Check if this session is currently open at the given time.
        pub fn is_open_at(&self, time: &DateTime<Utc>, gap: &Duration) -> bool {
            self.current_close_time(gap) > *time
        }

        /// Given an item_time, check if the item belongs to this session.
        pub fn contains(&self, item_time: &DateTime<Utc>, gap: &Duration) -> bool {
            *item_time >= self.start && self.is_open_at(item_time, gap)
        }

        /// Try to insert an item in this session:
        /// check if the item is in the current session, and
        /// update self.latest_event_time if needed.
        /// Returns true if the item was added and false otherwise.
        pub fn try_insert(&mut self, item_time: &DateTime<Utc>, gap: &Duration) -> bool {
            if self.contains(item_time, gap) {
                self.latest_event_time = self.latest_event_time.max(*item_time);
                true
            } else {
                false
            }
        }

        /// Merge this session with another, consuming both
        /// and returning the new merged session.
        /// Keeps this session's key.
        pub fn merge_with(self, other: Self) -> Self {
            // The merged session starts at the earliest of the two
            // and ends at the latest of the two.
            Self {
                key: self.key,
                start: self.start.min(other.start),
                latest_event_time: self.latest_event_time.max(other.latest_event_time),
            }
        }

        pub fn snapshot(&self, py: Python) -> PyResult<TdPyAny> {
            let state = PyDict::new(py);
            state.set_item("key", self.key.into_py(py))?;
            state.set_item("start", self.start.into_py(py))?;
            state.set_item("latest_event_time", self.latest_event_time.into_py(py))?;
            let state: PyObject = state.into();
            Ok(state.into())
        }
    }

    /// Implement traits to allow comparing and ordering sessions.
    /// This is not forced by the type system, but we assume keys are unique,
    /// so we can compare for that only.
    impl PartialEq for Session {
        fn eq(&self, other: &Self) -> bool {
            self.key == other.key
        }
    }

    /// We order Sessions based on their start time
    impl PartialOrd for Session {
        fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
            self.start.partial_cmp(&other.start)
        }
    }

    impl Ord for Session {
        fn cmp(&self, other: &Self) -> std::cmp::Ordering {
            self.start.cmp(&other.start)
        }
    }
}
