use std::{
    task::Poll,
    time::{Duration, Instant},
};

use pyo3::{Python, ToPyObject};

use crate::{pyo3_extensions::TdPyAny, unwrap_any};

use super::stateful_unary::{LogicFate, StatefulLogic};

pub(crate) struct BatchLogic {
    size: usize,
    timeout: Duration,
    last_drain: Instant,
    acc: Vec<TdPyAny>,
}

impl BatchLogic {
    pub(crate) fn builder(size: usize, timeout: Duration) -> impl Fn(Option<TdPyAny>) -> Self {
        move |resume_snapshot| {
            let acc = resume_snapshot
                .and_then(|state| {
                    let state: Option<Vec<TdPyAny>> =
                        unwrap_any!(Python::with_gil(|py| state.extract(py)));
                    state
                })
                .unwrap_or_default();
            Self {
                size,
                acc,
                timeout,
                last_drain: Instant::now(),
            }
        }
    }

    /// Drain self.acc, convert it to a TdPyAny and return it
    fn drain_acc(&mut self) -> TdPyAny {
        self.last_drain = Instant::now();
        Python::with_gil(|py| {
            self.acc
                .drain(..)
                .collect::<Vec<TdPyAny>>()
                .to_object(py)
                .into()
        })
    }
}

impl StatefulLogic<TdPyAny, TdPyAny, Option<TdPyAny>> for BatchLogic {
    fn on_awake(&mut self, next_value: Poll<Option<TdPyAny>>) -> Option<TdPyAny> {
        let timeout_expired = self.last_drain.elapsed() >= self.timeout;
        match next_value {
            Poll::Ready(Some(value)) => {
                self.acc.push(value);
                if self.acc.len() >= self.size || timeout_expired {
                    Some(self.drain_acc())
                } else {
                    None
                }
            }
            // Emit remaining items if the input reached EOF
            Poll::Ready(None) => Some(self.drain_acc()),
            // Emit items if timeout has expired, even if
            // no item was received during this awake
            _ if timeout_expired => Some(self.drain_acc()),
            _ => None,
        }
    }

    fn fate(&self) -> super::stateful_unary::LogicFate {
        if self.acc.is_empty() {
            LogicFate::Discard
        } else {
            LogicFate::Retain
        }
    }

    fn next_awake(&self) -> Option<chrono::DateTime<chrono::Utc>> {
        // Request an awake when the timeout expires
        Some(
            chrono::Utc::now()
                + chrono::Duration::from_std(self.timeout + self.last_drain.elapsed()).unwrap(),
        )
    }

    fn snapshot(&self) -> TdPyAny {
        Python::with_gil(|py| self.acc.to_object(py).into())
    }
}
