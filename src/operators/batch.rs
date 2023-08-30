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
    start: Instant,
    acc: Vec<TdPyAny>,
}

impl BatchLogic {
    pub(crate) fn builder(size: usize, timeout: Duration) -> impl Fn(Option<TdPyAny>) -> Self {
        move |resume_snapshot| {
            let acc = resume_snapshot
                .and_then(|state| -> Option<Vec<TdPyAny>> {
                    unwrap_any!(Python::with_gil(|py| state.extract(py)))
                })
                .unwrap_or_else(Vec::new);
            Self {
                size,
                acc,
                timeout,
                start: Instant::now(),
            }
        }
    }
}

impl StatefulLogic<TdPyAny, TdPyAny, Option<TdPyAny>> for BatchLogic {
    fn on_awake(&mut self, next_value: Poll<Option<TdPyAny>>) -> Option<TdPyAny> {
        let eof = matches!(next_value, Poll::Ready(None));
        if let Poll::Ready(Some(value)) = next_value {
            self.acc.push(value);
        }

        let timeout_expired = self.start.elapsed() >= self.timeout;
        let reached_size = self.acc.len() >= self.size;

        if (reached_size || timeout_expired || eof) && !self.acc.is_empty() {
            Some(Python::with_gil(|py| {
                std::mem::take(&mut self.acc).to_object(py).into()
            }))
        } else {
            None
        }
    }

    fn fate(&self) -> LogicFate {
        if self.acc.is_empty() {
            LogicFate::Discard
        } else {
            LogicFate::Retain
        }
    }

    fn next_awake(&self) -> Option<chrono::DateTime<chrono::Utc>> {
        // Request an awake when the timeout expires
        let remaining_time = self.timeout.saturating_sub(self.start.elapsed());
        Some(chrono::Utc::now() + chrono::Duration::from_std(remaining_time).unwrap())
    }

    fn snapshot(&self) -> TdPyAny {
        Python::with_gil(|py| self.acc.to_object(py).into())
    }
}
