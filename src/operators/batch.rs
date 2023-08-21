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
    last_activation: Instant,
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
                last_activation: Instant::now(),
            }
        }
    }
}

impl StatefulLogic<TdPyAny, TdPyAny, Option<TdPyAny>> for BatchLogic {
    fn on_awake(&mut self, next_value: Poll<Option<TdPyAny>>) -> Option<TdPyAny> {
        if let Poll::Ready(Some(value)) = next_value {
            self.acc.push(value);
            if self.acc.len() >= self.size || self.last_activation.elapsed() >= self.timeout {
                Python::with_gil(|py| {
                    Some(
                        self.acc
                            .drain(..)
                            .collect::<Vec<TdPyAny>>()
                            .to_object(py)
                            .into(),
                    )
                })
            } else {
                None
            }
        } else if self.last_activation.elapsed() >= self.timeout {
            Python::with_gil(|py| {
                Some(
                    self.acc
                        .drain(..)
                        .collect::<Vec<TdPyAny>>()
                        .to_object(py)
                        .into(),
                )
            })
        } else {
            None
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
        None
    }

    fn snapshot(&self) -> TdPyAny {
        Python::with_gil(|py| self.acc.to_object(py).into())
    }
}
