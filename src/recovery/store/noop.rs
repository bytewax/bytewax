//! State and progress stores which do nothing.

use crate::recovery::model::*;
use std::fmt::Debug;

/// Writes are dropped and reads are the same as an empty store.
pub(crate) struct NoOpStore;

impl NoOpStore {
    pub fn new() -> Self {
        NoOpStore {}
    }
}

impl<K, V> KWriter<K, V> for NoOpStore
where
    K: Debug,
    V: Debug,
{
    fn write(&mut self, kchange: KChange<K, V>) {
        tracing::trace!("No-op change {kchange:?}");
    }
}

impl<K, V> KReader<K, V> for NoOpStore {
    fn read(&mut self) -> Option<KChange<K, V>> {
        None
    }
}

impl<T> StateWriter<T> for NoOpStore where T: Debug {}
impl<T> StateReader<T> for NoOpStore {}
impl<T> ProgressWriter<T> for NoOpStore where T: Debug {}
impl<T> ProgressReader<T> for NoOpStore {}
