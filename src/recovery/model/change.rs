//! Base data model and traits for a "change data capture"-like
//! system.
//!
//! The idea here is to model a key-value DB **store**. Streams of
//! changes can be written and read to / from the store. The store
//! itself does _not_ model time, changes are applied in-order.
//!
//! Reading and writing are not perfect mirror images of each other,
//! since the store does not have to retain the changelog
//! itself. Instead, the entire set of read changes would result in
//! the same store state if written to an empty store.
//!
//! For example: Writing `KChange("a", Change::Upsert(1))`,
//! `KChange("b", Change::Upsert(2))`, `KChange("a",
//! Change::Upsert(2))` could result in reading `KChange("a",
//! Change::Upsert(2))`, `KChange("b", Change::Upsert(2))`, since that
//! middle change is not necessary to re-create the state of the
//! store.

use serde::Deserialize;
use serde::Serialize;
use std::cell::RefCell;
use std::collections::{BTreeMap, HashMap};
use std::hash::Hash;
use std::ops::DerefMut;
use std::rc::Rc;

/// Represents a change to a value.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) enum Change<V> {
    Upsert(V),
    Discard,
}

/// Uses to store a "type of change", but not the change data itself.
pub(crate) type ChangeType = Change<()>;

impl<V> Change<V> {
    pub(crate) fn map<U>(self, f: impl Fn(V) -> U) -> Change<U> {
        match self {
            Self::Upsert(v) => Change::Upsert(f(v)),
            Self::Discard => Change::Discard,
        }
    }

    pub(crate) fn typ(&self) -> ChangeType {
        match self {
            Self::Upsert(..) => Change::Upsert(()),
            Self::Discard => Change::Discard,
        }
    }
}

/// A "keyed change" representing changing a value for a key in a
/// store.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct KChange<K, V>(pub(crate) K, pub(crate) Change<V>);

/// Apply keyed changes to a store.
pub(crate) trait KWriter<K, V> {
    fn write(&mut self, kchange: KChange<K, V>);

    fn write_many(&mut self, batch: Vec<KChange<K, V>>) {
        for change in batch {
            self.write(change);
        }
    }
}

/// Return a stream of keyed changes that, if written, will result in
/// the same store state.
pub(crate) trait KReader<K, V> {
    fn read(&mut self) -> Option<KChange<K, V>>;

    fn read_many(&mut self) -> Option<Vec<KChange<K, V>>> {
        self.read().map(|change| vec![change])
    }
}

impl<P, K, V> KWriter<K, V> for Box<P>
where
    P: KWriter<K, V> + ?Sized,
{
    fn write(&mut self, kchange: KChange<K, V>) {
        self.deref_mut().write(kchange)
    }

    fn write_many(&mut self, batch: Vec<KChange<K, V>>) {
        self.deref_mut().write_many(batch)
    }
}

impl<P, K, V> KWriter<K, V> for Rc<RefCell<P>>
where
    P: KWriter<K, V> + ?Sized,
{
    fn write(&mut self, kchange: KChange<K, V>) {
        self.borrow_mut().write(kchange)
    }

    fn write_many(&mut self, batch: Vec<KChange<K, V>>) {
        self.borrow_mut().write_many(batch)
    }
}

impl<K, V> KWriter<K, V> for HashMap<K, V>
where
    K: Hash + Eq,
{
    fn write(&mut self, kchange: KChange<K, V>) {
        let KChange(key, change) = kchange;
        match change {
            Change::Upsert(value) => self.insert(key, value),
            Change::Discard => self.remove(&key),
        };
    }
}

impl<K, V> KWriter<K, V> for BTreeMap<K, V>
where
    K: Ord,
{
    fn write(&mut self, kchange: KChange<K, V>) {
        let KChange(key, change) = kchange;
        match change {
            Change::Upsert(value) => self.insert(key, value),
            Change::Discard => self.remove(&key),
        };
    }
}

impl<P, K, V> KReader<K, V> for Box<P>
where
    P: KReader<K, V> + ?Sized,
{
    fn read(&mut self) -> Option<KChange<K, V>> {
        self.deref_mut().read()
    }

    fn read_many(&mut self) -> Option<Vec<KChange<K, V>>> {
        self.deref_mut().read_many()
    }
}
