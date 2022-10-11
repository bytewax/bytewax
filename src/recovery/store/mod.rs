//! Implementations of state and progress stores.
//!
//! A store is modeled as a K-V table. See [`super::model`] for the
//! "schema" of these tables.
//!
//! There are 4 traits you'll have to implement to make a new recovery
//! store: [`super::model::state::StateWriter`],
//! [`super::model::state::StateReader`],
//! [`super::model::state::ProgressWriter`],
//! [`super::model::state::ProgressReader`].

pub(crate) mod in_mem;
pub(crate) mod kafka;
pub(crate) mod noop;
pub(crate) mod sqlite;
