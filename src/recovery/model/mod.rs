//! The data model for recovery.
//!
//! [`change`] is the base, then [`state`] and [`progress`] are built
//! on top of that.

pub(crate) mod change;
pub(crate) mod progress;
pub(crate) mod state;

// Re-export so you can get the whole model at once.

pub(crate) use change::*;
pub(crate) use progress::*;
pub(crate) use state::*;
