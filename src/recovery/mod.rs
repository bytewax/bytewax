//! Internal code for implementing recovery.
//!
//! For a user-centric version of recovery, read the
//! `bytewax.recovery` Python module docstring. Read that first.
//!
//! Because the recovery system is complex and requires coordination
//! at many points in execution, not all code for recovery lives
//! here. There are some custom operators and execution and dataflow
//! building code which all help implement the recovery logic. An
//! overview of it is given here, though.
//!
//! Stateful Operators
//! ------------------
//!
//! See [`crate::operators::stateful_unary`] for how normal stateful
//! operators are built, and [`crate::epochs`] for the input source
//! system.
//!
//! Architecture
//! ------------
//!
//! State recovery is based around snapshotting and backing up the
//! epoch-stamped state of each stateful operator at the end of each
//! epoch. This results in having the history of state across epochs,
//! which allows us to recover to a synchronized point in time (the
//! beginning of an epoch) across workers.
//!
//! We also snapshot the **finalized epoch** of each worker as the
//! dataflow executes. The finalized epoch is the newest epoch on a
//! given worker for which output or state backup is no longer
//! pending.
//!
//! See [`model`] for the specifics of what is stored for [`model::state`] and
//! [`model::progress`].
//!
//! See [`store`] for how to actually make a new storage backend.
//!
//! See [`dataflows`] for the actual Timely implementation of backup
//! and resume.

pub(crate) mod dataflows;
pub(crate) mod model;
pub(crate) mod operators;
pub(crate) mod python;
pub(crate) mod store;
