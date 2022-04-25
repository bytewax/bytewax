use pyo3::exceptions::PyValueError;
use retry::delay::Fixed;
use retry::retry;
use send_wrapper::SendWrapper;
use sqlx::query;
use sqlx::sqlite::SqliteConnectOptions;
use sqlx::sqlite::SqlitePoolOptions;
use sqlx::sqlite::SqliteRow;
use sqlx::Pool;
use sqlx::Row;
use sqlx::Sqlite;
use std::fmt::Debug;
use std::rc::Rc;
use std::time::Duration;
use tokio::runtime::Runtime;

use crate::pyo3_extensions::TdPyAny;
use log::debug;
use pyo3::prelude::*;

/// Base class for a recovery config.
///
/// This describes how to connect to a recovery store.
///
/// Use a specific subclass of this that matches the kind of storage
/// system you are going to use. See the subclasses in this module.
#[pyclass(module = "bytewax.recovery", subclass)]
#[pyo3(text_signature = "()")]
pub(crate) struct RecoveryConfig;

/// Use [SQLite](https://sqlite.org/index.html) as recovery storage.
///
/// Because it's not designed for high-concurrency, SQLite should only
/// be used for machine-local testing of dataflows. Multiple workers
/// will _not_ result in corrupted data, but there will be reduced
/// performance due to contention for the DB lock.
///
/// The SQLite DB does not need any preparation. A `state` table will
/// automatically be created and queried.
///
/// Only one dataflow can be persisted per SQLite DB. Use a new file
/// for a new dataflow.
///
/// Args:
///
///     db_file_path: Local path to the DB file in Sqlite3
///         format. E.g. `./state.sqlite3`
///
/// Returns:
///
///     Config object. Pass this as the `recovery_config` argument to
///     your execution entry point.
#[pyclass(module="bytewax.recovery", extends=RecoveryConfig)]
#[pyo3(text_signature = "(db_file_path)")]
pub(crate) struct SqliteRecoveryConfig {
    #[pyo3(get)]
    pub(crate) db_file_path: String,
}

#[pymethods]
impl SqliteRecoveryConfig {
    #[new]
    fn new(db_file_path: String) -> (Self, RecoveryConfig) {
        (Self { db_file_path }, RecoveryConfig {})
    }

    /// Pickle as a tuple.
    fn __getstate__(&self) -> (&str, &str) {
        ("SqliteRecoveryConfig", &self.db_file_path)
    }

    /// Egregious hack because pickling assumes the type has "empty"
    /// mutable objects.
    ///
    /// Pickle always calls `__new__(*__getnewargs__())` but notice we
    /// don't have access to the pickled `db_file_path` yet, so we
    /// have to pass in some dummy string value that will be
    /// overwritten by `__setstate__()` shortly.
    fn __getnewargs__(&self) -> (&str,) {
        ("UNINIT_PICKLED_STRING",)
    }

    /// Unpickle from tuple of arguments.
    fn __setstate__(&mut self, state: &PyAny) -> PyResult<()> {
        if let Ok(("SqliteRecoveryConfig", db_file_path)) = state.extract() {
            self.db_file_path = db_file_path;
            Ok(())
        } else {
            Err(PyValueError::new_err(format!(
                "bad pickle contents for SqliteRecoveryConfig: {state:?}"
            )))
        }
    }
}

/// Rust-side trait which represents actions the dataflow execution
/// will need to delegate to a recovery store.
///
/// We'll implement this for each kind of recovery store we need to
/// talk to. This is not exposed Python-side.
pub(crate) trait RecoveryStore {
    // TODO: I wanted to make a trait RecoveryStore statically typed, but I
    // think that'll require GAT / generic associated types.

    /// Build recovery interface for this step.
    fn for_step(&self, step_id: &str) -> Box<dyn StepRecovery<u64, TdPyAny, TdPyAny>>;

    // TODO: We can implement state compaction / GC once we know
    // epochs are output by every capture() operator.
    //fn compact(&self, done_epoch: u64);
}

/// Per-operator interface for state recovery.
///
/// Each [`RecoveryStore`] will need to implement this with the
/// specific queries or inserts needed.
pub(crate) trait StepRecovery<T, K, D> {
    fn recover_last(&self, current_epoch: &T, key: &K) -> Option<D>;

    fn save_complete(&self, completed_epoch: &T, key: &K, state: &Option<D>) -> ();
}

/// A recovery store which does nothing.
///
/// Saves are dropped and all recoveries result in "not found".
pub(crate) struct NoOpRecoveryStore;

impl NoOpRecoveryStore {
    pub(crate) fn new() -> Self {
        NoOpRecoveryStore {}
    }
}

impl RecoveryStore for NoOpRecoveryStore {
    fn for_step(&self, step_id: &str) -> Box<dyn StepRecovery<u64, TdPyAny, TdPyAny>> {
        Box::new(NoOpStepRecovery {
            step_id: step_id.to_string(),
        })
    }
}

struct NoOpStepRecovery {
    step_id: String,
}

impl<T: Debug, K: Debug, D: Debug> StepRecovery<T, K, D> for NoOpStepRecovery {
    fn recover_last(&self, current_epoch: &T, key: &K) -> Option<D> {
        debug!(
            "noop recovery queried step_id={} key={key:?}:state=None@epoch={current_epoch:?}",
            self.step_id
        );
        None
    }
    fn save_complete(&self, completed_epoch: &T, key: &K, state: &Option<D>) -> () {
        debug!("noop recovery saved step_id={} key={key:?}:state={state:?}@epoch={completed_epoch:?}", self.step_id);
        ()
    }
}

pub(crate) struct SqliteRecoveryStore {
    rt: SendWrapper<Rc<Runtime>>,
    pool: Pool<Sqlite>,
}

impl SqliteRecoveryStore {
    pub(crate) fn new(db_file_path: &str) -> Self {
        let rt = SendWrapper::new(Rc::new(
            tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap(),
        ));

        // For some reason the busy_timeout setting doesn't affect the
        // initial connection. So manually retry here.
        let pool = retry(Fixed::from_millis(100), || {
            let options = SqliteConnectOptions::new()
                .filename(db_file_path)
                .create_if_missing(true)
                .busy_timeout(Duration::from_secs(5))
                .journal_mode(sqlx::sqlite::SqliteJournalMode::Wal)
                .locking_mode(sqlx::sqlite::SqliteLockingMode::Normal)
                .synchronous(sqlx::sqlite::SqliteSynchronous::Normal)
                .thread_name(|i| format!("sqlx-sqlite-{i}"));
            let future = SqlitePoolOptions::new().connect_with(options);
            rt.block_on(future)
        })
        .unwrap();
        debug!("Opened Sqlite connection pool to {db_file_path}");

        SqliteRecoveryStore { rt, pool }
    }
}

impl RecoveryStore for SqliteRecoveryStore {
    fn for_step(&self, step_id: &str) -> Box<dyn StepRecovery<u64, TdPyAny, TdPyAny>> {
        let future = query(
            "CREATE TABLE IF NOT EXISTS states(epoch INTEGER, step_id, key, state, PRIMARY KEY (epoch, step_id, key));"
        ).execute(&self.pool);
        self.rt.block_on(future).unwrap();

        Box::new(SqliteStepRecovery {
            step_id: step_id.to_string(),
            rt: self.rt.clone(),
            pool: self.pool.clone(), // Said to be cheap.
        })
    }
}

struct SqliteStepRecovery {
    step_id: String,
    rt: SendWrapper<Rc<Runtime>>,
    pool: Pool<Sqlite>,
}

impl StepRecovery<u64, TdPyAny, TdPyAny> for SqliteStepRecovery {
    fn recover_last(&self, current_epoch: &u64, key: &TdPyAny) -> Option<TdPyAny> {
        let current_epoch_int =
            i64::try_from(*current_epoch).expect("Epoch too big to fit into SQLite int");
        let key_string: String =
            Python::with_gil(|py| key.extract(py)).expect("Key cannot be cast to string");

        let future = query("SELECT state FROM states WHERE step_id = ?1 AND key = ?2 AND epoch < ?3 ORDER BY epoch DESC LIMIT 1")
            .bind(&self.step_id)
            .bind(key_string)
            .bind(current_epoch_int)
            .map(|row: SqliteRow| {
                let state_pickled: &[u8] = row.get(0);
                Python::with_gil(|py| {
                    let pickle = py.import("dill")?;
                    Ok(pickle.call_method1("loads", (state_pickled, ))?.into())
                })
            })
            .fetch_optional(&self.pool);
        let state = self
            .rt
            .block_on(future)
            .unwrap()
            .map(|r: Result<TdPyAny, PyErr>| r.expect("Error unpickling state"));

        debug!(
            "sqlite recovery queried step_id={} key={key:?}:state={state:?}@epoch={current_epoch}",
            self.step_id
        );
        state
    }

    fn save_complete(&self, completed_epoch: &u64, key: &TdPyAny, state: &Option<TdPyAny>) -> () {
        let completed_epoch_int =
            i64::try_from(*completed_epoch).expect("Epoch too big to fit into SQLite int");
        let key_string: String =
            Python::with_gil(|py| key.extract(py)).expect("Key cannot be cast to string");
        let state_pickled: Option<Vec<u8>> = state.as_ref().map(|state| {
            Python::with_gil(|py| -> Result<Vec<u8>, PyErr> {
                let pickle = py.import("dill")?;
                Ok(pickle.call_method1("dumps", (state,))?.extract()?)
            })
            .expect("Error pickling state")
        });

        let future = query("INSERT INTO states (epoch, step_id, key, state) VALUES (?1, ?2, ?3, ?4) ON CONFLICT (epoch, step_id, key) DO UPDATE SET state = EXCLUDED.state")
            .bind(completed_epoch_int)
            .bind(&self.step_id)
            .bind(key_string)
            .bind(state_pickled)
            .execute(&self.pool);
        self.rt.block_on(future).unwrap();

        debug!(
            "sqlite recovery stored step_id={} key={key:?}:state={state:?}@epoch={completed_epoch}",
            self.step_id
        );
        // TODO: Warn on state overwriting?
        ()
    }
}

pub(crate) fn register(_py: Python, m: &PyModule) -> PyResult<()> {
    m.add_class::<RecoveryConfig>()?;
    m.add_class::<SqliteRecoveryConfig>()?;
    Ok(())
}
