//! Python objects and functions for recovery.
//!
//! The rest of the recovery system should be Python-agnostic.

use super::model::*;
use super::store::kafka::*;
use super::store::noop::*;
use super::store::sqlite::*;
use crate::add_pymethods;
use crate::errors::tracked_err;
use crate::errors::PythonException;
use crate::pyo3_extensions::PyConfigClass;
use pyo3::exceptions::*;
use pyo3::prelude::*;
use std::collections::HashMap;
use std::path::PathBuf;

/// Base class for a recovery config.
///
/// This describes how each worker in a dataflow cluster should store
/// its recovery data.
///
/// Use a specific subclass of this that matches the kind of storage
/// system you are going to use. See the subclasses in this module.
#[pyclass(module = "bytewax.recovery", subclass)]
#[pyo3(text_signature = "()")]
pub(crate) struct RecoveryConfig;

impl RecoveryConfig {
    /// Create an "empty" [`Self`] just for use in `__getnewargs__`.
    #[allow(dead_code)]
    pub(crate) fn pickle_new(py: Python) -> Py<Self> {
        PyCell::new(py, RecoveryConfig {}).unwrap().into()
    }
}

#[pymethods]
impl RecoveryConfig {
    #[new]
    pub(crate) fn new() -> Self {
        Self {}
    }

    /// Return a representation of this class as a PyDict.
    fn __getstate__(&self) -> HashMap<&str, Py<PyAny>> {
        Python::with_gil(|py| HashMap::from([("type", "RecoveryConfig".into_py(py))]))
    }

    /// Unpickle from a PyDict.
    fn __setstate__(&mut self, _state: &PyAny) -> PyResult<()> {
        Ok(())
    }
}

pub(crate) trait RecoveryBuilder {
    fn build_writers(
        &self,
        py: Python,
        worker_index: usize,
        worker_count: usize,
    ) -> PyResult<(Box<dyn ProgressWriter>, Box<dyn StateWriter>)>;
    fn build_readers(
        &self,
        py: Python,
        worker_index: usize,
        worker_count: usize,
    ) -> PyResult<(Box<dyn ProgressReader>, Box<dyn StateReader>)>;
}

impl PyConfigClass<Box<dyn RecoveryBuilder>> for Py<RecoveryConfig> {
    fn downcast(&self, py: Python) -> PyResult<Box<dyn RecoveryBuilder>> {
        if let Ok(conf) = self.extract::<NoopRecoveryConfig>(py) {
            Ok(Box::new(conf))
        } else if let Ok(conf) = self.extract::<KafkaRecoveryConfig>(py) {
            Ok(Box::new(conf))
        } else if let Ok(conf) = self.extract::<SqliteRecoveryConfig>(py) {
            Ok(Box::new(conf))
        } else {
            let pytype = self.as_ref(py).get_type();
            Err(tracked_err::<PyTypeError>(&format!(
                "Unknown recovery_config type: {pytype}"
            )))
        }
    }
}

impl RecoveryBuilder for Py<RecoveryConfig> {
    fn build_writers(
        &self,
        py: Python,
        worker_index: usize,
        worker_count: usize,
    ) -> PyResult<(Box<dyn ProgressWriter>, Box<dyn StateWriter>)> {
        self.downcast(py)?
            .build_writers(py, worker_index, worker_count)
    }

    fn build_readers(
        &self,
        py: Python,
        worker_index: usize,
        worker_count: usize,
    ) -> PyResult<(Box<dyn ProgressReader>, Box<dyn StateReader>)> {
        self.downcast(py)?
            .build_readers(py, worker_index, worker_count)
    }
}

/// Do not store any recovery data.
///
/// This is the default if no `recovery_config` is passed to your
/// execution entry point, so you shouldn't need to build this
/// explicitly.
#[pyclass(module="bytewax.recovery", extends=RecoveryConfig)]
#[derive(Clone)]
pub(crate) struct NoopRecoveryConfig;

add_pymethods!(
    NoopRecoveryConfig,
    parent: RecoveryConfig,
    signature: (),
    args {}
);

impl RecoveryBuilder for NoopRecoveryConfig {
    fn build_writers(
        &self,
        py: Python,
        _worker_index: usize,
        _worker_count: usize,
    ) -> PyResult<(Box<dyn ProgressWriter>, Box<dyn StateWriter>)> {
        let (progress_writer, state_writer) =
            py.allow_threads(|| (NoOpStore::new(), NoOpStore::new()));
        Ok((Box::new(progress_writer), Box::new(state_writer)))
    }

    fn build_readers(
        &self,
        py: Python,
        _worker_index: usize,
        _worker_count: usize,
    ) -> PyResult<(Box<dyn ProgressReader>, Box<dyn StateReader>)> {
        let (progress_reader, state_reader) =
            py.allow_threads(|| (NoOpStore::new(), NoOpStore::new()));
        Ok((Box::new(progress_reader), Box::new(state_reader)))
    }
}

/// Use [SQLite](https://sqlite.org/index.html) to store recovery
/// data.
///
/// Creates a SQLite DB per-worker in a given directory. Multiple DBs
/// are used to allow workers to write without contention.
///
/// Use a distinct directory per dataflow so recovery data is not
/// mixed.
///
/// >>> from tempfile import TemporaryDirectory
/// >>> from bytewax.testing import run_main, TestingInput
/// >>> from bytewax.connectors.stdio import StdOutput
/// >>> from bytewax.dataflow import Dataflow
/// >>> flow = Dataflow()
/// >>> flow.input("inp", TestingInput(range(3)))
/// >>> flow.output("out", StdOutput())
/// >>> tmp_dir = TemporaryDirectory()  # We'll store this somewhere temporary for this test.
/// >>> recovery_config = SqliteRecoveryConfig(tmp_dir.name)
/// >>> run_main(
/// ...     flow,
/// ...     recovery_config=recovery_config,
/// ... )
/// 0
/// 1
/// 2
///
/// DB files and tables will automatically be created if there's no
/// previous recovery data.
///
/// Args:
///
///   db_dir (Path): Existing directory to store per-worker DBs
///       in. Must be distinct per-dataflow. DB files will have
///       names like `"worker0.sqlite3"`. You can use `"."` for the
///       current directory.
///
/// Returns:
///
///   Config object. Pass this as the `recovery_config` argument to
///   your execution entry point.
#[pyclass(module="bytewax.recovery", extends=RecoveryConfig)]
#[derive(Clone)]
pub(crate) struct SqliteRecoveryConfig {
    #[pyo3(get)]
    db_dir: PathBuf,
}

add_pymethods!(
    SqliteRecoveryConfig,
    parent: RecoveryConfig,
    signature: (db_dir),
    args {
        db_dir: PathBuf => String::new().into()
    }
);

impl SqliteRecoveryConfig {
    pub(crate) fn db_file(&self, worker_index: usize) -> PathBuf {
        self.db_dir.join(format!("worker{worker_index}.sqlite3"))
    }
}

impl RecoveryBuilder for SqliteRecoveryConfig {
    fn build_writers(
        &self,
        py: Python,
        worker_index: usize,
        _worker_count: usize,
    ) -> PyResult<(Box<dyn ProgressWriter>, Box<dyn StateWriter>)> {
        let db_file = self.db_file(worker_index);
        let (progress_writer, state_writer) = py.allow_threads(|| -> PyResult<_> {
            Ok((
                SqliteProgressWriter::new(&db_file)
                    .raise::<PyRuntimeError>("error creating sqlite progress writer")?,
                SqliteStateWriter::new(&db_file)
                    .raise::<PyRuntimeError>("error creating sqlite state writer")?,
            ))
        })?;

        Ok((Box::new(progress_writer), Box::new(state_writer)))
    }

    fn build_readers(
        &self,
        py: Python,
        worker_index: usize,
        _worker_count: usize,
    ) -> PyResult<(Box<dyn ProgressReader>, Box<dyn StateReader>)> {
        let db_file = self.db_file(worker_index);

        let (progress_reader, state_reader) = py.allow_threads(|| -> PyResult<_> {
            Ok((
                SqliteProgressReader::new(&db_file)
                    .raise::<PyRuntimeError>("error creating sqlite progress reader")?,
                SqliteStateReader::new(&db_file)
                    .raise::<PyRuntimeError>("error creating sqlite state reader")?,
            ))
        })?;

        Ok((Box::new(progress_reader), Box::new(state_reader)))
    }
}

/// Use [Kafka](https://kafka.apache.org/) to store recovery data.
///
/// .. deprecated::
///
///     SQLite will be the only available recovery store in a future
///     version of Bytewax. Use `SqliteRecoveryConfig` currently to
///     facilitate a simple transition at that point.
///
/// Uses a "progress" topic and a "state" topic with a number of
/// partitions equal to the number of workers. Will take advantage of
/// log compaction so that topic size is proportional to state size,
/// not epoch count.
///
/// Use a distinct topic prefix per dataflow so recovery data is not
/// mixed.
///
/// >>> from bytewax.testing import run_main, TestingInput
/// >>> from bytewax.connectors.stdio import StdOutput
/// >>> from bytewax.dataflow import Dataflow
/// >>> flow = Dataflow()
/// >>> flow.input("inp", TestingInput(range(3)))
/// >>> flow.output("out", StdOutput())
/// >>> recovery_config = KafkaRecoveryConfig(
/// ...     ["localhost:9092"],
/// ...     "sample-dataflow",
/// ... )
/// >>> run_main(
/// ...     flow,
/// ...     recovery_config=recovery_config,
/// ... ) # doctest:+SKIP
///
/// If there's no previous recovery data, topics will automatically be
/// created with the correct number of partitions and log compaction
/// enabled
///
/// Args:
///
///   brokers (List[str]): List of `host:port` strings of Kafka
///       brokers.
///
///   topic_prefix (str): Prefix used for naming topics. Must be
///       distinct per-dataflow. Two topics will be created using
///       this prefix `"topic_prefix-progress"` and
///       `"topic_prefix-state"`.
///
/// Returns:
///
///   Config object. Pass this as the `recovery_config` argument to
///   your execution entry point.
#[pyclass(module="bytewax.recovery", extends=RecoveryConfig)]
#[pyo3(text_signature = "(brokers, topic_prefix)")]
#[derive(Clone)]
pub(crate) struct KafkaRecoveryConfig {
    #[pyo3(get)]
    pub(crate) brokers: Vec<String>,
    #[pyo3(get)]
    pub(crate) topic_prefix: String,
}

add_pymethods!(
    KafkaRecoveryConfig,
    parent: RecoveryConfig,
    signature: (brokers, topic_prefix),
    args {
        brokers: Vec<String> => Vec::new(),
        topic_prefix: String => String::new()
    }
);

impl KafkaRecoveryConfig {
    pub(crate) fn progress_topic(&self) -> String {
        format!("{}-progress", self.topic_prefix)
    }

    pub(crate) fn state_topic(&self) -> String {
        format!("{}-state", self.topic_prefix)
    }
}

impl RecoveryBuilder for KafkaRecoveryConfig {
    fn build_writers(
        &self,
        py: Python,
        worker_index: usize,
        worker_count: usize,
    ) -> PyResult<(Box<dyn ProgressWriter>, Box<dyn StateWriter>)> {
        let state_topic = self.state_topic();
        let progress_topic = self.progress_topic();
        let partition = worker_index.try_into().unwrap();
        let create_partitions = worker_count.try_into().unwrap();

        let (progress_writer, state_writer) = py.allow_threads(|| {
            create_kafka_topic(&self.brokers, &progress_topic, create_partitions);
            create_kafka_topic(&self.brokers, &state_topic, create_partitions);

            (
                KafkaWriter::new(&self.brokers, progress_topic, partition),
                KafkaWriter::new(&self.brokers, state_topic, partition),
            )
        });

        Ok((Box::new(progress_writer), Box::new(state_writer)))
    }

    fn build_readers(
        &self,
        py: Python,
        worker_index: usize,
        worker_count: usize,
    ) -> PyResult<(Box<dyn ProgressReader>, Box<dyn StateReader>)> {
        let state_topic = self.state_topic();
        let progress_topic = self.progress_topic();
        let partition = worker_index.try_into().unwrap();
        let create_partitions = worker_count.try_into().unwrap();

        let (progress_reader, state_reader) = py.allow_threads(|| {
            create_kafka_topic(&self.brokers, &progress_topic, create_partitions);
            create_kafka_topic(&self.brokers, &state_topic, create_partitions);

            (
                KafkaReader::new(&self.brokers, &progress_topic, partition),
                KafkaReader::new(&self.brokers, &state_topic, partition),
            )
        });

        Ok((Box::new(progress_reader), Box::new(state_reader)))
    }
}

pub(crate) fn register(_py: Python, m: &PyModule) -> PyResult<()> {
    m.add_class::<RecoveryConfig>()?;
    m.add_class::<SqliteRecoveryConfig>()?;
    m.add_class::<KafkaRecoveryConfig>()?;
    Ok(())
}

pub(crate) fn default_recovery_config() -> Py<RecoveryConfig> {
    Python::with_gil(|py| {
        PyCell::new(py, NoopRecoveryConfig::py_new())
            .unwrap()
            .extract()
            .unwrap()
    })
}

impl<'source> FromPyObject<'source> for StepId {
    fn extract(ob: &'source PyAny) -> PyResult<Self> {
        Ok(Self(<String as FromPyObject>::extract(ob)?))
    }
}

impl IntoPy<PyObject> for StepId {
    fn into_py(self, py: Python) -> Py<PyAny> {
        self.0.into_py(py)
    }
}

impl<'source> FromPyObject<'source> for StateKey {
    fn extract(ob: &'source PyAny) -> PyResult<Self> {
        Ok(Self(<String as FromPyObject>::extract(ob)?))
    }
}

impl IntoPy<PyObject> for StateKey {
    fn into_py(self, py: Python) -> Py<PyAny> {
        self.0.into_py(py)
    }
}
