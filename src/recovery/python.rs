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
    fn new() -> Self {
        Self {}
    }

    /// Pickle as a tuple.
    fn __getstate__(&self) -> (&str,) {
        ("RecoveryConfig",)
    }

    /// Unpickle from tuple of arguments.
    fn __setstate__(&mut self, state: &PyAny) -> PyResult<()> {
        if let Ok(("RecoveryConfig",)) = state.extract() {
            Ok(())
        } else {
            Err(PyValueError::new_err(format!(
                "bad pickle contents for RecoveryConfig: {state:?}"
            )))
        }
    }
}

/// Do not store any recovery data.
///
/// This is the default if no `recovery_config` is passed to your
/// execution entry point, so you shouldn't need to build this
/// explicitly.
#[pyclass(module="bytewax.recovery", extends=RecoveryConfig)]
struct NoopRecoveryConfig;

#[pymethods]
impl NoopRecoveryConfig {
    #[new]
    fn new() -> (Self, RecoveryConfig) {
        (Self {}, RecoveryConfig {})
    }

    /// Pickle as a tuple.
    fn __getstate__(&self) -> (&str,) {
        ("NoopRecoveryConfig",)
    }

    /// Unpickle from tuple of arguments.
    fn __setstate__(&mut self, state: &PyAny) -> PyResult<()> {
        if let Ok(("NoopRecoveryConfig",)) = state.extract() {
            Ok(())
        } else {
            Err(PyValueError::new_err(format!(
                "bad pickle contents for NoopRecoveryConfig: {state:?}"
            )))
        }
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
/// >>> from bytewax.execution import run_main
/// >>> from bytewax.inputs import TestingInputConfig
/// >>> from bytewax.outputs import StdOutputConfig
/// >>> flow = Dataflow()
/// >>> flow.input("inp", TestingInputConfig(range(3)))
/// >>> flow.capture(StdOutputConfig())
/// >>> tmp_dir = TemporaryDirectory()  # We'll store this somewhere temporary for this test.
/// >>> recovery_config = SqliteRecoveryConfig(tmp_dir)
/// >>> run_main(
/// ...     flow,
/// ...     recovery_config=recovery_config,
/// ... )  # doctest: +ELLIPSIS
/// (...)
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
#[pyo3(text_signature = "(db_dir)")]
pub(crate) struct SqliteRecoveryConfig {
    #[pyo3(get)]
    db_dir: PathBuf,
}

#[pymethods]
impl SqliteRecoveryConfig {
    #[new]
    #[args(db_dir)]
    pub(crate) fn new(db_dir: PathBuf) -> (Self, RecoveryConfig) {
        (Self { db_dir }, RecoveryConfig {})
    }

    /// Pickle as a tuple.
    fn __getstate__(&self) -> (&str, PathBuf) {
        ("SqliteRecoveryConfig", self.db_dir.clone())
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
        if let Ok(("SqliteRecoveryConfig", db_dir)) = state.extract() {
            self.db_dir = db_dir;
            Ok(())
        } else {
            Err(PyValueError::new_err(format!(
                "bad pickle contents for SqliteRecoveryConfig: {state:?}"
            )))
        }
    }
}

impl SqliteRecoveryConfig {
    pub(crate) fn db_file(&self, worker_index: usize) -> PathBuf {
        self.db_dir.join(format!("worker{worker_index}.sqlite3"))
    }
}

/// Use [Kafka](https://kafka.apache.org/) to store recovery data.
///
/// Uses a "progress" topic and a "state" topic with a number of
/// partitions equal to the number of workers. Will take advantage of
/// log compaction so that topic size is proportional to state size,
/// not epoch count.
///
/// Use a distinct topic prefix per dataflow so recovery data is not
/// mixed.
///
/// >>> from bytewax.execution import run_main
/// >>> from bytewax.inputs import TestingInputConfig
/// >>> from bytewax.outputs import StdOutputConfig
/// >>> flow = Dataflow()
/// >>> flow.inp("inp", TestingInputConfig(range(3)))
/// >>> flow.capture(StdOutputConfig())
/// >>> recovery_config = KafkaRecoveryConfig(
/// ...     ["localhost:9092"],
/// ...     "sample-dataflow",
/// ... )
/// >>> run_main(
/// ...     flow,
/// ...     recovery_config=recovery_config,
/// ... )  # doctest: +ELLIPSIS
/// (...)
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
pub(crate) struct KafkaRecoveryConfig {
    #[pyo3(get)]
    pub(crate) brokers: Vec<String>,
    #[pyo3(get)]
    pub(crate) topic_prefix: String,
}

#[pymethods]
impl KafkaRecoveryConfig {
    #[new]
    #[args(brokers, topic_prefix)]
    pub(crate) fn new(brokers: Vec<String>, topic_prefix: String) -> (Self, RecoveryConfig) {
        (
            Self {
                brokers,
                topic_prefix,
            },
            RecoveryConfig {},
        )
    }

    /// Pickle as a tuple.
    fn __getstate__(&self) -> (&str, Vec<String>, &str) {
        (
            "KafkaRecoveryConfig",
            self.brokers.clone(),
            &self.topic_prefix,
        )
    }

    /// Egregious hack see [`SqliteRecoveryConfig::__getnewargs__`].
    fn __getnewargs__(&self) -> (Vec<String>, &str) {
        (vec![], "UNINIT_PICKLED_STRING")
    }

    /// Unpickle from tuple of arguments.
    fn __setstate__(&mut self, state: &PyAny) -> PyResult<()> {
        if let Ok(("KafkaRecoveryConfig", hosts, topic_prefix)) = state.extract() {
            self.brokers = hosts;
            self.topic_prefix = topic_prefix;
            Ok(())
        } else {
            Err(PyValueError::new_err(format!(
                "bad pickle contents for KafkaRecoveryConfig: {state:?}"
            )))
        }
    }
}

impl KafkaRecoveryConfig {
    pub(crate) fn progress_topic(&self) -> String {
        format!("{}-progress", self.topic_prefix)
    }

    pub(crate) fn state_topic(&self) -> String {
        format!("{}-state", self.topic_prefix)
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
        PyCell::new(py, NoopRecoveryConfig::new())
            .unwrap()
            .extract()
            .unwrap()
    })
}

/// Use a recovery config and the current worker's identity to build
/// out the specific recovery writer instances that this worker will
/// need to backup recovery data.
///
/// This function is also the Python-Rust barrier for recovery; we
/// don't have any Python types in the recovery machinery after this.
pub(crate) fn build_recovery_writers(
    py: Python,
    worker_index: usize,
    worker_count: usize,
    recovery_config: Py<RecoveryConfig>,
) -> StringResult<(
    Box<dyn ProgressWriter<u64>>,
    Box<dyn StateWriter<u64>>,
    Box<dyn StateCollector<u64>>,
)> {
    // Horrible news: we have to be very studious and release the GIL
    // any time we know we have it and we call into complex Rust
    // libraries because internally it might call log!() on a
    // background thread, which because of `pyo3-log` might try to
    // re-acquire the GIL and then you have deadlock. E.g. `sqlx` and
    // `rdkafka` always spawn background threads.
    let recovery_config = recovery_config.as_ref(py);

    if let Ok(_noop_recovery_config) = recovery_config.downcast::<PyCell<NoopRecoveryConfig>>() {
        let (state_writer, progress_writer, state_collector) = py.allow_threads(|| {
            (
                NoopRecovery::new(),
                NoopRecovery::new(),
                NoopRecovery::new(),
            )
        });
        Ok((
            Box::new(state_writer),
            Box::new(progress_writer),
            Box::new(state_collector),
        ))
    } else if let Ok(sqlite_recovery_config) =
        recovery_config.downcast::<PyCell<SqliteRecoveryConfig>>()
    {
        let sqlite_recovery_config = sqlite_recovery_config.borrow();

        let db_file = sqlite_recovery_config.db_file(worker_index);

        let (progress_writer, state_writer, state_collector) = py.allow_threads(|| {
            (
                SqliteProgressWriter::new(&db_file),
                SqliteStateWriter::new(&db_file),
                SqliteStateWriter::new(&db_file),
            )
        });

        Ok((
            Box::new(progress_writer),
            Box::new(state_writer),
            Box::new(state_collector),
        ))
    } else if let Ok(kafka_recovery_config) =
        recovery_config.downcast::<PyCell<KafkaRecoveryConfig>>()
    {
        let kafka_recovery_config = kafka_recovery_config.borrow();

        let hosts = &kafka_recovery_config.brokers;
        let state_topic = kafka_recovery_config.state_topic();
        let progress_topic = kafka_recovery_config.progress_topic();
        let partition = worker_index.try_into().unwrap();
        let create_partitions = worker_count.try_into().unwrap();

        let (progress_writer, state_writer, state_collector): (
            KafkaWriter<ProgressRecoveryKey, ProgressOp<u64>>,
            KafkaWriter<StateRecoveryKey<u64>, StateOp>,
            KafkaWriter<StateRecoveryKey<u64>, StateOp>,
        ) = py.allow_threads(|| {
            create_kafka_topic(hosts, &progress_topic, create_partitions);
            create_kafka_topic(hosts, &state_topic, create_partitions);

            (
                KafkaWriter::new(hosts, progress_topic, partition),
                KafkaWriter::new(hosts, state_topic.clone(), partition),
                KafkaWriter::new(hosts, state_topic, partition),
            )
        });

        Ok((
            Box::new(progress_writer),
            Box::new(state_writer),
            Box::new(state_collector),
        ))
    } else {
        Err(format!(
            "Unknown recovery_config type: {}",
            recovery_config.get_type(),
        ))
    }
}

/// Use a recovery config and the current worker's identity to build
/// out the specific recovery reader instances that this worker will
/// need to load recovery data.
///
/// This function is also the Python-Rust barrier for recovery; we
/// don't have any Python types in the recovery machinery after this.
///
/// We need to know worker count and index here because each worker
/// needs to read distinct loading data from a worker in the previous
/// dataflow execution.
///
/// Note that as of now, this code assumes that the number of workers
/// _has not changed between executions_. Things will silently not
/// fully load if worker count is changed.
pub(crate) fn build_recovery_readers(
    py: Python,
    worker_index: usize,
    worker_count: usize,
    recovery_config: Py<RecoveryConfig>,
) -> StringResult<(Box<dyn ProgressReader<u64>>, Box<dyn StateReader<u64>>)> {
    // See comment about the GIL in
    // [`build_recovery_writers`].
    let recovery_config = recovery_config.as_ref(py);

    if let Ok(_noop_recovery_config) = recovery_config.downcast::<PyCell<NoopRecoveryConfig>>() {
        let (state_reader, progress_reader) =
            py.allow_threads(|| (NoopRecovery::new(), NoopRecovery::new()));
        Ok((Box::new(state_reader), Box::new(progress_reader)))
    } else if let Ok(sqlite_recovery_config) =
        recovery_config.downcast::<PyCell<SqliteRecoveryConfig>>()
    {
        let sqlite_recovery_config = sqlite_recovery_config.borrow();

        let db_file = sqlite_recovery_config.db_file(worker_index);

        let (progress_reader, state_reader) = py.allow_threads(|| {
            (
                SqliteProgressReader::new(&db_file),
                SqliteStateReader::new(&db_file),
            )
        });

        Ok((Box::new(progress_reader), Box::new(state_reader)))
    } else if let Ok(kafka_recovery_config) =
        recovery_config.downcast::<PyCell<KafkaRecoveryConfig>>()
    {
        let kafka_recovery_config = kafka_recovery_config.borrow();

        let brokers = &kafka_recovery_config.brokers;
        let state_topic = kafka_recovery_config.state_topic();
        let progress_topic = kafka_recovery_config.progress_topic();
        let partition = worker_index.try_into().unwrap();
        let create_partitions = worker_count.try_into().unwrap();

        let (progress_reader, state_reader) = py.allow_threads(|| {
            create_kafka_topic(brokers, &progress_topic, create_partitions);
            create_kafka_topic(brokers, &state_topic, create_partitions);

            (
                KafkaReader::new(brokers, &progress_topic, partition),
                KafkaReader::new(brokers, &state_topic, partition),
            )
        });

        Ok((Box::new(progress_reader), Box::new(state_reader)))
    } else {
        Err(format!(
            "Unknown recovery_config type: {}",
            recovery_config.get_type(),
        ))
    }
}

impl IntoPy<PyObject> for StepId {
    fn into_py(self, py: Python) -> Py<PyAny> {
        self.0.into_py(py)
    }
}

impl<'source> FromPyObject<'source> for StateKey {
    fn extract(ob: &'source PyAny) -> PyResult<Self> {
        if let Ok(py_string) = ob.cast_as::<PyString>() {
            Ok(Self::Hash(py_string.to_str()?.into()))
        } else if let Ok(py_int) = ob.cast_as::<PyLong>() {
            Ok(Self::Worker(WorkerIndex(py_int.extract()?)))
        } else {
            Err(PyTypeError::new_err("Can only make StateKey out of either str (route to worker by hash) or int (route to worker by index)"))
        }
    }
}

impl IntoPy<PyObject> for StateKey {
    fn into_py(self, py: Python) -> Py<PyAny> {
        match self {
            Self::Hash(key) => key.into_py(py),
            Self::Worker(index) => index.0.into_py(py),
        }
    }
}
