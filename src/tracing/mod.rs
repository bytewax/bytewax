//! Internal code for tracing.
use pyo3::{
    exceptions::PyValueError, pyclass, pymethods, types::PyModule, Py, PyAny, PyCell, PyResult,
    Python,
};
use tokio::runtime::EnterGuard;
use tracing::{level_filters::LevelFilter, subscriber::SetGlobalDefaultError};
use tracing_subscriber::{filter::Targets, registry::LookupSpan, Layer};

pub(crate) mod jaeger_tracing;
pub(crate) mod oltp_tracing;
pub(crate) mod stdout_tracing;

pub(crate) use jaeger_tracing::JaegerConfig;
pub(crate) use oltp_tracing::OltpTracingConfig;
pub(crate) use stdout_tracing::StdOutTracingConfig;

/// Tracing layer that logs to stdout, filtered by an environment variable.
pub(crate) fn log_layer<S>(log_level: LevelFilter) -> impl Layer<S>
where
    S: tracing::Subscriber + for<'span> LookupSpan<'span>,
{
    // TODO: Do we want to offer customization here?
    tracing_subscriber::fmt::Layer::default()
        .compact()
        // Show source file
        .with_file(true)
        // Display source code line numbers
        .with_line_number(true)
        // Display the thread ID an event was recorded on
        .with_thread_ids(true)
        // Don't display the event's target (module path)
        .with_target(false)
        .with_filter(Targets::new().with_target("bytewax", log_level))
}

/// Base class for tracing/logging configuration.
///
/// There defines what to do with traces and logs emitted by Bytewax.
///
/// Use a specific subclass of this to configure where you want the
/// traces to go.
#[pyclass(module = "bytewax.tracing", subclass)]
#[pyo3(text_signature = "()")]
pub(crate) struct TracingConfig;

impl TracingConfig {
    /// Create an "empty" [`Self`] just for use in `__getnewargs__`.
    #[allow(dead_code)]
    pub(crate) fn pickle_new(py: Python) -> Py<Self> {
        PyCell::new(py, TracingConfig::py_new()).unwrap().into()
    }
}

#[pymethods]
impl TracingConfig {
    #[new]
    fn py_new() -> Self {
        Self {}
    }
    /// Pickle as a tuple.
    fn __getstate__(&self) -> (&str,) {
        ("TracingConfig",)
    }

    /// Unpickle from tuple of arguments.
    fn __setstate__(&mut self, state: &PyAny) -> PyResult<()> {
        if let Ok(("TracingConfig",)) = state.extract() {
            Ok(())
        } else {
            Err(PyValueError::new_err(format!(
                "bad pickle contents for TracingConfig: {state:?}"
            )))
        }
    }
}

/// Trait that all the tracing config should implement.
/// This function should try to setup tracing and return
/// an error is something went wrong.
trait TracerBuilder {
    fn setup(&self, log_level: LevelFilter) -> Result<(), SetGlobalDefaultError>;
}

/// Utility class used to handle tracing.
///
/// It keeps a tokio runtime that is alive as long as the struct itself.
pub(crate) struct BytewaxTracer {
    rt: tokio::runtime::Runtime,
}

fn get_log_level(level: Option<String>) -> LevelFilter {
    if let Some(level) = level {
        match level.to_lowercase().as_str() {
            "trace" => LevelFilter::TRACE,
            "debug" => LevelFilter::DEBUG,
            "info" => LevelFilter::INFO,
            "warn" => LevelFilter::WARN,
            "error" => LevelFilter::ERROR,
            level => panic!("Wrong log level: {level}"),
        }
    } else {
        LevelFilter::ERROR
    }
}

impl BytewaxTracer {
    pub fn new() -> Self {
        let rt = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .unwrap();
        Self { rt }
    }

    fn extract_py_conf(
        py: Python,
        py_conf: Py<TracingConfig>,
    ) -> Result<Box<dyn TracerBuilder>, String> {
        if let Ok(oltp_conf) = py_conf.extract::<OltpTracingConfig>(py) {
            Ok(Box::new(oltp_conf))
        } else if let Ok(jaeger_conf) = py_conf.extract::<JaegerConfig>(py) {
            Ok(Box::new(jaeger_conf))
        } else if let Ok(stdout_conf) = py_conf.extract::<StdOutTracingConfig>(py) {
            Ok(Box::new(stdout_conf))
        } else {
            Err(format!("Unrecognized tracing config: {py_conf:?}"))
        }
    }

    /// Call this with a TracingConfig subclass to configure tracing.
    /// Returns a guard that you have to keep in scope for the
    /// whole execution of the code you want to trace.
    pub fn setup<'a>(
        &'a self,
        py_conf: Py<TracingConfig>,
        log_level: Option<String>,
    ) -> EnterGuard<'a> {
        let guard = self.rt.enter();

        let log_level = get_log_level(log_level);

        // We need an async block to properly initialize the tracing runtime.
        let initializer = async move {
            Python::with_gil(|py| {
                let conf = Self::extract_py_conf(py, py_conf).unwrap();
                if let Err(err) = conf.setup(log_level) {
                    // Try to setup tracing, but if it fails setup stdout and log the error.
                    // This can fail if tracing was already initialized, which currently
                    // happens in all tests.
                    _ = StdOutTracingConfig::new().setup(log_level);
                    tracing::warn!("{err}");
                }
            });
        };
        self.rt.block_on(self.rt.spawn(initializer)).unwrap();
        guard
    }
}

pub(crate) fn register(_py: Python, m: &PyModule) -> PyResult<()> {
    m.add_class::<TracingConfig>()?;
    m.add_class::<JaegerConfig>()?;
    m.add_class::<StdOutTracingConfig>()?;
    m.add_class::<OltpTracingConfig>()?;
    Ok(())
}
