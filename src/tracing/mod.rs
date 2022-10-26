//! Internal code for tracing/logging.
//!
//! This module is used to configure both tracing and logging.
//! Logging to stdout is always enabled, at least at the "ERROR" level.
//! Tracing can be configured by the user, by default it is disabled.
//!
//! Each tracing backend has to implement the `TracerBuilder` trait, which
//! requires a `setup` function that is used to configure both tracing and logging.
use opentelemetry::sdk::trace::Tracer;
use pyo3::{
    exceptions::PyValueError, pyclass, pymethods, types::PyModule, Py, PyAny, PyCell, PyResult,
    Python,
};
use tokio::runtime::EnterGuard;
use tracing::{level_filters::LevelFilter, Subscriber};
use tracing_subscriber::{filter::Targets, layer::SubscriberExt, Layer, Registry};

pub(crate) mod jaeger_tracing;
pub(crate) mod oltp_tracing;

pub(crate) use jaeger_tracing::JaegerConfig;
pub(crate) use oltp_tracing::OltpTracingConfig;

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
/// This function should just return the proper `Tracer` for the backend.
trait TracerBuilder {
    fn build(&self) -> Tracer;
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
        } else {
            Err(format!("Unrecognized tracing config: {py_conf:?}"))
        }
    }

    /// Call this with a TracingConfig subclass to configure tracing.
    /// Returns a guard that you have to keep in scope for the
    /// whole execution of the code you want to trace.
    pub fn setup(
        &self,
        py_conf: Option<Py<TracingConfig>>,
        log_level: Option<String>,
    ) -> EnterGuard<'_> {
        let guard = self.rt.enter();

        let log_level = get_log_level(log_level);

        // We need an async block to properly initialize the tracing runtime.
        let initializer = async move {
            // Only keep the GIL to extract the conf struct
            let conf = Python::with_gil(|py| {
                py_conf.map(|py_conf| Self::extract_py_conf(py, py_conf).unwrap())
            });

            // Prepare the log layer
            let logs = tracing_subscriber::fmt::Layer::default()
                .compact()
                // Show source file
                .with_file(true)
                // Display source code line numbers
                .with_line_number(true)
                // Display the thread ID an event was recorded on
                .with_thread_ids(true)
                .with_filter(Targets::new().with_target("bytewax", log_level));

            // If the conf was not none, setup the global subscriber with both log and
            // telemetry layer, otherwise just setup logging.
            if let Some(conf) = conf {
                let tracer = conf.build();
                let telemetry = tracing_opentelemetry::layer()
                    .with_tracer(tracer)
                    // Send all traces from bytewax
                    .with_filter(Targets::new().with_target("bytewax", LevelFilter::TRACE));
                set_global_subscriber(Registry::default().with(logs).with(telemetry));
            } else {
                set_global_subscriber(Registry::default().with(logs));
            };
        };
        self.rt.block_on(self.rt.spawn(initializer)).unwrap();
        guard
    }
}

// Utility function used to try to set a global default subscriber,
// logging the error without panicking if it was already set
fn set_global_subscriber<S>(subscriber: S)
where
    S: Subscriber + Send + Sync + 'static,
{
    // This can fail if tracing was already initialized, which currently
    // happens in all tests, and also if the user runs a cluster more than once
    // in the same process.
    if let Err(err) = tracing::subscriber::set_global_default(subscriber) {
        tracing::warn!("{err}");
    }
}

pub(crate) fn register(_py: Python, m: &PyModule) -> PyResult<()> {
    m.add_class::<TracingConfig>()?;
    m.add_class::<JaegerConfig>()?;
    m.add_class::<OltpTracingConfig>()?;
    Ok(())
}
