//! Internal code for tracing/logging.
//!
//! This module is used to configure both tracing and logging.
//! Tracing and logging can be configured by the user, by default
//! they are disabled.
//!
//! Each tracing backend has to implement the `TracerBuilder` trait, which
//! requires a `build` function that is used to build the telemetry layer.

use opentelemetry::sdk::trace::Tracer;
use pyo3::exceptions::PyRuntimeError;
use pyo3::exceptions::PyTypeError;
use pyo3::prelude::*;
use tracing::level_filters::LevelFilter;
use tracing_subscriber::filter::Targets;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::Layer;
use tracing_subscriber::Registry;

pub(crate) mod jaeger_tracing;
pub(crate) mod otlp_tracing;

pub(crate) use jaeger_tracing::JaegerConfig;
pub(crate) use otlp_tracing::OtlpTracingConfig;

use crate::errors::tracked_err;
use crate::errors::PythonException;
use crate::pyo3_extensions::PyConfigClass;

/// Base class for tracing/logging configuration.
///
/// There defines what to do with traces and logs emitted by Bytewax.
///
/// Use a specific subclass of this to configure where you want the
/// traces to go.
#[pyclass(module = "bytewax.tracing", subclass)]
pub(crate) struct TracingConfig;

#[pymethods]
impl TracingConfig {
    #[new]
    fn new() -> Self {
        Self {}
    }
}

/// Trait that all the tracing config should implement.
/// This function should just return the proper `Tracer` for the backend.
pub(crate) trait TracerBuilder {
    fn build(&self) -> PyResult<Tracer>;
}

impl PyConfigClass<Box<dyn TracerBuilder + Send>> for Py<TracingConfig> {
    fn downcast(&self, py: Python) -> PyResult<Box<dyn TracerBuilder + Send>> {
        if let Ok(otlp_conf) = self.extract::<OtlpTracingConfig>(py) {
            Ok(Box::new(otlp_conf))
        } else if let Ok(jaeger_conf) = self.extract::<JaegerConfig>(py) {
            Ok(Box::new(jaeger_conf))
        } else {
            let pytype = self.bind(py).get_type();
            Err(tracked_err::<PyTypeError>(&format!(
                "Unknown tracing_config type: {pytype}"
            )))
        }
    }
}

/// Utility class used to handle tracing.
///
/// It keeps a tokio runtime that is alive as long as the struct itself.
///
/// This should only be built via `setup_tracing`.
#[pyclass]
struct BytewaxTracer {
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
            level => panic!("Unknown log level: {level}"),
        }
    } else {
        LevelFilter::ERROR
    }
}

async fn setup(
    log_level: LevelFilter,
    tracer: Option<Box<dyn TracerBuilder + Send>>,
) -> PyResult<()> {
    let logs = tracing_subscriber::fmt::Layer::default()
        .compact()
        // Show source file
        .with_file(true)
        // Display source code line numbers
        .with_line_number(true)
        // Display the thread ID an event was recorded on
        .with_thread_names(true)
        .with_filter(Targets::new().with_target("bytewax", log_level));

    // If the conf was not none, setup the global subscriber with both log and
    // telemetry layer, otherwise just setup logging.
    if let Some(tracer) = tracer {
        let tracer = tracer.build().reraise("error building tracer")?;
        let telemetry = tracing_opentelemetry::layer()
            .with_tracer(tracer)
            // Send all traces from bytewax
            .with_filter(Targets::new().with_target("bytewax", LevelFilter::TRACE));
        tracing::subscriber::set_global_default(Registry::default().with(logs).with(telemetry))
            .raise::<PyRuntimeError>("error setting global default tracer")
    } else {
        tracing::subscriber::set_global_default(Registry::default().with(logs))
            .raise::<PyRuntimeError>("error setting global default tracer")
    }
}

impl BytewaxTracer {
    /// Call this with a TracingConfig subclass to configure tracing.
    /// Returns a guard that you have to keep in scope for the
    /// whole execution of the code you want to trace.
    pub(crate) fn setup(
        &self,
        tracer: Option<Box<dyn TracerBuilder + Send>>,
        log_level: Option<String>,
    ) -> PyResult<()> {
        // Prepare the log layer
        let log_level = get_log_level(log_level);

        // We need an async fn block to properly initialize the tracing runtime
        // and be able to propagate errors.
        self.rt
            .block_on(self.rt.spawn(setup(log_level, tracer)))
            .map_err(|err| {
                tracked_err::<PyRuntimeError>(&format!("error setting up tracing: {err}"))
            })?
    }
}

/// Setup Bytewax's internal tracing and logging.
///
/// By default it starts a tracer that logs all `ERROR`-level messages
/// to stdout.
///
/// Note: To make this work, you have to keep a reference of the
/// returned object.
///
/// % Skip this doctest because it requires starting the webserver.
///
/// ```python
/// from bytewax.tracing import setup_tracing

/// tracer = setup_tracing()
/// ```
///
/// :arg tracing_config: The specific backend you want to use.
///
/// :type tracing_config: bytewax.tracing.TracingConfig
///
/// :arg log_level: String of the log level. One of `"ERROR"`,
///     `"WARN"`, `"INFO"`, `"DEBUG"`, `"TRACE"`. Defaults to
///     `"ERROR"`.
///
/// :type log_level: str
#[pyfunction]
#[pyo3(signature = (tracing_config=None, log_level=None))]
fn setup_tracing(
    py: Python,
    tracing_config: Option<Py<TracingConfig>>,
    log_level: Option<String>,
) -> PyResult<Bound<'_, BytewaxTracer>> {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap();
    let tracer = Bound::new(py, BytewaxTracer { rt })?;
    let builder = tracing_config.map(|conf| conf.downcast(py).unwrap());
    tracer.borrow().setup(builder, log_level)?;
    Ok(tracer)
}

pub(crate) fn register(_py: Python, m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<TracingConfig>()?;
    m.add_class::<JaegerConfig>()?;
    m.add_class::<OtlpTracingConfig>()?;
    m.add_class::<BytewaxTracer>()?;
    m.add_function(wrap_pyfunction!(setup_tracing, m)?)?;
    Ok(())
}
