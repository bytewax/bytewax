//! Internal code for tracing/logging.
//!
//! This module is used to configure both tracing and logging.
//! Tracing and logging can be configured by the user, by default
//! they are disabled.
//!
//! Each tracing backend has to implement the `TracerBuilder` trait, which
//! requires a `build` function that is used to build the telemetry layer.
use opentelemetry::sdk::trace::Tracer;
use pyo3::{
    exceptions::PyValueError, pyclass, pymethods, types::PyModule, Py, PyAny, PyCell, PyResult,
    Python,
};
use tracing::level_filters::LevelFilter;
use tracing_subscriber::{filter::Targets, layer::SubscriberExt, Layer, Registry};

pub(crate) mod jaeger_tracing;
pub(crate) mod otlp_tracing;

pub(crate) use jaeger_tracing::JaegerConfig;
pub(crate) use otlp_tracing::OtlpTracingConfig;

use crate::common::{ParentClass, StringResult};

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
pub(crate) trait TracerBuilder {
    fn build(&self) -> StringResult<Tracer>;
}

impl ParentClass for Py<TracingConfig> {
    type Children = Box<dyn TracerBuilder + Send>;

    fn get_subclass(&self, py: Python) -> StringResult<Self::Children> {
        if let Ok(otlp_conf) = self.extract::<OtlpTracingConfig>(py) {
            Ok(Box::new(otlp_conf))
        } else if let Ok(jaeger_conf) = self.extract::<JaegerConfig>(py) {
            Ok(Box::new(jaeger_conf))
        } else {
            let pytype = self.as_ref(py).get_type();
            Err(format!("Unknown tracing_config type: {pytype}"))
        }
    }
}

/// Utility class used to handle tracing.
///
/// It keeps a tokio runtime that is alive as long as the struct itself.
#[pyclass]
pub struct BytewaxTracer {
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

impl BytewaxTracer {
    pub fn new() -> Self {
        let rt = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .unwrap();
        Self { rt }
    }

    /// Call this with a TracingConfig subclass to configure tracing.
    /// Returns a guard that you have to keep in scope for the
    /// whole execution of the code you want to trace.
    pub(crate) fn setup(
        &self,
        tracer: Option<Box<dyn TracerBuilder + Send>>,
        log_level: Option<String>,
    ) {
        // Prepare the log layer
        let log_level = get_log_level(log_level);

        // We need an async block to properly initialize the tracing runtime.
        self.rt
            .block_on(self.rt.spawn(async move {
                let logs = tracing_subscriber::fmt::Layer::default()
                    .compact()
                    // Show source file
                    .with_file(true)
                    // Display source code line numbers
                    .with_line_number(true)
                    // Display the thread ID an event was recorded on
                    .with_thread_ids(true)
                    .with_filter(Targets::new().with_target("bytewax", log_level));

                let tracer = tracer.map(|tracer| tracer.build().unwrap());
                let telemetry = tracer.map(|tracer| {
                    tracing_opentelemetry::layer()
                        .with_tracer(tracer)
                        // Send all traces from bytewax
                        .with_filter(Targets::new().with_target("bytewax", LevelFilter::TRACE))
                });

                // If the conf was not none, setup the global subscriber with both log and
                // telemetry layer, otherwise just setup logging.
                if let Some(telemetry) = telemetry {
                    tracing::subscriber::set_global_default(
                        Registry::default().with(logs).with(telemetry),
                    )
                    .unwrap();
                } else {
                    tracing::subscriber::set_global_default(Registry::default().with(logs))
                        .unwrap();
                };
            }))
            .unwrap();
    }
}

pub(crate) fn register(_py: Python, m: &PyModule) -> PyResult<()> {
    m.add_class::<TracingConfig>()?;
    m.add_class::<JaegerConfig>()?;
    m.add_class::<OtlpTracingConfig>()?;
    m.add_class::<BytewaxTracer>()?;
    Ok(())
}
