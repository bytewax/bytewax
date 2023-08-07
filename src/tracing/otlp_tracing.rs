use opentelemetry::runtime::Tokio;
use opentelemetry::sdk::trace::config;
use opentelemetry::sdk::trace::Sampler;
use opentelemetry::sdk::trace::Tracer;
use opentelemetry::sdk::Resource;
use opentelemetry::KeyValue;
use opentelemetry_otlp::WithExportConfig;
use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;

use crate::errors::PythonException;

use super::TracerBuilder;
use super::TracingConfig;

/// Send traces to the opentelemetry collector:
/// https://opentelemetry.io/docs/collector/
///
/// Only supports GRPC protocol, so make sure to enable
/// it on your OTEL configuration.
///
/// This is the recommended approach since it allows
/// the maximum flexibility in what to do with all the data
/// bytewax can generate.
#[pyclass(module="bytewax.tracing", extends=TracingConfig)]
#[derive(Clone)]
pub(crate) struct OtlpTracingConfig {
    /// Service name, identifies this dataflow.
    #[pyo3(get)]
    pub(crate) service_name: String,
    /// Optional collector's URL, defaults to `grpc:://127.0.0.1:4317`
    #[pyo3(get)]
    pub(crate) url: Option<String>,
    /// Sampling ratio:
    ///   samplig_ratio >= 1 - all traces are sampled
    ///   samplig_ratio <= 0 - most traces are not sampled
    #[pyo3(get)]
    pub(crate) sampling_ratio: f64,
}

#[pymethods]
impl OtlpTracingConfig {
    #[new]
    #[pyo3(signature=(service_name, url=None, sampling_ratio=1.0))]
    fn new(
        service_name: String,
        url: Option<String>,
        sampling_ratio: f64,
    ) -> (Self, TracingConfig) {
        let self_ = Self {
            service_name,
            url,
            sampling_ratio,
        };
        let super_ = TracingConfig::new();
        (self_, super_)
    }
}

impl TracerBuilder for OtlpTracingConfig {
    fn build(&self) -> PyResult<Tracer> {
        // Instantiate the builder
        let mut exporter = opentelemetry_otlp::new_exporter().tonic();

        // Change the url if required
        if let Some(endpoint) = self.url.as_ref() {
            exporter = exporter.with_endpoint(endpoint);
        }

        // Create the tracer
        opentelemetry_otlp::new_pipeline()
            .tracing()
            .with_exporter(exporter)
            .with_trace_config(
                config()
                    .with_sampler(Sampler::TraceIdRatioBased(self.sampling_ratio))
                    .with_resource(Resource::new(vec![KeyValue::new(
                        "service.name",
                        self.service_name.clone(),
                    )])),
            )
            .install_batch(Tokio)
            .raise::<PyRuntimeError>("error installing tracer")
    }
}
