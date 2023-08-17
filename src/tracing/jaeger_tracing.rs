use opentelemetry::runtime::Tokio;
use opentelemetry::sdk::trace::config;
use opentelemetry::sdk::trace::Sampler;
use opentelemetry::sdk::trace::Tracer;
use pyo3::prelude::*;

use super::TracerBuilder;
use super::TracingConfig;

/// Configure tracing to send traces to a Jaeger instance.
///
/// The endpoint can be configured with the parameter passed to this config,
/// or with two environment variables:
///
///   OTEL_EXPORTER_JAEGER_AGENT_HOST="127.0.0.1"
///   OTEL_EXPORTER_JAEGER_AGENT_PORT="6831"
///
/// By default the endpoint is set to "127.0.0.1:6831".
///
/// If the environment variables are set, the endpoint is changed to that.
///
/// If a config option is passed to JaegerConfig,
/// it takes precedence over env vars.
#[pyclass(module="bytewax.tracing", extends=TracingConfig)]
#[derive(Clone)]
pub(crate) struct JaegerConfig {
    /// Service name, identifies this dataflow.
    service_name: String,
    /// Optional Jaeger's URL
    endpoint: Option<String>,
    /// Sampling ratio:
    ///   samplig_ratio >= 1 - all traces are sampled
    ///   samplig_ratio <= 0 - most traces are not sampled
    #[pyo3(get)]
    pub(crate) sampling_ratio: f64,
}

#[pymethods]
impl JaegerConfig {
    #[new]
    #[pyo3(signature=(service_name, endpoint=None, sampling_ratio=1.0))]
    fn new(
        service_name: String,
        endpoint: Option<String>,
        sampling_ratio: f64,
    ) -> (Self, TracingConfig) {
        let self_ = Self {
            service_name,
            endpoint,
            sampling_ratio,
        };
        let super_ = TracingConfig::new();
        (self_, super_)
    }
}

impl TracerBuilder for JaegerConfig {
    fn build(&self) -> PyResult<Tracer> {
        opentelemetry::global::set_text_map_propagator(opentelemetry_jaeger::Propagator::new());
        let mut tracer = opentelemetry_jaeger::new_agent_pipeline()
            .with_trace_config(
                config().with_sampler(Sampler::TraceIdRatioBased(self.sampling_ratio)),
            )
            .with_service_name(self.service_name.clone());

        // Overwrite the endpoint if needed
        if let Some(endpoint) = self.endpoint.as_ref() {
            tracer = tracer.with_endpoint(endpoint);
        }

        Ok(tracer.install_batch(Tokio).unwrap())
    }
}
