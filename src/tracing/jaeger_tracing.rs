use opentelemetry::runtime::Tokio;
use opentelemetry::sdk::trace::config;
use opentelemetry::sdk::trace::Sampler;
use opentelemetry::sdk::trace::Tracer;
use pyo3::prelude::*;

use super::TracerBuilder;
use super::TracingConfig;

/// Configure tracing to send traces to a Jaeger instance.
///
/// The endpoint can be configured with the parameter passed to this
/// config, or with two environment variables:
///
///   OTEL_EXPORTER_JAEGER_AGENT_HOST="127.0.0.1"
///   OTEL_EXPORTER_JAEGER_AGENT_PORT="6831"
///
/// Args:
///   service_name (str): Identifies this dataflow in Jaeger.
///
///   endpoint (Optional[str]): Connection info. Takes precidence over
///     env vars. Defaults to `"127.0.0.1:6831"`.
///
///   sampling_ratio (float): Fraction of traces to send between `0.0`
///     and `1.0`.
#[pyclass(module="bytewax.tracing", extends=TracingConfig)]
#[derive(Clone)]
pub(crate) struct JaegerConfig {
    #[pyo3(get)]
    service_name: String,
    #[pyo3(get)]
    endpoint: Option<String>,
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
