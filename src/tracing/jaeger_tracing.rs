use opentelemetry::{
    runtime::Tokio,
    sdk::trace::{config, Sampler, Tracer},
};
use pyo3::{exceptions::PyValueError, pyclass, pymethods, PyAny, PyResult};

use super::{TracerBuilder, TracingConfig};

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
#[pyo3(text_signature = "(service_name, endpoint, sampling_ratio=1.0)")]
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
    pub(crate) sampling_ratio: Option<f64>,
}

impl TracerBuilder for JaegerConfig {
    fn build(&self) -> Tracer {
        opentelemetry::global::set_text_map_propagator(opentelemetry_jaeger::Propagator::new());
        let mut tracer = opentelemetry_jaeger::new_agent_pipeline()
            .with_trace_config(config().with_sampler(Sampler::TraceIdRatioBased(
                self.sampling_ratio.unwrap_or(1.0),
            )))
            .with_service_name(self.service_name.clone());

        // Overwrite the endpoint if needed
        if let Some(endpoint) = self.endpoint.as_ref() {
            tracer = tracer.with_endpoint(endpoint);
        }

        tracer.install_batch(Tokio).unwrap()
    }
}

#[pymethods]
impl JaegerConfig {
    #[new]
    #[args(service_name, endpoint, sampling_ratio = "None")]
    pub(crate) fn py_new(
        service_name: String,
        endpoint: Option<String>,
        sampling_ratio: Option<f64>,
    ) -> (Self, TracingConfig) {
        (
            Self {
                service_name,
                endpoint,
                sampling_ratio,
            },
            TracingConfig {},
        )
    }

    /// Pickle as a tuple.
    fn __getstate__(&self) -> (&str, String, Option<String>, Option<f64>) {
        (
            "JaegerConfig",
            self.service_name.clone(),
            self.endpoint.clone(),
            self.sampling_ratio,
        )
    }

    /// Egregious hack see [`SqliteRecoveryConfig::__getnewargs__`].
    fn __getnewargs__(&self) -> (String, Option<String>, Option<f64>) {
        (String::new(), None, None)
    }

    /// Unpickle from tuple of arguments.
    fn __setstate__(&mut self, state: &PyAny) -> PyResult<()> {
        if let Ok(("JaegerConfig", service_name, endpoint, sampling_ratio)) = state.extract() {
            self.service_name = service_name;
            self.endpoint = endpoint;
            self.sampling_ratio = sampling_ratio;
            Ok(())
        } else {
            Err(PyValueError::new_err(format!(
                "bad pickle contents for JaegerConfig: {state:?}"
            )))
        }
    }
}
