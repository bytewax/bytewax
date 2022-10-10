use opentelemetry::{
    runtime::Tokio,
    sdk::{trace::config, Resource},
    KeyValue,
};
use opentelemetry_otlp::WithExportConfig;
use pyo3::{exceptions::PyValueError, pyclass, pymethods, PyAny, PyResult};
use tracing_subscriber::{layer::SubscriberExt, EnvFilter, Layer, Registry};

use super::{log_layer, TracingConfig, TracingSetupError};

/// Send traces to the opentelemetry collector:
/// https://opentelemetry.io/docs/collector/
///
/// This is the recommended approach since it allows
/// the maximum flexibility in what to do with all the data
/// bytewax can generate.
#[pyclass(module="bytewax.tracing", extends=TracingConfig)]
#[pyo3(text_signature = "(service_name, url, protocol)")]
#[derive(Clone)]
pub(crate) struct OltpTracingConfig {
    /// Service name, identifies this dataflow.
    #[pyo3(get)]
    pub(crate) service_name: String,
    /// Optional collector's URL
    #[pyo3(get)]
    pub(crate) url: Option<String>,
    /// Optional protocol. This can be either:
    /// - "GRPC" (default)
    /// - "HTTP"
    #[pyo3(get)]
    pub(crate) protocol: Option<String>,
}

impl OltpTracingConfig {
    pub(crate) fn setup(self) -> Result<(), TracingSetupError> {
        // Instantiate the builder
        let mut exporter = opentelemetry_otlp::new_exporter().tonic();

        // Change the protocol if required
        if let Some(protocol) = self.protocol {
            exporter = match protocol.as_str() {
                "HTTP" => exporter.with_protocol(opentelemetry_otlp::Protocol::HttpBinary),
                "GRPC" => exporter.with_protocol(opentelemetry_otlp::Protocol::Grpc),
                val => panic!("Unknown protocol: {val}. Only 'GRPC' and 'HTTP' allowed"),
            }
        }

        // Change the url if required
        if let Some(endpoint) = self.url {
            exporter = exporter.with_endpoint(endpoint);
        }

        // Create the tracer
        let tracer = opentelemetry_otlp::new_pipeline()
            .tracing()
            .with_exporter(exporter)
            .with_trace_config(config().with_resource(Resource::new(vec![KeyValue::new(
                "service.name",
                self.service_name,
            )])))
            .install_batch(Tokio)
            .map_err(|err| TracingSetupError::InitRuntime(err.to_string()))?;

        let telemetry = tracing_opentelemetry::layer()
            .with_tracer(tracer)
            // By default we trace everything in bytewax, and only errors
            // coming from other libraries we use.
            .with_filter(EnvFilter::new("bytewax=trace,error"));

        let subscriber = Registry::default()
            .with(telemetry)
            // Add stdout logs anyway
            .with(log_layer());
        tracing::subscriber::set_global_default(subscriber)
            .map_err(|err| TracingSetupError::Init(err.to_string()))
    }
}

#[pymethods]
impl OltpTracingConfig {
    #[new]
    #[args(service_name, url ,protocol)]
    pub(crate) fn py_new(
        service_name: String,
        url: Option<String>,
        protocol: Option<String>,
    ) -> (Self, TracingConfig) {
        (
            Self {
                service_name,
                url,
                protocol,
            },
            TracingConfig {},
        )
    }

    /// Pickle as a tuple.
    fn __getstate__(&self) -> (&str, String, Option<String>, Option<String>) {
        (
            "OltpTracingConfig",
            self.service_name.clone(),
            self.url.clone(),
            self.protocol.clone(),
        )
    }

    /// Egregious hack see [`SqliteRecoveryConfig::__getnewargs__`].
    fn __getnewargs__(&self) -> (String, Option<String>, Option<String>) {
        (String::new(), None, None)
    }

    /// Unpickle from tuple of arguments.
    fn __setstate__(&mut self, state: &PyAny) -> PyResult<()> {
        if let Ok(("OltpTracingConfig", service_name, url, protocol)) = state.extract() {
            self.service_name = service_name;
            self.url = url;
            self.protocol = protocol;
            Ok(())
        } else {
            Err(PyValueError::new_err(format!(
                "bad pickle contents for OltpTracingConfig: {state:?}"
            )))
        }
    }
}
