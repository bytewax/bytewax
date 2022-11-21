use std::collections::HashMap;

use opentelemetry::{
    runtime::Tokio,
    sdk::{
        trace::{config, Sampler, Tracer},
        Resource,
    },
    KeyValue,
};
use opentelemetry_otlp::WithExportConfig;
use pyo3::{prelude::*, types::PyDict};

use crate::common::{pickle_extract, StringResult};

use super::{TracerBuilder, TracingConfig};

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
#[pyo3(text_signature = "(service_name, url, sampling_ratio=1.0)")]
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
    pub(crate) sampling_ratio: Option<f64>,
}

impl TracerBuilder for OtlpTracingConfig {
    fn build(&self) -> StringResult<Tracer> {
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
                    .with_sampler(Sampler::TraceIdRatioBased(
                        self.sampling_ratio.unwrap_or(1.0),
                    ))
                    .with_resource(Resource::new(vec![KeyValue::new(
                        "service.name",
                        self.service_name.clone(),
                    )])),
            )
            .install_batch(Tokio)
            .map_err(|err| format!("Error installing tracer: {err}"))
    }
}

#[pymethods]
impl OtlpTracingConfig {
    #[new]
    #[args(service_name, url, sampling_ratio = "None")]
    pub(crate) fn py_new(
        service_name: String,
        url: Option<String>,
        sampling_ratio: Option<f64>,
    ) -> (Self, TracingConfig) {
        (
            Self {
                service_name,
                url,
                sampling_ratio,
            },
            TracingConfig {},
        )
    }

    /// Return a representation of this class as a PyDict.
    fn __getstate__(&self) -> HashMap<&str, Py<PyAny>> {
        Python::with_gil(|py| {
            HashMap::from([
                ("type", "OtlpTracingConfig".into_py(py)),
                ("service_name", self.service_name.clone().into_py(py)),
                ("url", self.url.clone().into_py(py)),
                ("sampling_ratio", self.sampling_ratio.into_py(py)),
            ])
        })
    }

    /// Egregious hack see [`SqliteRecoveryConfig::__getnewargs__`].
    fn __getnewargs__(&self) -> (String, Option<String>, Option<f64>) {
        (String::new(), None, None)
    }

    /// Unpickle from tuple of arguments.
    fn __setstate__(&mut self, state: &PyAny) -> PyResult<()> {
        let dict: &PyDict = state.downcast()?;
        self.service_name = pickle_extract(dict, "service_name")?;
        self.url = pickle_extract(dict, "url")?;
        self.sampling_ratio = pickle_extract(dict, "sampling_ratio")?;
        Ok(())
    }
}
