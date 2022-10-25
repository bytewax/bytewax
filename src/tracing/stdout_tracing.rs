use pyo3::{pyclass, pymethods};
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

use super::{log_layer, TracerBuilder, TracingConfig, TracingSetupError};

/// This is the default tracing config, sends traces to stdout.
///
/// The output can be configured with an env var: "BYTEWAX_LOG".
///
/// See tracing-subscriber's documentation:
/// https://docs.rs/tracing-subscriber/latest/tracing_subscriber/filter/struct.EnvFilter.html
///
/// eg: to set bytewax's logs to the "debug" level, and all other packages
/// to the "error" level, you can run the dataflow like this:
///     $ BYTEWAX_LOG="bytewax=debug,error" python dataflow.py
#[pyclass(module="bytewax.tracing", extends=TracingConfig)]
#[pyo3(text_signature = "()")]
#[derive(Clone)]
pub(crate) struct StdOutTracingConfig {}

impl StdOutTracingConfig {
    pub(crate) fn new() -> Self {
        Self {}
    }
}

impl TracerBuilder for StdOutTracingConfig {
    fn setup(&self) -> Result<(), TracingSetupError> {
        let fmt = log_layer();
        tracing_subscriber::registry()
            .with(fmt)
            .try_init()
            .map_err(|err| TracingSetupError::Init(err.to_string()))
    }
}

#[pymethods]
impl StdOutTracingConfig {
    #[new]
    pub(crate) fn py_new() -> (Self, TracingConfig) {
        (Self::new(), TracingConfig {})
    }
}
