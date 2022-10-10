//! Internal code for tracing.
//! TODO
use std::fmt::Display;

use pyo3::{
    exceptions::PyValueError, pyclass, pymethods, types::PyModule, Py, PyAny, PyCell, PyResult,
    Python,
};
use tokio::runtime::EnterGuard;
use tracing_subscriber::{registry::LookupSpan, EnvFilter, Layer};

pub(crate) mod jaeger_tracing;
pub(crate) mod oltp_tracing;
pub(crate) mod stdout_tracing;

pub(crate) use jaeger_tracing::JaegerConfig;
pub(crate) use oltp_tracing::OltpTracingConfig;
pub(crate) use stdout_tracing::StdOutTracingConfig;

/// Possible errors while setting up tracing
#[derive(Debug)]
pub enum TracingSetupError {
    /// Error while initializing the tracing subscriber
    Init(String),
    /// Error while initializing the tracing runtime
    InitRuntime(String),
}

impl Display for TracingSetupError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            TracingSetupError::Init(msg) => write!(f, "Error initializing tracing: {}", msg),
            TracingSetupError::InitRuntime(msg) => {
                write!(f, "Error initializing tracing runtime: {}", msg)
            }
        }
    }
}

impl std::error::Error for TracingSetupError {}

/// Tracing layer that logs to stdout, filtered by an environment variable.
pub(crate) fn log_layer<S>() -> impl Layer<S>
where
    S: tracing::Subscriber + for<'span> LookupSpan<'span>,
{
    // TODO: Do we want to offer customization here?
    tracing_subscriber::fmt::Layer::default()
        .compact()
        // Show source file
        .with_file(true)
        // Display source code line numbers
        .with_line_number(true)
        // Display the thread ID an event was recorded on
        .with_thread_ids(true)
        // Don't display the event's target (module path)
        .with_target(false)
        .with_filter(EnvFilter::from_env("BYTEWAX_LOG"))
}

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

/// Utility class used to handle tracing.
///
/// It keeps a tokio runtime that is alive as long as the struct itself.
pub(crate) struct BytewaxTracer {
    rt: tokio::runtime::Runtime,
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
    pub fn setup<'a>(&'a self, py_conf: Py<TracingConfig>) -> EnterGuard<'a> {
        let guard = self.rt.enter();

        // We need an async block to properly initialize the tracing runtime.
        let initializer = async move {
            Python::with_gil(|py| {
                if let Ok(oltp_conf) = py_conf.extract::<OltpTracingConfig>(py) {
                    // Try to setup tracing, but if it fails setup stdout and log the error.
                    if let Err(err) = oltp_conf.setup() {
                        let conf = StdOutTracingConfig::new();
                        // This can fail if tracing was already initialized, which can happen
                        // if the user runs the same dataflow multiple times.
                        // TODO: Right now this happens in execution tests, so I'm not
                        //       unwrapping, but this solution is not ideal.
                        _ = conf.setup();
                        tracing::error!("{err}");
                    }
                } else if let Ok(jaeger_conf) = py_conf.extract::<JaegerConfig>(py) {
                    // Try to setup tracing, but if it fails setup stdout and log the error.
                    if let Err(err) = jaeger_conf.setup() {
                        let conf = StdOutTracingConfig::new();
                        // This can fail if tracing was already initialized, which can happen
                        // if the user runs the same dataflow multiple times.
                        // TODO: Right now this happens in execution tests, so I'm not
                        //       unwrapping, but this solution is not ideal.
                        _ = conf.setup();
                        tracing::error!("{err}");
                    }
                } else if let Ok(stdout_conf) = py_conf.extract::<StdOutTracingConfig>(py) {
                    // This can fail if tracing was already initialized, which can happen
                    // if the user runs the same dataflow multiple times.
                    // TODO: Right now this happens in execution tests, so I'm not
                    //       unwrapping, but this solution is not ideal.
                    _ = stdout_conf.setup();
                } else {
                    panic!("Unrecognized tracing config: {py_conf:?}");
                }
            });
        };
        self.rt.block_on(self.rt.spawn(initializer)).unwrap();
        guard
    }
}

pub(crate) fn register(_py: Python, m: &PyModule) -> PyResult<()> {
    m.add_class::<TracingConfig>()?;
    m.add_class::<JaegerConfig>()?;
    m.add_class::<StdOutTracingConfig>()?;
    m.add_class::<OltpTracingConfig>()?;
    Ok(())
}
