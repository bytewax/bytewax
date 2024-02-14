use opentelemetry::{
    global,
    sdk::metrics::{Aggregation, Instrument, MeterProvider, Stream},
};
use prometheus::{
    default_registry, histogram_opts, opts, register_counter_vec, register_gauge_vec,
    register_histogram_vec, CounterVec, GaugeVec, HistogramVec, DEFAULT_BUCKETS,
};
use pyo3::prelude::*;
use pyo3::{exceptions::PyRuntimeError, PyErr, PyResult};
use std::collections::HashMap;

use crate::errors::PythonException;

#[macro_export]
macro_rules! with_timer {
    ($histogram: expr, $labels: expr, $body: expr) => {{
        let now = std::time::Instant::now();
        let res = $body;
        $histogram.record(now.elapsed().as_secs_f64(), &$labels);
        res
    }};
}

impl<T> PythonException<T> for Result<T, prometheus::Error> {
    fn into_pyresult(self) -> PyResult<T> {
        self.map_err(|err| PyErr::new::<PyRuntimeError, _>(err.to_string()))
    }
}

/// Wrapper class for a Prometheus Counter that can be called from Python.
///
/// :arg name: Name for the meter, will automatically be prefixed with `bytewax_`.
///
/// :type name: str
///
/// :arg description: Description text to be used for the meter.
///
/// :type description: str
///
/// :arg labels: A list of of labels be used when recording values.
///
/// :type labels: List[str]
#[pyclass(module = "bytewax.metrics")]
pub(crate) struct Counter(CounterVec);

#[pymethods]
impl Counter {
    #[new]
    fn new(name: String, description: String, labels: Vec<&str>) -> PyResult<Self> {
        let opts = opts!(format!("bytewax_{name}"), description);
        let counter_vec = register_counter_vec!(opts, &labels)
            .raise::<PyRuntimeError>("Error creating Counter")?;

        Ok(Self(counter_vec))
    }

    fn add(&self, labels: HashMap<&str, &str>) {
        self.0.with(&labels).inc();
    }
}

/// Wrapper class for a Prometheus Gauge that can be called from Python.
///
/// :arg name: Name for the meter, will automatically be prefixed with `bytewax_`.
///
/// :type name: str
///
/// :arg description: Description text to be used for the meter.
///
/// :type description: str
///
/// :arg labels: A list of of labels be used when recording values.
///
/// :type labels: List[str]
#[pyclass(module = "bytewax.metrics")]
pub(crate) struct Gauge(GaugeVec);

#[pymethods]
impl Gauge {
    #[new]
    fn new(name: String, description: String, labels: Vec<&str>) -> PyResult<Self> {
        let opts = opts!(format!("bytewax_{name}"), description);
        let gauge_vec =
            register_gauge_vec!(opts, &labels).raise::<PyRuntimeError>("Error creating Gauge")?;

        Ok(Self(gauge_vec))
    }

    // Can't call this method set, as it conflicts with Python's built-in set method.
    fn set_val(&self, val: f64, labels: HashMap<&str, &str>) {
        self.0.with(&labels).set(val);
    }
}

/// Wrapper class for a Prometheus Histogram that can be called from Python.
///
/// :arg name: Name for the histogram, will automatically be prefixed with `bytewax_`.
///
/// :type name: str
///
/// :arg description: Description text to be used for the histogram.
///
/// :type description: str
///
/// :arg labels: A list of labels to be used when recording values.
///
/// :type description: List[str]
///
/// :arg labels: A list of labels be used when recording values.
///
/// :type labels: List[str]
#[pyclass(module = "bytewax.metrics")]
pub(crate) struct Histogram(HistogramVec);

#[pymethods]
impl Histogram {
    #[new]
    fn new(
        name: String,
        description: String,
        labels: Vec<&str>,
        buckets: Option<Vec<f64>>,
    ) -> PyResult<Self> {
        let buckets = buckets.unwrap_or_else(|| Vec::from(DEFAULT_BUCKETS));
        let histogram_opts = histogram_opts!(format!("bytewax_{name}"), description, buckets);
        let histogram_vec = register_histogram_vec!(histogram_opts, &labels)
            .raise::<PyRuntimeError>("Error creating Histogram")?;

        Ok(Self(histogram_vec))
    }

    fn observe(&self, val: f64, labels: HashMap<&str, &str>) {
        self.0.with(&labels).observe(val);
    }
}

/// Initialize the global registry for Prometheus metrics,
/// and create a global MeterProvider.
pub(crate) fn initialize_metrics() -> PyResult<()> {
    // Initialize the global default registry for prometheus metrics
    // as internally it's a lazy static.
    let registry = default_registry();
    let exporter = opentelemetry_prometheus::exporter()
        .with_registry(registry.clone())
        .with_namespace("bytewax")
        .build()
        .map_err(|err| PyErr::new::<PyRuntimeError, _>(err.to_string()))?;

    // Create a global MeterProvider
    let provider = MeterProvider::builder()
        .with_reader(exporter)
        .with_view(
            opentelemetry_sdk::metrics::new_view(
                Instrument::new().name("*duration*"), // Must match histogram name
                Stream::new().aggregation(Aggregation::ExplicitBucketHistogram {
                    boundaries: vec![
                        0.0, 0.0005, 0.005, 0.01, 0.025, 0.05, 0.075, 0.1, 0.25, 0.5, 0.75, 1.0,
                        2.5, 5.0, 7.5, 10.0,
                    ],
                    record_min_max: true,
                }),
            )
            .map_err(|err| PyErr::new::<PyRuntimeError, _>(err.to_string()))?,
        )
        .build();
    global::set_meter_provider(provider);
    Ok(())
}

pub(crate) fn register(_py: Python, m: &PyModule) -> PyResult<()> {
    m.add_class::<Counter>()?;
    m.add_class::<Gauge>()?;
    m.add_class::<Histogram>()?;
    Ok(())
}
