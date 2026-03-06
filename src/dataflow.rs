use pyo3::exceptions::PyTypeError;
use pyo3::prelude::*;
use pyo3::types::PyDict;

use crate::errors::PythonException;
use crate::pyo3_extensions::SafePy;
use crate::recovery::StepId;

pub(crate) struct Dataflow(SafePy<PyAny>);

/// Do some eager type checking.
impl<'py> FromPyObject<'_, 'py> for Dataflow {
    type Error = PyErr;
    fn extract(ob: Borrowed<'_, 'py, PyAny>) -> PyResult<Self> {
        let py = ob.py();
        let abc = py.import("bytewax.dataflow")?.getattr("Dataflow")?;
        if !ob.is_instance(&abc)? {
            Err(PyTypeError::new_err(
                "dataflow must subclass `bytewax.dataflow.Dataflow`",
            ))
        } else {
            Ok(Self(SafePy::from(ob.to_owned().unbind())))
        }
    }
}

impl<'py> IntoPyObject<'py> for Dataflow {
    type Target = PyAny;
    type Output = Bound<'py, PyAny>;
    type Error = std::convert::Infallible;
    fn into_pyobject(self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
        Ok(self.0.into_inner().into_bound(py))
    }
}

impl Dataflow {
    pub(crate) fn clone_ref(&self, py: Python) -> Self {
        Self(SafePy::from(self.0.clone_ref(py)))
    }

    pub(crate) fn substeps(&self, py: Python) -> PyResult<Vec<Operator>> {
        self.0.getattr(py, "substeps")?.extract(py)
    }
}

pub(crate) struct Operator(SafePy<PyAny>);

/// Do some eager type checking.
impl<'py> FromPyObject<'_, 'py> for Operator {
    type Error = PyErr;
    fn extract(ob: Borrowed<'_, 'py, PyAny>) -> PyResult<Self> {
        let py = ob.py();
        let abc = py.import("bytewax.dataflow")?.getattr("Operator")?;
        if !ob.is_instance(&abc)? {
            Err(PyTypeError::new_err(
                "operator must subclass `bytewax.dataflow.Operator`",
            ))
        } else {
            Ok(Self(SafePy::from(ob.to_owned().unbind())))
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) struct StreamId(String);

impl<'py> FromPyObject<'_, 'py> for StreamId {
    type Error = PyErr;
    fn extract(ob: Borrowed<'_, 'py, PyAny>) -> PyResult<Self> {
        Ok(Self(ob.extract()?))
    }
}

impl Operator {
    pub(crate) fn get_arg(&self, py: Python, attr_name: &str) -> PyResult<Py<PyAny>> {
        self.0.getattr(py, attr_name)
    }

    pub(crate) fn name(&self, py: Python) -> PyResult<String> {
        Ok(self.0.bind(py).get_type().qualname()?.to_string())
    }

    pub(crate) fn step_id(&self, py: Python) -> PyResult<StepId> {
        self.0.getattr(py, "step_id")?.extract(py)
    }

    pub(crate) fn substeps(&self, py: Python) -> PyResult<Vec<Operator>> {
        self.0.getattr(py, "substeps")?.extract(py)
    }

    pub(crate) fn is_core(&self, py: Python) -> PyResult<bool> {
        let core_cls = py.import("bytewax.dataflow")?.getattr("_CoreOperator")?;
        self.0.bind(py).is_instance(&core_cls)
    }

    pub(crate) fn get_port_stream(&self, py: Python, port_name: &str) -> PyResult<StreamId> {
        self.0
            .bind(py)
            .getattr(port_name)
            .reraise_with(|| format!("operator did not have Port {port_name:?}"))?
            .getattr("stream_id")?
            .extract()
    }

    pub(crate) fn get_multiport_streams(
        &self,
        py: Python,
        port_name: &str,
    ) -> PyResult<Vec<StreamId>> {
        let binding = self
            .0
            .bind(py)
            .getattr(port_name)
            .reraise_with(|| format!("operator did not have MultiPort {port_name:?}"))?
            .getattr("stream_ids")?;
        let stream_ids = binding.cast::<PyDict>()?;
        stream_ids.values().extract()
    }
}
