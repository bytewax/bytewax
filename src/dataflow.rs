use pyo3::exceptions::PyTypeError;
use pyo3::prelude::*;
use pyo3::types::PyDict;

use crate::errors::PythonException;
use crate::recovery::StepId;

pub(crate) struct Dataflow(PyObject);

/// Do some eager type checking.
impl<'source> FromPyObject<'source> for Dataflow {
    fn extract(ob: &'source PyAny) -> PyResult<Self> {
        let abc = ob
            .py()
            .import("bytewax.dataflow")?
            .getattr("Dataflow")?
            .extract()?;
        if !ob.is_instance(abc)? {
            Err(PyTypeError::new_err(
                "dataflow must subclass `bytewax.dataflow.Dataflow`",
            ))
        } else {
            Ok(Self(ob.into()))
        }
    }
}

impl IntoPy<Py<PyAny>> for Dataflow {
    fn into_py(self, _py: Python<'_>) -> Py<PyAny> {
        self.0
    }
}

impl Dataflow {
    pub(crate) fn clone_ref(&self, py: Python) -> Self {
        Self(self.0.clone_ref(py))
    }

    pub(crate) fn substeps(&self, py: Python) -> PyResult<Vec<Operator>> {
        self.0.getattr(py, "substeps")?.extract(py)
    }
}

pub(crate) struct Operator(PyObject);

/// Do some eager type checking.
impl<'source> FromPyObject<'source> for Operator {
    fn extract(ob: &'source PyAny) -> PyResult<Self> {
        let abc = ob
            .py()
            .import("bytewax.dataflow")?
            .getattr("Operator")?
            .extract()?;
        if !ob.is_instance(abc)? {
            Err(PyTypeError::new_err(
                "operator must subclass `bytewax.dataflow.Operator`",
            ))
        } else {
            Ok(Self(ob.into()))
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) struct StreamId(String);

impl<'source> FromPyObject<'source> for StreamId {
    fn extract(ob: &'source PyAny) -> PyResult<Self> {
        Ok(Self(ob.extract()?))
    }
}

impl Operator {
    pub(crate) fn get_arg(&self, py: Python, attr_name: &str) -> PyResult<PyObject> {
        self.0.getattr(py, attr_name)
    }

    pub(crate) fn name(&self, py: Python) -> PyResult<String> {
        Ok(self.0.as_ref(py).get_type().name()?.to_owned())
    }

    pub(crate) fn step_id(&self, py: Python) -> PyResult<StepId> {
        self.0.getattr(py, "step_id")?.extract(py)
    }

    pub(crate) fn substeps(&self, py: Python) -> PyResult<Vec<Operator>> {
        self.0.getattr(py, "substeps")?.extract(py)
    }

    pub(crate) fn is_core(&self, py: Python) -> PyResult<bool> {
        let core_cls = py
            .import("bytewax.dataflow")?
            .getattr("_CoreOperator")?
            .extract()?;
        self.0.as_ref(py).is_instance(core_cls)
    }

    pub(crate) fn get_port_stream(&self, py: Python, port_name: &str) -> PyResult<StreamId> {
        self.0
            .as_ref(py)
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
        let stream_ids = self
            .0
            .as_ref(py)
            .getattr(port_name)
            .reraise_with(|| format!("operator did not have MultiPort {port_name:?}"))?
            .getattr("stream_ids")?
            .extract::<&PyDict>()?;
        stream_ids.values().extract()
    }
}
