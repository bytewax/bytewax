use pyo3::{exceptions::PyValueError, prelude::*, types::PyDict};

/// Extracts a field from a `PyDict` during unpickling
pub(crate) fn pickle_extract<'a, D>(dict: &'a PyDict, key: &str) -> PyResult<D>
where
    D: FromPyObject<'a>,
{
    dict.get_item(key)
        .ok_or_else(|| PyValueError::new_err(format!("bad pickle contents for {key}: {dict}")))?
        .extract()
}
