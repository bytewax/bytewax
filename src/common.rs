use pyo3::{exceptions::PyValueError, prelude::*, types::PyDict};

/// Extracts a field from a `PyDict` during unpickling
pub(crate) fn pickle_extract<'a, D>(dict: &'a PyDict, key: &str) -> PyResult<D>
where
    D: FromPyObject<'a>,
{
    dict.get_item(key)
        .ok_or_else(|| PyValueError::new_err(format!("bad pickle contents for {}: {}", key, dict)))?
        .extract()
}

// Extract a field of a Dataflow `Step` from a PyDict
pub(crate) fn step_extract<'a, D>(dict: &'a PyDict, key: &str) -> PyResult<D>
where
    D: FromPyObject<'a>,
{
    dict.get_item(key)
        .expect("Unable to extract step, missing `{key}` field.")
        .extract()
}

/// Result type used in the crate that holds a String as the Err type.
pub(crate) type StringResult<T> = Result<T, String>;
