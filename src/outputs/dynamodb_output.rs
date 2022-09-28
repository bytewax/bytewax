use std::collections::HashMap;

use crate::py_unwrap;
use crate::pyo3_extensions::TdPyAny;
use anyhow::{anyhow, Result};

use pyo3::{
    exceptions::{PyTypeError, PyValueError},
    prelude::*,
    types::{PyBool, PyBytes, PyDict, PyFloat, PyList, PyLong, PyUnicode},
};

use tokio::runtime::Runtime;

use aws_sdk_dynamodb::model::AttributeValue;
use aws_sdk_dynamodb::types::Blob;
use aws_sdk_dynamodb::Client;

use super::{OutputConfig, OutputWriter};

/// Use [DynamoDB](https://aws.amazon.com/dynamodb/) as the output.
///
/// Output to DynamoDB must be structured as pairs of (key, dict) where
/// `key` is to be used as the primary_key in the output DynamoDB table, and
/// `dict` is a dictionary containing key, value pairs where `key` is a string,
/// and `value` is one of the following types supported by DynamoDB:
/// - None
/// - String
/// - Bytes
/// - Bool
/// - Float
/// - List (containing the types in this list)
/// - Dict (where keys are strings, and values are types in this list)
///
/// Writes to DynamoDB must be authenticated using AWS credentials.
/// For more information on specifying your AWS credentials and configuring
/// a region, see https://docs.aws.amazon.com/sdk-for-rust/latest/dg/credentials.html
///
/// Args:
///
///   table (str): Name of the table to write records to.
///   primary_key (str): Name of the DynamoDB field to store the key in.
///
/// Returns:
///
///   Config object. Pass this as an argument to `bytewax.dataflow.Dataflow.capture`.
#[pyclass(module = "bytewax.outputs", extends = OutputConfig)]
#[pyo3(text_signature = "(table, primary_key)")]
pub(crate) struct DynamoDBOutputConfig {
    #[pyo3(get)]
    pub(crate) table: String,
    #[pyo3(get)]
    pub(crate) primary_key: String,
}

#[pymethods]
impl DynamoDBOutputConfig {
    #[new]
    #[args(table, primary_key)]
    fn new(table: String, primary_key: String) -> (Self, OutputConfig) {
        (Self { table, primary_key }, OutputConfig {})
    }

    fn __getstate__(&self) -> (&str, String, String) {
        (
            "DynamoDBOutputConfig",
            self.table.clone(),
            self.primary_key.clone(),
        )
    }

    /// Egregious hack see [`SqliteRecoveryConfig::__getnewargs__`].
    fn __getnewargs__(&self) -> (Vec<String>, &str) {
        let s = "UNINIT_PICKLED_STRING";
        (Vec::new(), s)
    }

    /// Unpickle from tuple of arguments.
    fn __setstate__(&mut self, state: &PyAny) -> PyResult<()> {
        if let Ok(("DynamoDBOutputConfig", table, primary_key)) = state.extract() {
            self.table = table;
            self.primary_key = primary_key;
            Ok(())
        } else {
            Err(PyValueError::new_err(format!(
                "bad pickle contents for DynamoDBOutputConfig: {state:?}"
            )))
        }
    }
}

/// Produce output to DynamoDB
pub(crate) struct DynamoDBOutput {
    client: Client,
    table: String,
    primary_key: String,
    rt: Runtime,
}

impl DynamoDBOutput {
    pub(crate) fn new(table: String, primary_key: String) -> Self {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();

        let region_provider = aws_config::default_provider::region::default_provider();
        let shared_config = rt.block_on(aws_config::from_env().region(region_provider).load());

        let client = Client::new(&shared_config);
        Self {
            client,
            table,
            primary_key,
            rt,
        }
    }
}

// Create an [AttributeValue](aws_sdk_dynamodb::model::AttributeValue)
// from the user-supplied Python type for insertion as a DynamoDB
// value.
fn create_dynamodb_attribute(val: &PyAny) -> Result<AttributeValue> {
    // Python boolean values can be cast to a number,
    // so make sure Bool comes before PyLong.
    if val.is_none() {
        Ok(AttributeValue::Null(true))
    } else if let Ok(b) = val.downcast::<PyBool>() {
        Ok(AttributeValue::Bool(b.is_true()))
    } else if let Ok(num) = val.downcast::<PyLong>() {
        Ok(AttributeValue::N(num.to_string()))
    } else if let Ok(num) = val.downcast::<PyFloat>() {
        Ok(AttributeValue::N(num.to_string()))
    } else if let Ok(s) = val.downcast::<PyUnicode>() {
        Ok(AttributeValue::S(s.to_string()))
    } else if let Ok(bytes) = val.downcast::<PyBytes>() {
        Ok(AttributeValue::B(Blob::new(bytes.as_bytes())))
    } else if let Ok(list) = val.downcast::<PyList>() {
        let attr_list: Result<Vec<AttributeValue>> =
            list.into_iter().map(create_dynamodb_attribute).collect();
        Ok(AttributeValue::L(attr_list.unwrap()))
    } else if let Ok(m) = val.downcast::<PyDict>() {
        let attribute_map: HashMap<String, AttributeValue> = m
            .into_iter()
            .map(|(k, v)| (k.to_string(), create_dynamodb_attribute(v).unwrap()))
            .collect();
        Ok(AttributeValue::M(attribute_map))
    } else {
        Err(anyhow!("Could not convert `{val:?}` into a DynamoDB type."))
    }
}

// impl<'a> FromPyObject<'a> for &'a AttributeValue {
//     fn extract(obj: &'a PyAny) -> PyResult<Self> {
//         Ok(<PyBytes as PyTryFrom>::try_from(obj)?.as_bytes())
//     }
// }

impl OutputWriter<u64, TdPyAny> for DynamoDBOutput {
    fn push(&mut self, _epoch: u64, key_payload: TdPyAny) {
        Python::with_gil(|py| {
            let (key, payload): (Option<TdPyAny>, TdPyAny) = py_unwrap!(
                key_payload.extract(py),
                format!(
                    "DynamoDBOutput requires a `(key, payload)` 2-tuple; \
                    got `{key_payload:?}` instead"
                )
            );
            let p: &PyDict = py_unwrap!(
                payload.extract(py),
                format!(
                    "DynamoDBOutput requires that the payload be a Dictionary; got \
                    `{payload:?}` instead"
                )
            );

            // Create the DynamoDB PutItem
            let mut request = self.client.put_item().table_name(&self.table);

            for (k, v) in p {
                let key: String = k
                    .extract()
                    .expect("Keys must be formatted as Strings for Dynamodb. Got `{k:?}`");
                let attr = create_dynamodb_attribute(&v).unwrap();
                request = request.item(key, attr);
            }

            // Add the key to the request as the primary key for the table
            if let Some(key) = key {
                log::debug!("Setting primary key to {key:?}");
                let attr = AttributeValue::S(key.to_string());
                request = request.item(&self.primary_key, attr);
            }

            log::debug!("Finished building request: {request:?}");

            // The DynamoDB crate uses the rust Tracing crate, which
            // deadlocks here with pyo3-log unless we release the GIL.
            py.allow_threads(|| {
                self.rt.block_on(request.send()).unwrap();
            });
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    proptest! {
        #[test]
        fn test_dynamodb_64_numbers(n: u64) {
            pyo3::prepare_freethreaded_python();
            Python::with_gil(|py| {
                let n = n.to_object(py).into_ref(py);
                assert_eq!(create_dynamodb_attribute(n).unwrap(), AttributeValue::N(n.to_string()));
            });
        }

        #[test]
        fn test_dynamodb_bool(b: bool) {
            pyo3::prepare_freethreaded_python();
            Python::with_gil(|py| {
                let py_b = PyBool::new(py, b).downcast::<PyAny>().unwrap();
                assert_eq!(create_dynamodb_attribute(py_b).unwrap(), AttributeValue::Bool(b));
            });
        }

        #[test]
        // Bounding floats here as maximum values don't convert properly
        fn test_dynamodb_float(p in 0.0..1000000f64, n in -1000000f64..0.0) {
            prop_assume!(n != 0.0); // Converting 0.0 results in 0
            prop_assume!(p != 0.0);
            pyo3::prepare_freethreaded_python();
            Python::with_gil(|py| {
                let f1 = PyFloat::new(py, p).downcast::<PyAny>().unwrap();
                assert_eq!(create_dynamodb_attribute(f1).unwrap(), AttributeValue::N(p.to_string()));
                let f2 = PyFloat::new(py, n).downcast::<PyAny>().unwrap();
                assert_eq!(create_dynamodb_attribute(f2).unwrap(), AttributeValue::N(n.to_string()));
            });
        }

        #[test]
        fn test_dynamodb_strings(s: String) {
            pyo3::prepare_freethreaded_python();
            Python::with_gil(|py| {
                let u = PyUnicode::new(py, &s).downcast::<PyAny>().unwrap();
                assert_eq!(create_dynamodb_attribute(u).unwrap(), AttributeValue::S(s.to_string()));
            });
        }

        #[test]
        fn test_dynamodb_bytes(s: String) {
            pyo3::prepare_freethreaded_python();
            Python::with_gil(|py| {
                let bytes = PyBytes::new(py, s.as_bytes())
                    .downcast::<PyAny>()
                    .unwrap();
                assert_eq!(
                    create_dynamodb_attribute(bytes).unwrap(),
                    AttributeValue::B(Blob::new(s.as_bytes()))
                );
            });
        }

        #[test]
        fn test_dynamodb_array(n: u32) {
            pyo3::prepare_freethreaded_python();
            Python::with_gil(|py| {
                let v = vec![n].into_py(py);
                let r = v.as_ref(py).downcast::<PyAny>().unwrap();
                assert_eq!(
                    create_dynamodb_attribute(r).unwrap(),
                    AttributeValue::L(vec![AttributeValue::N(n.to_string())])
                );
            });
        }

        #[test]
        fn test_dynamodb_map(s: String, n: u32) {
            pyo3::prepare_freethreaded_python();
            Python::with_gil(|py| {
                let h = HashMap::from([(&s, n)]).into_py(py);
                let r = h.as_ref(py).downcast::<PyAny>().unwrap();
                assert_eq!(
                    create_dynamodb_attribute(r).unwrap(),
                    AttributeValue::M(HashMap::from([(
                        s.to_string(),
                        AttributeValue::N(n.to_string())
                    )]))
                )
            });
        }
    }
    #[test]
    fn test_dynamodb_null() {
        pyo3::prepare_freethreaded_python();
        Python::with_gil(|py| {
            let n = py.None();
            let r = n.as_ref(py).downcast::<PyAny>().unwrap();
            assert_eq!(
                create_dynamodb_attribute(r).unwrap(),
                AttributeValue::Null(true)
            );
        });
    }
}
