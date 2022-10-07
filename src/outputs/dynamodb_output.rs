use crate::py_unwrap;
use crate::pyo3_extensions::TdPyAny;

use pyo3::{
    exceptions::{PyTypeError, PyValueError},
    prelude::*,
    types::{IntoPyDict, PyDict},
};

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
#[pyo3(text_signature = "(table)")]
pub(crate) struct DynamoDBOutputConfig {
    #[pyo3(get)]
    pub(crate) table: String,
}

#[pymethods]
impl DynamoDBOutputConfig {
    #[new]
    #[args(table)]
    fn new(table: String) -> (Self, OutputConfig) {
        (Self { table }, OutputConfig {})
    }

    fn __getstate__(&self) -> (&str, String) {
        ("DynamoDBOutputConfig", self.table.clone())
    }

    /// Egregious hack see [`SqliteRecoveryConfig::__getnewargs__`].
    fn __getnewargs__(&self) -> (Vec<String>, &str) {
        let s = "UNINIT_PICKLED_STRING";
        (Vec::new(), s)
    }

    /// Unpickle from tuple of arguments.
    fn __setstate__(&mut self, state: &PyAny) -> PyResult<()> {
        if let Ok(("DynamoDBOutputConfig", table)) = state.extract() {
            self.table = table;
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
    table: PyObject,
}

impl DynamoDBOutput {
    pub(crate) fn new(table: String) -> Self {
        let table = Python::with_gil(|py| {
            let boto = py.import("boto3").unwrap();
            let dynamodb = boto.call_method1("resource", ("dynamodb",)).unwrap();
            let table: PyObject = dynamodb.call_method1("Table", (&table,)).unwrap().into();
            table
        });

        Self { table }
    }
}

impl OutputWriter<u64, TdPyAny> for DynamoDBOutput {
    fn push(&mut self, _epoch: u64, key_payload: TdPyAny) {
        Python::with_gil(|py| {
            let (_key, payload): (Option<TdPyAny>, TdPyAny) = py_unwrap!(
                key_payload.extract(py),
                format!(
                    "DynamoDBOutput requires a `(key, payload)` 2-tuple; \
                    got `{key_payload:?}` instead"
                )
            );
            let item: &PyDict = py_unwrap!(
                payload.extract(py),
                format!(
                    "DynamoDBOutput requires that the payload be a Dictionary; got \
                    `{payload:?}` instead"
                )
            );
            let kwargs = [("Item", item)].into_py_dict(py);
            self.table
                .call_method(py, "put_item", (), Some(kwargs))
                .map_err(|err| {
                    let traceback = err.traceback(py).unwrap().format().unwrap();
                    format!("{}{}", err, traceback)
                })
                .unwrap();
        });
    }
}
