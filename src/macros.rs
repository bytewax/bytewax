//! Internal macros

/// Unwraps using [`std::panic::panic_any`], needed to panic with any structure in Rust 2021.
/// See [https://github.com/rust-lang/rust/issues/78500]
#[macro_export]
macro_rules! unwrap_any {
    ($pyfunc:expr) => {
        $pyfunc.unwrap_or_else(|err| std::panic::panic_any(err))
    };
}

#[macro_export]
/// Creates a new scope for the expression where the `?` operator can be used if the expression
/// returns the same type of the `?` call, then unwraps the result using [`std::panic::panic_any`]
/// This can be used to panic and send a proper error message to the python interpreter.
///
/// ```rust
/// // Example usage
///
/// use std::num::ParseIntError;
/// use bytewax::try_unwrap;
///
/// struct RangeParser {
///     start: u64,
///     end: Option<u64>,
/// }
///
/// impl RangeParser {
///     pub fn new(start: &str) -> Result<RangeParser, ParseIntError> {
///         let start = start.parse::<u64>()?;
///         Ok(Self { start, end: None })
///     }
///
///     pub fn end(mut self, end: &str) -> Result<RangeParser, ParseIntError> {
///         self.end = Some(end.parse::<u64>()?);
///         Ok(self)
///     }
/// }
///
/// let res = try_unwrap!(RangeParser::new("0")?.end("12"));
/// assert_eq!(res.start, 0);
/// assert_eq!(res.end, Some(12));
/// ```
macro_rules! try_unwrap {
    ($pyfunc:expr) => {
        // This would be the perfect use for the
        // https://doc.rust-lang.org/nightly/unstable-book/language-features/try-blocks.html
        // feature.
        (|| $pyfunc)().unwrap_or_else(|err| std::panic::panic_any(err))
    };
}

#[macro_export]
/// Unwraps the result of the expression using [`std::panic::panic_any`].
/// The error is mapped to a [`PyTypeError`] with a custom error message,
/// taken from the second parameter of the macro.
/// This can be used to panic and send a proper error message to the python interpreter.
/// The macro invocations requires that [`pyo3::exceptions::PyTypeError`]
/// is in scope and that the Python interpreter is available to PyO3.
///
/// ```rust
/// use bytewax::py_unwrap;
/// use pyo3::exceptions::PyTypeError;
///
/// pyo3::prepare_freethreaded_python();
///
/// let something = "abc".parse::<u64>();
/// let res = std::panic::catch_unwind(|| py_unwrap!(something, "A custom python error message"));
/// assert!(res.is_err());
/// ```
macro_rules! py_unwrap {
    ($pyfunc:expr, $err_msg:expr) => {
        $pyfunc
            .map_err(|_err| PyTypeError::new_err($err_msg))
            .unwrap_or_else(|err| std::panic::panic_any(err))
    };
}

#[macro_export]
macro_rules! log_func {
    () => {{
        fn f() {}
        fn type_name_of<T>(_: T) -> &'static str {
            std::any::type_name::<T>()
        }
        let name = type_name_of(f);
        &name[..name.len() - 3]
    }};
}
