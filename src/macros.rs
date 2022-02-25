//! Internal macros
#[macro_export]

macro_rules! with_traceback {
    ($py:expr, $pyfunc:expr) => {
        // This would be the perfect use for the
        // https://doc.rust-lang.org/nightly/unstable-book/language-features/try-blocks.html
        // feature.
        match (|| $pyfunc)() {
            Ok(r) => r,
            Err(err) => std::panic::panic_any(err),
        }
    };
}
