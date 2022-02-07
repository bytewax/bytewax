//! Internal macros

macro_rules! with_traceback {
    ($py:expr, $pyfunc:expr) => {
        // This would be the perfect use for the
        // https://doc.rust-lang.org/nightly/unstable-book/language-features/try-blocks.html
        // feature.
        match (|| $pyfunc)() {
            Ok(r) => r,
            Err(err) => {
                let traceback_msg = match err.ptraceback($py) {
                    // Not all Python function calls have a traceback
                    Some(t) => t.format().unwrap_or("no traceback available".to_string()),
                    None => "no traceback available".to_string(),
                };
                // TODO: We may not actually want to panic/abort here
                // But we'll need the ability to decide what to do on error
                // because of the way we are calling operations in
                // timely dataflow
                panic!("{}, {}", err, traceback_msg)
            }
        }
    };
}
