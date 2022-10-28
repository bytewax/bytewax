use pyo3::Python;

/// Result type used in the crate that holds a String as the Err type.
pub(crate) type StringResult<T> = Result<T, String>;

// This is a trait that can be implemented by any parent class.
// The function returns one of the possible subclasses instances.
pub(crate) trait ParentClass {
    type Children;
    fn get_subclass(&self, py: Python) -> StringResult<Self::Children>;
}
