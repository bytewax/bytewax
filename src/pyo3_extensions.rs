//! Newtypes around PyO3 types which allow easier interfacing with
//! Timely or other Rust libraries we use.
use crate::try_unwrap;

use pyo3::basic::CompareOp;
use pyo3::exceptions::PyTypeError;
use pyo3::ffi;
use pyo3::intern;
use pyo3::prelude::*;
use pyo3::sync::GILOnceCell;
use pyo3::types::PyBytes;
use serde::ser::Error;
use std::fmt;
use std::mem::ManuallyDrop;
use std::sync::atomic::AtomicPtr;
use std::sync::atomic::Ordering;
use std::sync::Arc;

static PICKLE_MODULE: GILOnceCell<Py<PyModule>> = GILOnceCell::new();

/// Represents a Python object flowing through a Timely dataflow.
///
/// As soon as you need to manipulate this object, convert it into a
/// [`PyObject`] or bind it into a [`Bound`]. This should only exist
/// within the dataflow.
///
/// A newtype for [`Py`]<[`PyAny`]> so we can
/// extend it with traits that Timely needs. See
/// <https://github.com/Ixrec/rust-orphan-rules> for why we need a
/// newtype and what they are.
///
/// Cloning must not need the GIL. Since PyO3 0.22, incrementing a
/// Python refcount requires the GIL (the `py-clone` feature panics
/// without it). Timely clones every item on worker threads whenever
/// a stream is read by more than one operator, so taking the GIL
/// there made workers contend on it and multi-worker dataflows
/// slowed down. So a clone moves the [`Py`] into an [`Arc`] shared by
/// both copies, which defers the Python incref until an item is
/// unwrapped back into a [`PyObject`], and that happens with the GIL
/// already held. Dropping the last [`Arc`] without the GIL is fine:
/// PyO3 queues the decref in its reference pool.
///
/// Most items are never cloned, so an item holds its reference to the
/// object directly until its first clone. Wrapping every item in an
/// [`Arc`] made each operator allocate and free one per item.
///
/// The item is one pointer, in one of two forms told apart by its low
/// bit, which is clear in the address of a Python object and set here
/// in the address of an [`Arc`]:
///
/// - the owned reference to the object, as given by [`Py::into_ptr`];
/// - an [`Arc`]`<`[`PyObject`]`>` count, as given by [`Arc::into_raw`].
///
/// The pointer is atomic only so that the first clone can move an
/// item from the first form to the second through `&self`. Nothing
/// else changes it.
pub(crate) struct TdPyAny {
    repr: AtomicPtr<ffi::PyObject>,
}

// `TdPyAny` is `Send` and `Sync` because its only field is an
// `AtomicPtr`. That is sound for the same reason as the manual impls
// on `Py<T>`, and needs one rule more: the Python refcount of the
// object only changes with the GIL held. A clone only changes the
// count of the `Arc`, and a drop without the GIL queues the decref in
// PyO3's reference pool, which runs on the next GIL acquisition.

/// Set in the address of an [`Arc`] in [`TdPyAny::repr`].
const SHARED: usize = 1;

fn is_shared(repr: *mut ffi::PyObject) -> bool {
    repr.addr() & SHARED != 0
}

fn tag_shared(arc: *const PyObject) -> *mut ffi::PyObject {
    arc.cast::<ffi::PyObject>()
        .cast_mut()
        .map_addr(|addr| addr | SHARED)
}

fn untag_shared(repr: *mut ffi::PyObject) -> *const PyObject {
    repr.map_addr(|addr| addr & !SHARED)
        .cast::<PyObject>()
        .cast_const()
}

/// Take back the reference an item holds.
///
/// # Safety
///
/// `ptr` is a reference to a Python object that the caller owns. The
/// GIL is not needed to hold it, and dropping the result without the
/// GIL queues the decref.
unsafe fn owned_from_raw(ptr: *mut ffi::PyObject) -> PyObject {
    // The token only serves the null check, which `ptr` passes.
    Py::from_owned_ptr(Python::assume_gil_acquired(), ptr)
}

impl TdPyAny {
    /// Bind the object. This holds a new reference, so the item can
    /// still be cloned or moved while the result is in use.
    pub(crate) fn bind<'py>(&self, py: Python<'py>) -> Bound<'py, PyAny> {
        let repr = self.repr.load(Ordering::Acquire);
        // Safety: the item owns its reference, or a count of the
        // `Arc` that does, for as long as `self` lives.
        unsafe {
            if is_shared(repr) {
                (*untag_shared(repr)).bind(py).clone()
            } else {
                Bound::from_borrowed_ptr(py, repr)
            }
        }
    }
}

impl Clone for TdPyAny {
    fn clone(&self) -> Self {
        let repr = self.repr.load(Ordering::Acquire);
        if is_shared(repr) {
            // Safety: `self` holds a count, so the `Arc` is alive.
            unsafe { Arc::increment_strong_count(untag_shared(repr)) };
            return Self {
                repr: AtomicPtr::new(repr),
            };
        }

        // The first clone. Move the reference into an `Arc` with a
        // count for each of the two items.
        //
        // Safety: `repr` is the reference `self` owns, and `self` is
        // borrowed, so it is still owned while this runs.
        let arc = Arc::new(unsafe { owned_from_raw(repr) });
        let for_self = Arc::into_raw(arc.clone());
        match self.repr.compare_exchange(
            repr,
            tag_shared(for_self),
            Ordering::AcqRel,
            Ordering::Acquire,
        ) {
            Ok(_) => Self {
                repr: AtomicPtr::new(tag_shared(Arc::into_raw(arc))),
            },
            Err(_) => {
                // Another thread cloned `self` at the same time and
                // owns the reference now. Give up this `Arc` without
                // dropping the reference in it, and clone again.
                //
                // Safety: `for_self` is the count taken above, which
                // `self` did not take over, so the `Arc` is unique
                // after it is dropped.
                unsafe { drop(Arc::from_raw(for_self)) };
                if let Ok(obj) = Arc::try_unwrap(arc) {
                    std::mem::forget(obj);
                }
                self.clone()
            }
        }
    }
}

impl Drop for TdPyAny {
    fn drop(&mut self) {
        let repr = *self.repr.get_mut();
        // Safety: the item owns what `repr` points to.
        unsafe {
            if is_shared(repr) {
                drop(Arc::from_raw(untag_shared(repr)));
            } else {
                drop(owned_from_raw(repr));
            }
        }
    }
}

/// Only takes the GIL to incref if other clones of this item are
/// still alive.
impl From<TdPyAny> for PyObject {
    fn from(x: TdPyAny) -> Self {
        let x = ManuallyDrop::new(x);
        let repr = x.repr.load(Ordering::Acquire);
        // Safety: the item owned what `repr` points to, and `x` will
        // not drop it.
        unsafe {
            if is_shared(repr) {
                Arc::try_unwrap(Arc::from_raw(untag_shared(repr)))
                    .unwrap_or_else(|arc| Python::with_gil(|py| arc.clone_ref(py)))
            } else {
                owned_from_raw(repr)
            }
        }
    }
}

impl From<PyObject> for TdPyAny {
    fn from(x: PyObject) -> Self {
        Self {
            repr: AtomicPtr::new(x.into_ptr()),
        }
    }
}

impl From<Bound<'_, PyAny>> for TdPyAny {
    fn from(x: Bound<'_, PyAny>) -> Self {
        Self::from(x.unbind())
    }
}

/// Allows you to debug print Python objects using their repr.
impl std::fmt::Debug for TdPyAny {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        let s: PyResult<String> = Python::with_gil(|py| {
            let self_ = self.bind(py);
            let binding = self_.repr()?;
            let repr = binding.to_str()?;
            Ok(String::from(repr))
        });
        f.write_str(&s.map_err(|_| std::fmt::Error {})?)
    }
}

/// Serialize Python objects flowing through Timely that cross
/// process bounds as bytes.
impl serde::Serialize for TdPyAny {
    // We can't do better than isolating the Result<_, PyErr> part and
    // the explicitly converting.  1. `?` automatically trys to
    // convert using From<ReturnedError> for OuterError. But orphan
    // rule means we can't implement it since we don't own either py
    // or serde error types.  2. Using the newtype trick isn't worth
    // it, since you'd have to either wrap all the PyErr when they're
    // generated, or you implement From twice, once to get MyPyErr and
    // once to get serde::Err. And then you're calling .into()
    // explicitly since the last line isn't a `?` anyway.
    //
    // There's the separate problem if we could even implement the
    // Froms. We aren't allowed to "capture" generic types in an inner
    // `impl<S>` https://doc.rust-lang.org/error-index.html#E0401 and
    // we can't move them top-level since we don't know the concrete
    // type of S, and you're not allowed to have unconstrained generic
    // parameters https://doc.rust-lang.org/error-index.html#E0207 .
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        Python::with_gil(|py| {
            let x = self.bind(py);
            let pickle = PICKLE_MODULE
                .get_or_try_init(py, || -> PyResult<Py<PyModule>> {
                    Ok(py.import("pickle")?.unbind())
                })
                .map_err(S::Error::custom)?;
            let binding = pickle
                .bind(py)
                .call_method1(intern!(py, "dumps"), (x,))
                .map_err(S::Error::custom)?;
            let bytes = binding.downcast::<PyBytes>().map_err(S::Error::custom)?;
            serializer
                .serialize_bytes(bytes.as_bytes())
                .map_err(S::Error::custom)
        })
    }
}

pub(crate) struct PickleVisitor;

impl serde::de::Visitor<'_> for PickleVisitor {
    type Value = TdPyAny;

    fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        formatter.write_str("a pickled byte array")
    }

    fn visit_bytes<'py, E>(self, bytes: &[u8]) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        let x: Result<TdPyAny, PyErr> = Python::with_gil(|py| {
            let pickle = py.import("pickle")?;
            let x = pickle
                .call_method1(intern!(py, "loads"), (bytes,))?
                .unbind()
                .into();
            Ok(x)
        });
        x.map_err(E::custom)
    }
}

/// Deserialize Python objects flowing through Timely that cross
/// process bounds from pickled bytes.
impl<'de> serde::Deserialize<'de> for TdPyAny {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        deserializer.deserialize_bytes(PickleVisitor)
    }
}

/// Re-use Python's value semantics in Rust code.
impl PartialEq for TdPyAny {
    fn eq(&self, other: &Self) -> bool {
        Python::with_gil(|py| {
            // Don't use Py.eq or PyAny.eq since it only checks
            // pointer identity.
            let self_ = self.bind(py);
            let other = other.bind(py);
            try_unwrap!(self_.rich_compare(other, CompareOp::Eq)?.is_truthy())
        })
    }
}

/// A Python object that is callable.
///
/// To actually call, you must [`bind`] it and use the bound interface
/// in order to not need to have a dual `TdPyX` vs `TdBoundX`.
pub(crate) struct TdPyCallable(PyObject);

/// Have PyO3 do type checking to ensure we only make from callable
/// objects.
impl<'py> FromPyObject<'py> for TdPyCallable {
    fn extract_bound(ob: &Bound<'py, PyAny>) -> PyResult<Self> {
        if ob.is_callable() {
            let py = ob.py();
            Ok(Self(ob.as_unbound().clone_ref(py)))
        } else {
            let msg = if let Ok(type_name) = ob.get_type().qualname() {
                format!("'{type_name}' object is not callable")
            } else {
                "object is not callable".to_string()
            };
            Err(PyTypeError::new_err(msg))
        }
    }
}

impl fmt::Debug for TdPyCallable {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let s: PyResult<String> = Python::with_gil(|py| {
            let name: String = self.0.bind(py).getattr("__name__")?.extract()?;
            Ok(name)
        });
        f.write_str(&s.map_err(|_| std::fmt::Error {})?)
    }
}

impl TdPyCallable {
    pub(crate) fn bind<'py>(&self, py: Python<'py>) -> &Bound<'py, PyAny> {
        self.0.bind(py)
    }
}

// This is a trait that can be implemented by any parent class.
// The function returns one of the possible subclasses instances.
pub(crate) trait PyConfigClass<S> {
    fn downcast(&self, py: Python) -> PyResult<S>;
}

#[cfg(test)]
mod tests {
    use super::*;
    use pyo3::types::PyList;

    fn new_obj(py: Python) -> PyObject {
        PyList::empty(py).into_any().unbind()
    }

    #[test]
    fn td_py_any_is_a_pointer() {
        assert_eq!(std::mem::size_of::<TdPyAny>(), std::mem::size_of::<usize>());
    }

    #[test]
    fn unwrapping_an_unshared_item_keeps_its_reference() {
        pyo3::prepare_freethreaded_python();
        Python::with_gil(|py| {
            let obj = new_obj(py);
            let base = obj.get_refcnt(py);

            let item = TdPyAny::from(obj.clone_ref(py));
            assert_eq!(obj.get_refcnt(py), base + 1);
            let back = PyObject::from(item);
            assert_eq!(obj.get_refcnt(py), base + 1);
            assert!(back.is(&obj));

            drop(back);
            assert_eq!(obj.get_refcnt(py), base);
        });
    }

    #[test]
    fn clones_do_not_touch_the_refcount_until_unwrapped() {
        pyo3::prepare_freethreaded_python();
        Python::with_gil(|py| {
            let obj = new_obj(py);
            let base = obj.get_refcnt(py);

            let item = TdPyAny::from(obj.clone_ref(py));
            let copy = item.clone();
            let third = copy.clone();
            // All three share the single reference taken above.
            assert_eq!(obj.get_refcnt(py), base + 1);
            assert!(item.bind(py).is(&obj));
            assert!(third.bind(py).is(&obj));

            drop(item);
            drop(third);
            assert_eq!(obj.get_refcnt(py), base + 1);

            // The last item moves the reference out without a new one.
            let back = PyObject::from(copy);
            assert_eq!(obj.get_refcnt(py), base + 1);
            drop(back);
            assert_eq!(obj.get_refcnt(py), base);
        });
    }

    #[test]
    fn unwrapping_a_shared_item_takes_its_own_reference() {
        pyo3::prepare_freethreaded_python();
        Python::with_gil(|py| {
            let obj = new_obj(py);
            let base = obj.get_refcnt(py);

            let item = TdPyAny::from(obj.clone_ref(py));
            let copy = item.clone();
            let back = PyObject::from(item);
            assert_eq!(obj.get_refcnt(py), base + 2);
            drop(copy);
            drop(back);
            assert_eq!(obj.get_refcnt(py), base);
        });
    }

    #[test]
    fn clones_race_without_the_gil() {
        pyo3::prepare_freethreaded_python();
        let (obj, base) = Python::with_gil(|py| {
            let obj = new_obj(py);
            let base = obj.get_refcnt(py);
            (obj, base)
        });

        for _ in 0..200 {
            let item = Arc::new(Python::with_gil(|py| TdPyAny::from(obj.clone_ref(py))));
            // Hold back the threads so they clone at about the same
            // time. They take no GIL, as on a worker thread.
            let barrier = Arc::new(std::sync::Barrier::new(4));
            let threads: Vec<_> = (0..4)
                .map(|_| {
                    let item = item.clone();
                    let barrier = barrier.clone();
                    std::thread::spawn(move || {
                        barrier.wait();
                        let copies: Vec<_> = (0..16).map(|_| (*item).clone()).collect();
                        // Unwrapping the last copy needs no GIL.
                        drop(copies);
                    })
                })
                .collect();
            for thread in threads {
                thread.join().unwrap();
            }
            Python::with_gil(|py| {
                let item = Arc::try_unwrap(item).ok().unwrap();
                assert!(item.bind(py).is(&obj));
                // The thread drops are queued decrefs, run on the next
                // GIL acquisition, which is this one.
                drop(item);
            });
            Python::with_gil(|py| assert_eq!(obj.get_refcnt(py), base));
        }
    }
}
