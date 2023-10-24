"""Data model for dataflows and custom operators.

Create a `Dataflow` instance, then use the operator methods extended
onit to add computational steps. See `bytewax.operators` for all
built-in operators.

>>> flow = Dataflow("my_flow")
>>> nums = flow.input("nums", TestingSource([1, 2, 3]))

## Custom Operators

"""

import dataclasses
import functools
import inspect
import sys
from collections import OrderedDict
from dataclasses import dataclass, field
from inspect import BoundArguments, Parameter, Signature
from types import FunctionType, ModuleType
from typing import (
    Any,
    Callable,
    List,
    Optional,
    Protocol,
    Type,
    runtime_checkable,
)


def f_repr(f: FunctionType) -> str:
    """Nicer `repr` for functions with the defining module and line.

    Use this to help with writing easier to debug exceptions in your
    operators.

    The built in repr just shows a memory address.

    >>> def my_f(x):
    ...     pass
    >>> f_repr(my_f)
    <function '__main__.my_f':1>

    """
    path = f"{f.__module__}.{f.__name__}"
    line = f":{f.__code__.co_firstlineno}"
    return f"<function {path!r}{line}>"


@dataclass(frozen=True)
class Port:
    port_id: str
    stream_id: str


@dataclass(frozen=True)
class Operator:
    step_id: str
    substeps: List["Operator"]
    inp: Any
    out: Any
    inp_ports: OrderedDict[str, Port]
    out_ports: OrderedDict[str, Port]

    def _get_id(self) -> str:
        return self.step_id


@dataclass(frozen=True)
class _CoreOperator(Operator):
    core: bool = True


@dataclass(frozen=True)
class _Scope:
    # This will be the ID of the `Dataflow` or `Operator` and is not
    # unique.
    parent_id: str
    substeps: List[Operator] = field(default_factory=list, compare=False, repr=False)

    def _new_nested_scope(self, parent_name: str) -> "_Scope":
        fq_parent_id = f"{self.parent_id}.{parent_name}"
        return _Scope(fq_parent_id)

    def _new_stream(self, stream_name: str) -> "Stream":
        fq_stream_id = f"{self.parent_id}.{stream_name}"
        return Stream(fq_stream_id, self)


@runtime_checkable
class _HasScope(Protocol):
    _scope: _Scope

    def _with_scope(self, scope: _Scope):
        ...


@runtime_checkable
class _ToRef(Protocol):
    def _to_ref(self):
        ...


def _rec_subclasses(cls):
    yield cls
    for subcls in cls.__subclasses__():
        yield subcls
        yield from _rec_subclasses(subcls)


@dataclass(frozen=True)
class DataflowId:
    """Unique ID of a dataflow."""

    flow_id: str


@dataclass(frozen=True)
class Dataflow:
    """Create a dataflow.

    Once you instantiate this, Use the `bytewax.operator` methods
    extended onto this class (e.g. `input`) to create `Stream`s.

    Operator methods are not documented here since you need to
    dynamically `load_op` them. See `bytewax.operator` for all the
    operator methods documentation.

    """

    flow_id: str
    substeps: List["Operator"] = field(default_factory=list)
    _scope: _Scope = field(default=None, compare=False)

    def __post_init__(self):
        if "." in self.flow_id:
            msg = "flow ID can't contain `.`"
            raise ValueError(msg)
        if self._scope is None:
            # The default context at the `Dataflow` level is recursive and
            # means add things to this object.
            scope = _Scope(self.flow_id, self.substeps)
            # Trixy get around the fact this is frozen. We don't modify
            # after init, though.
            object.__setattr__(self, "_scope", scope)

    def _with_scope(self, scope: _Scope) -> "Dataflow":
        return dataclasses.replace(self, _scope=scope)

    def _to_ref(self) -> DataflowId:
        return DataflowId(self.flow_id)

    def _get_id(self) -> str:
        return self.flow_id

    def __getattr__(self, name):
        for subcls in _rec_subclasses(Stream):
            if hasattr(subcls, name):
                msg = (
                    f"operator {name!r} can only be used on a {subcls!r}; "
                    "use `Dataflow.input` create an initial stream"
                )
                raise AttributeError(msg)

        msg = f"no operator named {name!r}"
        raise AttributeError(msg)


@dataclass(frozen=True)
class StreamId:
    """Unique ID of a stream."""

    stream_id: str


@dataclass(frozen=True)
class Stream:
    """Handle to a specific stream of items you can add steps to.

    Use the `bytewax.operator` methods extended onto this class (e.g.
    `map`, `filter`, `key_on`) to create `Stream`s; you won't be
    instantiating this manually.

    Operator methods are not documented here since you need to
    dynamically `load_op` them. See `bytewax.operator` for all the
    operator method documentation.

    You can reference this stream multiple times to duplicate the data
    within.

    """

    stream_id: str
    _scope: _Scope = field(compare=False)

    def _with_scope(self, scope: _Scope) -> "Stream":
        return dataclasses.replace(self, _scope=scope)

    def _to_ref(self) -> StreamId:
        return StreamId(self.stream_id)

    def __getattr__(self, name):
        for subcls in _rec_subclasses(self.__class__):
            if hasattr(subcls, name):
                msg = f"operator {name!r} can only be used on a {subcls!r}"
                try:
                    msg += f"; {subcls._help_msg()}"
                except AttributeError:
                    pass
                raise AttributeError(msg)

        msg = f"no operator named {name!r}"
        raise AttributeError(msg)


@dataclass(frozen=True)
class KeyedStream(Stream):
    """A `Stream` that specifically contains `(key, value)` pairs.

    Operators extended onto this all require their upstream to have
    this shape. See `bytewax.operators.key_on` and
    `bytewax.operators.assert_keyed` to create `KeyedStream`s.

    """

    @classmethod
    def _assert_from(cls, stream: Stream) -> "KeyedStream":
        return cls(stream.stream_id, stream._scope)

    @staticmethod
    def _help_msg() -> str:
        return (
            "use `key_on` to add a key or "
            "`assert_keyed` if the stream is already a `(key, value)`"
        )


def _ref_type(anno) -> Type:
    if anno is Parameter.empty:
        return Any
    elif inspect.isclass(anno) and issubclass(anno, _ToRef):
        sig = inspect.signature(anno._to_ref)
        return (
            sig.return_annotation if sig.return_annotation != Signature.empty else Any
        )
    else:
        return anno


@classmethod
def _from_bound(cls, bound: BoundArguments):
    kwargs = dict(bound.arguments)
    kwargs.pop("step_id")
    for name, val in kwargs.items():
        if isinstance(val, _ToRef):
            kwargs[name] = val._to_ref()
    return cls(**kwargs)


def _gen_op_cls(
    builder: FunctionType,
    sig: Signature,
    core: bool,
) -> Type[Operator]:
    if "step_id" not in sig.parameters:
        msg = "builder function requires a `step_id` parameter"
        raise TypeError(msg)

    # Use an `OrderedDict` so the arguments are in the same order.
    inp_fields = OrderedDict(
        (name, _ref_type(param.annotation)) for name, param in sig.parameters.items()
    )
    # `step_id` is saved on the operator itself.
    del inp_fields["step_id"]
    inp_cls = dataclasses.make_dataclass(
        "Inputs",
        inp_fields,
        frozen=True,
        namespace={
            "_from_bound": _from_bound,
        },
    )
    inp_cls.__module__ = builder.__module__

    if inspect.isclass(sig.return_annotation) and issubclass(
        sig.return_annotation, Stream
    ):
        out_fields = OrderedDict([("down", StreamId)])

        @classmethod
        def _from_out(cls, out):
            if not isinstance(out, Stream):
                msg = (
                    f"builder {f_repr(builder)} was annotated to return a `Stream`; "
                    f"got {type(out)!r} instead"
                )
                raise TypeError(msg)

            out = out._to_ref()
            return cls(out)

    elif sig.return_annotation is None:
        out_fields = OrderedDict()

        @classmethod
        def _from_out(cls, out):
            if out is not None:
                msg = (
                    f"builder {f_repr(builder)} was annotated to return `None`; "
                    f"got {type(out)!r} instead"
                )
                raise TypeError(msg)
            return cls()

    # This must be last because `Stream` is a dataclass itself.
    elif dataclasses.is_dataclass(sig.return_annotation):
        out_fields = OrderedDict(
            (field.name, _ref_type(field.type))
            for field in dataclasses.fields(sig.return_annotation)
        )

        @classmethod
        def _from_out(cls, out):
            if not dataclasses.is_dataclass(out):
                msg = (
                    f"builder {f_repr(builder)} was annotated to return a dataclass; "
                    f"got {type(out)!r} instead"
                )
                raise TypeError(msg)
            kwargs = {
                field.name: getattr(out, field.name)
                for field in dataclasses.fields(out)
            }
            for name, val in kwargs.items():
                if isinstance(val, _ToRef):
                    kwargs[name] = val._to_ref()
            return cls(**kwargs)

    else:
        msg = (
            "builder function requires a return type annotation; "
            "must return a dataclass, a `Stream`, or `None`"
        )
        raise TypeError(msg)

    out_cls = dataclasses.make_dataclass(
        "Outputs",
        out_fields,
        frozen=True,
        namespace={
            "_from_out": _from_out,
        },
    )
    out_cls.__module__ = builder.__module__

    # Add the class to extend as a class variable.
    extend_cls = next(iter(sig.parameters.values())).annotation
    if extend_cls is Parameter.empty:
        msg = (
            "builder function requires type annotation on the first "
            "parameter to know what class to extend; "
            "this is usually `Stream`"
        )
        raise TypeError(msg)

    cls = dataclasses.make_dataclass(
        builder.__name__,
        [
            ("inp", inp_cls),
            ("out", out_cls),
        ],
        bases=(_CoreOperator if core else Operator,),
        frozen=True,
        namespace={
            "__doc__": "Operator class",
            # Store the IO class definitions as a nested classes.
            "Inputs": inp_cls,
            "Outputs": out_cls,
            "extend_cls": extend_cls,
        },
    )
    cls.__module__ = builder.__module__
    inp_cls.__qualname__ = f"{cls.__qualname__}.{inp_cls.__name__}"
    out_cls.__qualname__ = f"{cls.__qualname__}.{out_cls.__name__}"

    mod = sys.modules[cls.__module__]
    if not hasattr(mod, "__pdoc__"):
        mod.__pdoc__ = {}
    # mod.__pdoc__[f"{cls.__name__}"] = False

    return cls


def _magicmap(x, t: Type, f: Callable):
    if isinstance(x, t):
        return f(x)
    elif isinstance(x, BoundArguments):
        out = BoundArguments(x.signature, x.arguments)
        for name, val in out.arguments.items():
            if isinstance(val, t):
                out.arguments[name] = f(val)
        return out
    elif dataclasses.is_dataclass(x):
        fields_to_values = {
            field.name: getattr(x, field.name) for field in dataclasses.fields(x)
        }
        mapped = {
            name: f(val) for name, val in fields_to_values.items() if isinstance(val, t)
        }
        return dataclasses.replace(x, **mapped)
    elif x is None:
        return None
    else:
        raise TypeError()


def _rescope(x, scope: _Scope):
    return _magicmap(x, _HasScope, lambda val: val._with_scope(scope))


def _extract_ports(save_dc, step_id: str) -> OrderedDict[str, Port]:
    ports = OrderedDict()
    for dc_f in dataclasses.fields(save_dc):
        val = getattr(save_dc, dc_f.name)
        if isinstance(val, StreamId):
            fq_port_id = f"{step_id}.{dc_f.name}"
            ports[dc_f.name] = Port(fq_port_id, val.stream_id)
    return ports


def _gen_op_method(
    sig: Signature,
    builder: FunctionType,
    cls: Type[Operator],
) -> Callable:
    # Wraps ensures that docstrings and type annotations are the same.
    @functools.wraps(builder)
    def method(*args, **kwargs):
        try:
            bound = sig.bind(*args, **kwargs)
        except TypeError as ex:
            msg = (
                f"operator {cls.__name__!r} extension method called incorrectly; "
                "see cause above"
            )
            raise TypeError(msg) from ex
        bound.apply_defaults()
        step_id = bound.arguments["step_id"]
        if "." in step_id:
            msg = "step ID can't contain `.`"
            raise ValueError(msg)

        outer_scopes = set(
            val._scope for val in bound.arguments.values() if isinstance(val, _HasScope)
        )
        assert len(outer_scopes) == 1
        # Get the singular outer_scope.
        outer_scope = next(iter(outer_scopes))

        save_inp = cls.Inputs._from_bound(bound)

        # Re-scope input arguments that have a scope so internal calls
        # to operator methods will result in sub-steps.
        inner_scope = outer_scope._new_nested_scope(step_id)

        # Creating the nested scope is what defines the new inner
        # fully-qualified step id. We pass this into the builder
        # function in case you need it for error messages.
        bound.arguments["step_id"] = inner_scope.parent_id
        bound = _rescope(bound, inner_scope)
        inp_ports = _extract_ports(save_inp, inner_scope.parent_id)

        # Now call the builder to cause sub-steps to be built.
        out = builder(*bound.args, **bound.kwargs)

        save_out = cls.Outputs._from_out(out)
        out_ports = _extract_ports(save_out, inner_scope.parent_id)
        # Now actually build the step instance.
        step = cls(
            step_id=inner_scope.parent_id,
            substeps=inner_scope.substeps,
            inp=save_inp,
            out=save_out,
            inp_ports=inp_ports,
            out_ports=out_ports,
        )

        # Check for ID clashes since this will cause streams to be
        # lost.
        for existing_step in outer_scope.substeps:
            if existing_step.step_id == step.step_id:
                msg = (
                    f"step {step.step_id!r} already exists; "
                    "do you have two steps with the same name?"
                )
                raise ValueError(msg)

        # And store it in the outer scope.
        outer_scope.substeps.append(step)

        # Re-scope outputs that have a scope so calls to operator
        # methods will not still be added in this operator a substeps.
        out = _rescope(out, outer_scope)

        return out

    return method


def _register_op(cls: Type[Operator]) -> None:
    # Add this operator name to the list of all operators in the
    # defining module.
    mod = sys.modules[cls.__module__]
    if not hasattr(mod, "__all_ops__"):
        mod.__all_ops__ = []
    mod.__all_ops__.append(cls.__name__)


def operator(_detect_parens=None, *, _core: bool = False) -> Callable:
    """Function decorator to define a new operator."""
    if _detect_parens is not None:
        msg = (
            "operator decorator must be called with `()`; "
            "use `@operator()` instead of `@operator`"
        )
        raise TypeError(msg)

    def inner_deco(builder: FunctionType) -> Type:
        sig = inspect.signature(builder)
        cls = _gen_op_cls(builder, sig, _core)
        cls.method = _gen_op_method(sig, builder, cls)
        _register_op(cls)
        # Note that although this is a function definition, a class will
        # be bound to the function name.
        return cls

    return inner_deco


def _monkey_patch(extend_cls: Type, op_method) -> None:
    # Monkey patch this method onto the relevent class. First check
    # that it isn't already set.
    try:
        old_method = getattr(extend_cls, op_method.__name__)
    except AttributeError:
        old_method = None

    if old_method is not None:
        # `repr(old_method)` doesn't show the module location, just a
        # memory address (strangely, unlike `repr(cls)`), so manually
        # construct a nicer repr.
        old_rep = f_repr(old_method)
        msg = (
            f"{extend_cls!r} already has an operator loaded named "
            f"{op_method.__name__!r} at {old_rep}; "
            "operators can't be overridden; "
            "manually `bytewax.dataflow.load_op` with a different "
            "`name` arg?"
        )
        raise AttributeError(msg)

    setattr(extend_cls, op_method.__name__, op_method)


def _erase_method_pdoc(extend_cls: Type, op_method) -> None:
    # `__module__` is just a string. Lookup the corresponding module.
    extend_cls_mod = sys.modules[extend_cls.__module__]
    # Ensure we have a way of overriding pdoc in the parent module of
    # the monkey patched class.
    if not hasattr(extend_cls_mod, "__pdoc__"):
        extend_cls_mod.__pdoc__ = {}
    # Ignore documentation of the method added to the class; it'll be
    # documented on the operator builder function.
    extend_cls_mod.__pdoc__[f"{extend_cls.__name__}.{op_method.__name__}"] = False


def load_op(op: Type[Operator], name: Optional[str] = None) -> None:
    """Load an operator builder method into its extension class.

    This needs to be done by hand so you can manually deal with
    extension method name clashes.

    Args:
        op:

        name:

    """
    if not issubclass(op, Operator):
        if isinstance(op, FunctionType):
            rep = f_repr(op)
        else:
            rep = repr(op)
        msg = (
            f"{rep!r} isn't an operator; "
            "decorate a builder function with `@operator()` to create "
            "a new operator"
        )
        raise TypeError(msg)

    _monkey_patch(op.extend_cls, op.method)
    _erase_method_pdoc(op.extend_cls, op.method)


def load_mod_ops(mod: ModuleType) -> None:
    """Load all operators in a module into their extension classes.

    Use this to enable operators bundled in your own packages.

    >>> import my_package
    >>> load_mod_ops(my_package)

    This is done by default for all built-in Bytewax operators. You do
    not need to call `load_mod_ops(bytewax.operators)`.

    This needs to be done by hand so you can manually deal with
    extension method name clashes.

    Operator names should be listed in `__all_ops__` in that module.
    The `@operator` decorator will add to this list for you.

    Args:
        mod:

    """
    if not hasattr(mod, "__all_ops__"):
        msg = (
            f"{mod!r} has no operators defined in it; "
            "decorate builder functions with `@operator()`"
        )
        raise ValueError(msg)

    for name in mod.__all_ops__:
        op = getattr(mod, name)
        load_op(op, name)
