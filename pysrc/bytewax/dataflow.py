"""Data model for dataflows and custom operators.

See the `bytewax` module docstring for the basics of building and
running dataflows.

# Custom Operators

You can define new custom operators in terms of already existing
operators. To do this you define an **operator function** and
decorate it with `operator`.

>>> import bytewax.operators as op
>>> @operator
... def add_to(step_id: str, up: Stream[int], y: int) -> Stream[int]:
...     return op.map("shim_map", lambda x: x + y)

Each input or output `Stream` turns into a `Port` in the resulting
data model.

In order to generate the operator data model, and proper nesting of
operators, you must follow a few rules when writing your function:

- There must be a `step_id: str` argument, even if not used.

- All arguments, the return value, and return dataclass fields that
  are `Stream`s or `MultiStream`s must have type annotations. We
  recommend annotating all the arguments, the return value, and all
  fields in a return dataclass.

- You must use a `dataclass` to return multiple values.

- Argument and return dataclass field names must not overlap with the
  names defined on the `Operator` base class.

- `Stream`s, `MultiStream`s, and `Dataflow`s _must not appear in
  nested objects_: they either can be arguments, the return type
  directly, or the top-level fields of a dataclass that is the return
  type; nowhere else.

## Docstrings

A good docstring for a custom operator has a few things:

- A one line summary of the operator.

- A doctest example using the operator.

- Any arguments that are streams describe the required shape of that
  upstream.

- The return streams describe the shape of the data that is being sent
  downstream.

"""
import dataclasses
import functools
import inspect
import itertools
import typing
from dataclasses import dataclass, field
from inspect import Parameter, Signature
from types import FunctionType
from typing import (
    Any,
    Callable,
    ClassVar,
    Dict,
    Generic,
    Iterable,
    List,
    Protocol,
    Tuple,
    Type,
    TypeVar,
    overload,
    runtime_checkable,
)

from typing_extensions import Concatenate, ParamSpec

P = ParamSpec("P")
R = TypeVar("R")
X_co = TypeVar("X_co", covariant=True)


def f_repr(f: Callable) -> str:
    """Nicer `repr` for functions with the defining module and line.

    Use this to help with writing easier to debug exceptions in your
    operators.

    The built in repr just shows a memory address.

    >>> def my_f(x):
    ...     pass
    >>> f_repr(my_f)
    "<function 'bytewax.dataflow.my_f' line 1>"

    """
    if isinstance(f, FunctionType):
        path = f"{f.__module__}.{f.__qualname__}"
        line = f"{f.__code__.co_firstlineno}"
        return f"<function {path!r} line {line}>"
    else:
        return repr(f)


@runtime_checkable
class Port(Protocol):
    """Generic interface to a port.

    Either `SinglePort` or `MultiPort`.

    """

    port_id: str
    stream_ids: Dict[str, str]


@dataclass(frozen=True)
class SinglePort:
    """A input or output location on an `Operator`.

    You won't be instantiating this manually. The `operator` decorator
    will create these for you whenever an operator function takes or
    returns a `Stream`.

    """

    port_id: str
    stream_id: str

    @property
    def stream_ids(self) -> Dict[str, str]:
        """Allow this to conform to the `Port` protocol."""
        return {"stream": self.stream_id}


@dataclass(frozen=True)
class MultiPort:
    """A multi-stream input or output location on an `Operator`.

    You won't be instantiating this manually. The `operator` decorator
    will create these for you whenever an operator function takes or
    returns a `*args` of `Stream` or `**kwargs` of `Stream`s or a
    `MultiStream`.

    """

    port_id: str
    stream_ids: Dict[str, str]


@dataclass(frozen=True)
class Operator:
    """Base class for an operator type.

    Subclasses of this must be generated via the `operator` builder
    function decorator. See the `bytewax.dataflow` module docstring
    for a tutorial.

    Subclasses will contain the specific configuration fields each
    operator needs.

    """

    step_name: str
    step_id: str
    substeps: List["Operator"]
    ups_names: ClassVar[List[str]]
    dwn_names: ClassVar[List[str]]


@dataclass(frozen=True)
class _CoreOperator(Operator):
    #: This operator is a core operator.
    core: ClassVar[bool] = True


@dataclass(frozen=True)
class _Scope:
    # This will be the ID of the `Dataflow` or `Operator` to modify.
    parent_id: str
    substeps: List[Operator] = field(compare=False, repr=False)
    flow: "Dataflow" = field(compare=False, repr=False)


@runtime_checkable
class _HasScope(Protocol):
    def _get_scopes(self) -> Iterable[_Scope]:
        ...

    def _with_scope(self, scope: _Scope):
        ...


@runtime_checkable
class _ToRef(Protocol):
    def _to_ref(self, port_id: str):
        ...


@runtime_checkable
class _AsArgs(Protocol):
    @staticmethod
    def _from_args(args: Tuple):
        ...

    @staticmethod
    def _into_args(obj) -> Tuple:
        ...

    @staticmethod
    def _from_kwargs(kwargs: Dict):
        ...

    @staticmethod
    def _into_kwargs(obj) -> Dict:
        ...


def _rec_subclasses(cls):
    yield cls
    for subcls in cls.__subclasses__():
        yield from _rec_subclasses(subcls)


@dataclass(frozen=True)
class DataflowId:
    """Unique ID of a dataflow."""

    flow_id: str


@dataclass(frozen=True)
class Dataflow:
    """Dataflow definition.

    Once you instantiate this, Use the `bytewax.operators` (e.g.
    `bytewax.operators.input`) to create `Stream`s.

    """

    flow_id: str
    substeps: List[Operator] = field(default_factory=list)
    _scope: _Scope = field(default=None, compare=False)  # type: ignore[assignment]

    def __post_init__(self):
        if "." in self.flow_id:
            msg = "flow ID can't contain a period `.`"
            raise ValueError(msg)
        if self._scope is None:
            # The default context at the `Dataflow` level is recursive and
            # means add things to this object.
            scope = _Scope(self.flow_id, self.substeps, self)
            # Trixy get around the fact this is frozen. We don't modify
            # after init, though.
            object.__setattr__(self, "_scope", scope)

    def _get_scopes(self) -> Iterable[_Scope]:
        return [self._scope]

    def _with_scope(self, scope: _Scope) -> "Dataflow":
        return dataclasses.replace(self, _scope=scope)

    def _to_ref(self, _port_id: str) -> DataflowId:
        return DataflowId(self.flow_id)


@dataclass(frozen=True)
class Stream(Generic[X_co]):
    """Handle to a specific stream of items you can add steps to.

    You won't be instantiating this manually. Use the
    `bytewax.operators` (e.g. `bytewax.operators.map`,
    `bytewax.operators.filter`, `bytewax.operators.key_on`) to create
    `Stream`s.

    You can reference this stream multiple times to duplicate the data
    within.

    Operator functions take or return this if they want to create an
    input or output port.

    """

    stream_id: str
    _scope: _Scope = field(compare=False)

    def flow(self) -> Dataflow:
        """The containing `Dataflow`.

        You might want access to this to add "top level" operators
        like `bytewax.operators.merge_all.merge_all`.

        """
        return self._scope.flow

    def _get_scopes(self) -> Iterable[_Scope]:
        return [self._scope]

    def _with_scope(self, scope: _Scope) -> "Stream[X_co]":
        return dataclasses.replace(self, _scope=scope)

    def _to_ref(self, ref_id: str) -> SinglePort:
        return SinglePort(ref_id, self.stream_id)

    @staticmethod
    def _from_args(args: Tuple) -> "MultiStream[X_co]":
        return MultiStream({str(i): stream for i, stream in enumerate(args)})

    @staticmethod
    def _into_args(obj: "MultiStream[X_co]") -> Tuple:
        return tuple(obj.streams.values())

    @staticmethod
    def _from_kwargs(kwargs: Dict[str, "Stream[X_co]"]) -> "MultiStream[X_co]":
        return MultiStream(kwargs)

    @staticmethod
    def _into_kwargs(obj: "MultiStream[X_co]") -> Dict[str, "Stream[X_co]"]:
        return dict(obj.streams)

    def then(
        self,
        op_fn: Callable[Concatenate[str, "Stream[X_co]", P], R],
        step_id: str,
        *args: P.args,
        **kwargs: P.kwargs,
    ) -> R:
        """Convenience method to add an operator in a fluent style.

        The following two dataflow definitions are equivalent:

        >>> def add_one(item):
        ...     return item + 1

        >>> import bytewax.operators as op
        >>> from bytewax.testing import run_main, TestingSource
        >>> from bytewax.dataflow import Dataflow

        >>> flow = Dataflow("map_eg")
        >>> s = op.input("inp", flow, TestingSource(range(3)))
        >>> s = op.map("add_one", s, add_one)

        >>> flow = Dataflow("map_eg")
        >>> s = op.input("inp", flow, TestingSource(range(3))) \
        ...     .then(op.map, "add_one", add_one)

        Because of the limitations of the fluent style having a
        required single stream parameter, this won't work for all
        operators. In general, it's best for operators that are shaped
        like `bytewax.operators.map`: a single stream as input and a
        single stream as output.

        Args:
            step_id: Unique ID.

            op_fn: Operator function. This fluent transformation only
              works on operators that take a single stream as the
              second argument.

            *args: Remaining arguments to pass to `op_fn`.

            **kwargs: Remaining arguments to pass to `op_fn`.

        """
        return op_fn(step_id, self, *args, **kwargs)


@dataclass(frozen=True)
class MultiStream(Generic[X_co]):
    """A bundle of named `Stream`s.

    Operator functions take or return this if they want to create an
    input or output port that can recieve multiple named streams
    dynamically.

    This is also created internally whenever a builder function takes
    or returns a `*args` of `Stream` or `**kwargs` of `Stream`s.

    """

    streams: Dict[str, Stream[X_co]]

    def _get_scopes(self) -> Iterable[_Scope]:
        return (stream._scope for stream in self.streams.values())

    def _with_scope(self, scope: _Scope) -> "MultiStream[X_co]":
        streams = {
            name: stream._with_scope(scope) for name, stream in self.streams.items()
        }
        return dataclasses.replace(self, streams=streams)

    def _to_ref(self, port_id: str) -> MultiPort:
        return MultiPort(
            port_id,
            {name: stream.stream_id for name, stream in self.streams.items()},
        )

    def __iter__(self):
        return iter(self.streams.values())


def _norm_type_hints(obj) -> Dict[str, Type]:
    sig_types = typing.get_type_hints(obj)
    for name, typ in sig_types.items():
        orig = typing.get_origin(typ)
        if orig is not None:
            sig_types[name] = orig
        elif typ is Any:
            sig_types[name] = object

    return sig_types


_OPERATOR_BASE_NAMES = frozenset(typing.get_type_hints(_CoreOperator).keys())


def _gen_inp_fields(sig: Signature, sig_types: Dict[str, Type]) -> Dict[str, Type]:
    inp_fields = {}
    for name, param in sig.parameters.items():
        # If the argument is un-annotated, assume the type class for
        # "Any".
        typ = sig_types.get(name, object)

        as_typ = typ
        # If any of the arguments require special casing when they're
        # *args or **kwargs, find out what the "packed" version of the
        # argument type is. The most common use of this is `*ups:
        # Stream` will actually be stored as a single `MultiStream`,
        # rather than a `Tuple[Stream]`.
        if issubclass(typ, _AsArgs):
            if param.kind == Parameter.VAR_POSITIONAL:
                method_typs = _norm_type_hints(typ._from_args)
                as_typ = method_typs.get("return", object)
            elif param.kind == Parameter.VAR_KEYWORD:
                method_typs = _norm_type_hints(typ._from_kwargs)
                as_typ = method_typs.get("return", object)

        inp_fields[name] = as_typ

    return inp_fields


def _gen_out_fields(sig: Signature, sig_types: Dict[str, Type]) -> Dict[str, Type]:
    out_fields = {}
    out_typ = sig_types.get("return", object)
    # A single `Stream` is stored by convention in a field named
    # "down".
    if issubclass(out_typ, Stream) or issubclass(out_typ, MultiStream):
        out_fields["down"] = out_typ
    # A `None` return value doesn't store any field.
    elif issubclass(out_typ, type(None)):  # type: ignore
        pass
    # Dataclass is the "named return value" options. We copy all the
    # first level fields into the operator dataclass. We use
    # dataclasses because the stdlib gives us tools to introspect them
    # easily.
    elif dataclasses.is_dataclass(out_typ):
        out_field_typs = _norm_type_hints(out_typ)
        for fld in dataclasses.fields(out_typ):
            out_fields[fld.name] = out_field_typs.get(fld.name, object)
    else:
        out_fields["down"] = out_typ

    return out_fields


def _gen_op_cls(
    builder: FunctionType,
    sig: Signature,
    sig_types: Dict[str, Type],
    core: bool,
) -> Type[Operator]:
    if "step_id" not in sig.parameters:
        msg = "builder function requires a 'step_id' parameter"
        raise TypeError(msg)

    # First add fields for all the input arguments.
    inp_fields = _gen_inp_fields(sig, sig_types)
    # Then add fields for the return values.
    out_fields = _gen_out_fields(sig, sig_types)

    conflicting_fields = frozenset(inp_fields.keys()) & frozenset(out_fields.keys())
    if len(conflicting_fields) > 0:
        fmt_fields = ", ".join(repr(name) for name in conflicting_fields)
        msg = (
            f"{fmt_fields} are both a build function parameter "
            "and a return dataclass field name; rename so there are no "
            "overlapping field names"
        )
        raise TypeError(msg)

    cls_fields = {}
    cls_fields.update(inp_fields)
    cls_fields.update(out_fields)

    # Now update the types to any that store references instead. This
    # is because some types (like `Stream`) have `_Scope` which would
    # result in circular references (because they contain pointers to
    # the substep list) if stored directly. Their "reference versions"
    # (for `Stream` it's `SinglePort`) don't include the scope or
    # anything that is just there to facilitate the fluent API.
    for name, typ in cls_fields.items():
        if issubclass(typ, _ToRef):
            method_typs = _norm_type_hints(typ._to_ref)
            cls_fields[name] = method_typs.get("return", object)

    # Store the names of the upstream and downstream ports to enable
    # visualization.
    ups_names = []
    dwn_names = []
    for name, typ in cls_fields.items():
        if issubclass(typ, SinglePort) or issubclass(typ, MultiPort):
            if name in inp_fields:
                ups_names.append(name)
            elif name in out_fields:
                dwn_names.append(name)

    # `step_id` is defined on the parent class.
    del cls_fields["step_id"]

    # Because we're cramming all the input arguments and return value
    # dataclass field names onto the same operator data model
    # dataclass, ensure we aren't clobbering any of the base class
    # names.
    forbidden_fields = frozenset(cls_fields.keys()) & _OPERATOR_BASE_NAMES
    if len(forbidden_fields) > 0:
        fmt_fields = ", ".join(repr(name) for name in forbidden_fields)
        msg = (
            "builder function can't have parameters or return dataclass fields "
            "that shadow any of the field names in `bytewax.dataflow.Operator`; "
            f"rename the {fmt_fields} parameter or fields"
        )
        raise TypeError(msg)

    cls_doc = f"""`{builder.__name__}` operator data model."""

    cls_ns = {
        "__doc__": cls_doc,
        "ups_names": ups_names,
        "dwn_names": dwn_names,
    }

    # Now finally build the dataclass definition. This does not
    # actually instantiate it, we do that in the wrapper method.
    cls = dataclasses.make_dataclass(
        builder.__name__,
        cls_fields.items(),
        bases=(_CoreOperator if core else Operator,),
        frozen=True,
        namespace=cls_ns,
    )
    cls.__module__ = builder.__module__

    return cls


def _gen_op_fn(
    sig: Signature,
    sig_types: Dict[str, Type],
    builder: FunctionType,
    cls: Type[Operator],
    core: bool,
) -> Callable:
    # Wraps ensures that docstrings and type annotations are the same.
    @functools.wraps(builder)
    def fn(*args, **kwargs):
        try:
            bound = sig.bind(*args, **kwargs)
        except TypeError as ex:
            msg = (
                f"operator {cls.__name__!r} method called incorrectly; "
                "see cause above"
            )
            raise TypeError(msg) from ex
        bound.apply_defaults()

        step_id = bound.arguments["step_id"]
        if not isinstance(step_id, str):
            msg = "'step_id' must be a string"
            raise TypeError(msg)
        if "." in step_id:
            msg = "'step_id' can't contain any periods '.'"
            raise ValueError(msg)

        # Pack *args and **kwargs into any special types.
        for name, param in sig.parameters.items():
            val = bound.arguments[name]
            typ = sig_types.get(name, object)
            if issubclass(typ, _AsArgs):
                if param.kind == Parameter.VAR_POSITIONAL:
                    bound.arguments[name] = typ._from_args(val)
                elif param.kind == Parameter.VAR_KEYWORD:
                    bound.arguments[name] = typ._from_kwargs(val)

        outer_scopes = set(
            itertools.chain.from_iterable(
                val._get_scopes()
                for val in bound.arguments.values()
                if isinstance(val, _HasScope)
            )
        )
        if len(outer_scopes) != 1:
            msg = (
                "inconsistent stream scoping; "
                f"found scopes {outer_scopes!r}; "
                "possible nested `Stream` in arguments to this operator "
                "or return value from previous operator; "
                "see `bytewax.dataflow` module docstring for custom operator rules"
            )
            raise ValueError(msg)
        # Get the singular outer_scope.
        outer_scope = next(iter(outer_scopes))
        # Re-scope input arguments that have a scope so internal calls
        # to operator methods will result in sub-steps.
        fq_inner_scope_id = f"{outer_scope.parent_id}.{step_id}"
        inner_scope = _Scope(fq_inner_scope_id, [], outer_scope.flow)
        inner_scope = dataclasses.replace(
            inner_scope, flow=inner_scope.flow._with_scope(inner_scope)
        )
        for name, val in bound.arguments.items():
            if isinstance(val, _HasScope):
                bound.arguments[name] = val._with_scope(inner_scope)
        # Creating the nested scope is what defines the new inner
        # fully-qualified step id. We pass this into the builder
        # function in case you need it for error messages.
        bound.arguments["step_id"] = inner_scope.parent_id

        # Save the input arguments.
        cls_vals = dict(bound.arguments.items())
        cls_vals["step_name"] = step_id

        # Now unpack the special *args and **kwargs types for calling.
        for name, param in sig.parameters.items():
            val = bound.arguments[name]
            typ = sig_types.get(name, object)
            if issubclass(typ, _AsArgs):
                if param.kind == Parameter.VAR_POSITIONAL:
                    bound.arguments[name] = typ._into_args(val)
                elif param.kind == Parameter.VAR_KEYWORD:
                    bound.arguments[name] = typ._into_kwargs(val)

        # Now call the builder to cause sub-steps to be built.
        out = builder(*bound.args, **bound.kwargs)

        # Now unwrap output values into the cls.
        if isinstance(out, Stream) or isinstance(out, MultiStream):
            cls_vals["down"] = out
        elif isinstance(out, type(None)):
            pass
        elif dataclasses.is_dataclass(out):
            for fld in dataclasses.fields(out):
                cls_vals[fld.name] = getattr(out, fld.name)
        else:
            cls_vals["down"] = out

        # Turn into references.
        for name, val in cls_vals.items():
            if isinstance(val, _ToRef):
                fq_ref_id = f"{inner_scope.parent_id}.{name}"
                cls_vals[name] = val._to_ref(fq_ref_id)

        # Now actually build the step instance.
        step = cls(
            substeps=inner_scope.substeps,
            **cls_vals,
        )

        # Check for ID clashes since this will cause streams to be
        # lost.
        for existing_step in outer_scope.substeps:
            if existing_step.step_id == step.step_id:
                msg = (
                    f"step {step.step_id!r} already exists; "
                    "do you have two steps with the same ID?"
                )
                raise ValueError(msg)

        # And store it in the outer scope.
        outer_scope.substeps.append(step)

        # Re-scope outputs that have a scope so calls to operator
        # methods will not still be added in this operator a substeps.
        if isinstance(out, _HasScope):
            out = out._with_scope(outer_scope)
        elif dataclasses.is_dataclass(out):
            vals = {}
            for fld in dataclasses.fields(out):
                val = getattr(out, fld.name)
                if isinstance(val, _HasScope):
                    vals[fld.name] = val._with_scope(outer_scope)
            out = dataclasses.replace(out, **vals)

        return out

    return fn


F = TypeVar("F", bound=Callable[..., Any])


@overload
def operator(builder: F) -> F:
    ...


@overload
def operator(*, _core: bool = False) -> Callable[[F], F]:
    ...


def operator(builder=None, *, _core: bool = False) -> Callable:
    """Function decorator to define a new operator.

    See `bytewax.dataflow` module docstring for how to use this.

    """

    def inner_deco(builder: FunctionType) -> Callable:
        sig = inspect.signature(builder)
        sig_types = _norm_type_hints(builder)
        cls = _gen_op_cls(builder, sig, sig_types, _core)
        fn = _gen_op_fn(sig, sig_types, builder, cls, _core)
        fn._op_cls = cls  # type: ignore[attr-defined]
        return fn

    if builder is not None:
        return inner_deco(builder)
    else:
        return inner_deco
