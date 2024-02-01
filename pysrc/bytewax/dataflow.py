"""Data model for dataflows and custom operators.

See {py:obj}`bytewax.operators` for the built-in operators.

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
    Optional,
    Protocol,
    Type,
    TypeVar,
    overload,
    runtime_checkable,
)

from typing_extensions import Concatenate, ParamSpec, Self

P = ParamSpec("P")
"""Signature of an operator function."""

R = TypeVar("R")
"""Return type of an operator function."""

N = TypeVar("N")
"""Type of name of each stream.

Usually either {py:obj}`int` if derived from `*args` or {py:obj}`str`
if derived from `**kwargs`.

"""

X_co = TypeVar("X_co", covariant=True)
"""Type contained within a {py:obj}`Stream`."""

F = TypeVar("F", bound=Callable[..., Any])
"""Type of operator builder function."""


def f_repr(f: Callable) -> str:
    """Nicer function {py:obj}`repr` showing module and line number.

    Use this to help with writing easier to debug exceptions in your
    operators.

    The built in repr just shows a memory address.

    ```python
    >>> def my_f(x):
    ...     pass
    >>> f_repr(my_f)
    "<function 'bytewax.dataflow.my_f' line 1>"
    ```

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

    Either {py:obj}`SinglePort` or {py:obj}`MultiPort`.

    """

    port_id: str
    stream_ids: Dict[str, str]


@dataclass(frozen=True)
class SinglePort:
    """A input or output location on an {py:obj}`Operator`.

    You won't be instantiating this manually. The {py:obj}`operator`
    decorator will create these for you whenever an operator function
    takes or returns a {py:obj}`Stream`.

    """

    port_id: str
    stream_id: str

    @property
    def stream_ids(self) -> Dict[str, str]:
        """Allow this to conform to the {py:obj}`Port` protocol."""
        return {"stream": self.stream_id}


@dataclass(frozen=True)
class MultiPort(Generic[N]):
    """A multi-stream input or output location on an {py:obj}`Operator`.

    You won't be instantiating this manually. The {py:obj}`operator`
    decorator will create these for you whenever an operator function
    takes or returns a `*args` of {py:obj}`Stream` or `**kwargs` of
    {py:obj}`Stream`s.

    """

    port_id: str
    stream_ids: Dict[N, str]


@dataclass(frozen=True)
class Operator:
    """Base class for an operator type.

    Subclasses of this must be generated via the {py:obj}`operator`
    builder function decorator. See <project:#custom-operators> for a
    tutorial.

    Subclasses will contain the specific configuration fields each
    operator needs.

    """

    step_name: str
    step_id: str
    substeps: List[Self]
    ups_names: ClassVar[List[str]]
    dwn_names: ClassVar[List[str]]


@dataclass(frozen=True)
class _CoreOperator(Operator):
    core: ClassVar[bool] = True
    """This operator is a core operator."""


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

    def _with_scope(self, scope: _Scope) -> Self:
        ...


@runtime_checkable
class _ToRef(Protocol):
    def _to_ref(self, port_id: str):
        ...


@dataclass(frozen=True)
class DataflowId:
    """Unique ID of a dataflow."""

    flow_id: str


@dataclass(frozen=True)
class Dataflow:
    """Dataflow definition.

    Once you instantiate this, Use the {py:obj}`bytewax.operators`
    (e.g. {py:obj}`~bytewax.operators.input`) to create
    {py:obj}`Stream`s.

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

    def _with_scope(self, scope: _Scope) -> Self:
        return dataclasses.replace(self, _scope=scope)

    def _to_ref(self, _port_id: str) -> DataflowId:
        return DataflowId(self.flow_id)


@dataclass(frozen=True)
class Stream(Generic[X_co]):
    """Handle to a specific stream of items you can add steps to.

    You won't be instantiating this manually. Use the
    {py:obj}`bytewax.operators` (e.g.
    {py:obj}`~bytewax.operators.map`,
    {py:obj}`~bytewax.operators.filter`,
    {py:obj}`~bytewax.operators.key_on`) to create streams.

    You can reference this stream multiple times to duplicate the data
    within.

    Operator functions take or return this if they want to create an
    input or output port.

    """

    stream_id: str
    _scope: _Scope = field(compare=False)

    def flow(self) -> Dataflow:
        """The containing dataflow.

        You might want access to this to add "top level" operators
        like {py:obj}`bytewax.operators.merge`.

        """
        return self._scope.flow

    def _get_scopes(self) -> Iterable[_Scope]:
        return [self._scope]

    def _with_scope(self, scope: _Scope) -> Self:
        return dataclasses.replace(self, _scope=scope)

    def _to_ref(self, ref_id: str) -> SinglePort:
        return SinglePort(ref_id, self.stream_id)

    def then(
        self,
        op_fn: Callable[Concatenate[str, Self, P], R],
        step_id: str,
        *args: P.args,
        **kwargs: P.kwargs,
    ) -> R:
        """Chain a new step onto this stream.

        This allows you to add intermediate steps to a dataflow
        without needing to nest operator function calls or make
        intermediate variables.

        The following two dataflow definitions are equivalent:

        ```python
        >>> import bytewax.operators as op
        >>> from bytewax.testing import run_main, TestingSource
        >>> from bytewax.dataflow import Dataflow
        >>> def add_one(item):
        ...     return item + 1
        ```

        ```python
        >>> flow = Dataflow("map_eg")
        >>> s = op.input("inp", flow, TestingSource(range(3)))
        >>> s = op.map("add_one", s, add_one)
        ```

        and

        ```python
        >>> flow = Dataflow("map_eg")
        >>> s = op.input("inp", flow, TestingSource(range(3))).then(
        ...     op.map, "add_one", add_one
        ... )
        ```

        This kind of method chaining is called a "fluent style API".

        Because this style requires a single upstream before the
        method calling `.`, this transformation only works for
        operators that could be called like `op_fn(step_id, upstream,
        ...)`, like {py:obj}`bytewax.operators.map`. It will not work
        for operators like {py:obj}`bytewax.operators.join_named`,
        since they do not have that shape of function signature.

        :arg step_id: Unique ID.

        :arg op_fn: Operator function. This fluent transformation only
          works on operators that take a single stream as the second
          argument.

        :arg *args: Remaining arguments to pass to `op_fn`.

        :arg **kwargs: Remaining arguments to pass to `op_fn`.

        :returns: `op_fun`'s return value as if called normally.

        """
        return op_fn(step_id, self, *args, **kwargs)


@dataclass(frozen=True)
class _MultiStream(Generic[N]):
    """A bundle of named streams.

    This is also created internally whenever a builder function takes
    or returns a `*args` of {py:obj}`Stream` or `**kwargs` of
    {py:obj}`Stream`s.

    """

    streams: Dict[N, Stream[Any]]

    def _get_scopes(self) -> Iterable[_Scope]:
        return (stream._scope for stream in self.streams.values())

    def _with_scope(self, scope: _Scope) -> Self:
        streams = {
            name: stream._with_scope(scope) for name, stream in self.streams.items()
        }
        return dataclasses.replace(self, streams=streams)

    def _to_ref(self, port_id: str) -> MultiPort[N]:
        return MultiPort(
            port_id,
            {name: stream.stream_id for name, stream in self.streams.items()},
        )


_OPERATOR_BASE_NAMES = frozenset(typing.get_type_hints(_CoreOperator).keys())


def _anno_to_typ(anno: Any) -> Optional[Type]:
    if anno is Any:
        return object

    if inspect.isclass(anno):
        return anno

    orig = typing.get_origin(anno)
    if orig is not None and inspect.isclass(orig):
        return orig

    return None


def _gen_inp_fields(sig: Signature, sig_annos: Dict[str, Any]) -> Dict[str, Any]:
    inp_fields: Dict[str, Any] = {}
    for name, param in sig.parameters.items():
        # If the argument is un-annotated, assume the type class for
        # "Any".
        anno = sig_annos.get(name, Any)
        # Assume we pass through the annotation unmodified.
        inp_fields[name] = anno

        typ = _anno_to_typ(anno)
        if typ is not None:
            # If any of the arguments require special casing when
            # they're *args or **kwargs, find out what the "packed"
            # version of the argument type is. The most common use of
            # this is `*ups: Stream` will actually be stored as a
            # single `_MultiStream`, rather than a `Tuple[Stream]`.
            if issubclass(typ, Stream):
                # If the stream is typed, get that inner type to apply to
                # the `_MultiStream`.
                try:
                    stream_typ_arg = typing.get_args(anno)[0]
                except IndexError:
                    # If not, it's effectively typed `Any`.
                    stream_typ_arg = Any

                if param.kind == Parameter.VAR_POSITIONAL:
                    inp_fields[name] = _MultiStream[stream_typ_arg]
                elif param.kind == Parameter.VAR_KEYWORD:
                    inp_fields[name] = _MultiStream[stream_typ_arg]

    return inp_fields


def _gen_out_fields(_sig: Signature, sig_annos: Dict[str, Any]) -> Dict[str, Any]:
    out_fields: Dict[str, Any] = {}
    anno = sig_annos.get("return", Any)
    typ = _anno_to_typ(anno)
    if typ is not None:
        # A single `Stream` is stored by convention in a field named
        # "down". This must be first even though it is the same
        # behavior as the else branch because `Stream` is defined
        # using dataclasses and we don't want to split the stream into
        # it's private fields.
        if issubclass(typ, Stream) or issubclass(typ, _MultiStream):
            out_fields["down"] = anno
        # A `None` return value doesn't store any field.
        elif issubclass(typ, type(None)):
            pass
        # Dataclass is the "named return value" options. We copy all the
        # first level fields into the operator dataclass. We use
        # dataclasses because the stdlib gives us tools to introspect them
        # easily.
        elif dataclasses.is_dataclass(typ):
            out_field_annos = typing.get_type_hints(typ)
            for fld in dataclasses.fields(typ):
                out_fields[fld.name] = out_field_annos.get(fld.name, Any)
        else:
            # If this isn't a 0 return value function (via `None`) or
            # an N return value function (via a dataclass), there's a
            # single field called `down`. Copy the annotation.
            out_fields["down"] = anno
    else:
        # If the return type is not checkable, assume it's a single
        # value and copy the annotation.
        out_fields["down"] = anno

    return out_fields


def _gen_op_cls(
    builder: FunctionType,
    sig: Signature,
    sig_annos: Dict[str, Any],
    core: bool,
) -> Type[Operator]:
    if "step_id" not in sig.parameters:
        msg = "builder function requires a 'step_id' parameter"
        raise TypeError(msg)

    # First add fields for all the input arguments.
    inp_fields = _gen_inp_fields(sig, sig_annos)
    # Then add fields for the return values.
    out_fields = _gen_out_fields(sig, sig_annos)

    conflicting_fields = frozenset(inp_fields.keys()) & frozenset(out_fields.keys())
    if len(conflicting_fields) > 0:
        fmt_fields = ", ".join(repr(name) for name in conflicting_fields)
        msg = (
            f"{fmt_fields} are both a build function parameter "
            "and a return dataclass field name; rename so there are no "
            "overlapping field names"
        )
        raise TypeError(msg)

    cls_fields: Dict[str, Any] = {}
    cls_fields.update(inp_fields)
    cls_fields.update(out_fields)

    # Now update the types to any that store references instead. This
    # is because some types (like `Stream`) have `_Scope` which would
    # result in circular references (because they contain pointers to
    # the substep list) if stored directly. Their "reference versions"
    # (for `Stream` it's `SinglePort`) don't include the scope or
    # anything that is just there to facilitate the fluent API.
    for name, anno in cls_fields.items():
        typ = _anno_to_typ(anno)
        if typ is not None and issubclass(typ, _ToRef):
            method_typs = typing.get_type_hints(typ._to_ref)
            cls_fields[name] = method_typs.get("return", Any)

    # Store the names of the upstream and downstream ports to enable
    # visualization.
    ups_names = []
    dwn_names = []
    for name, anno in cls_fields.items():
        typ = _anno_to_typ(anno)
        if typ is not None and (
            issubclass(typ, SinglePort) or issubclass(typ, MultiPort)
        ):
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
    sig_annos: Dict[str, Any],
    builder: FunctionType,
    cls: Type[Operator],
    _core: bool,
) -> Callable:
    # Wraps ensures that docstrings and type annotations are the same.
    @functools.wraps(builder)
    def fn(*args, **kwargs):
        try:
            bound = sig.bind(*args, **kwargs)
        except TypeError as ex:
            msg = f"operator {cls.__name__!r} called incorrectly; see cause above"
            raise TypeError(msg) from ex
        bound.apply_defaults()

        for name in cls.ups_names:
            param = sig.parameters[name]
            if param.kind == Parameter.VAR_POSITIONAL:
                vals = bound.arguments[name]
                desc = f"{name!r} *args all"
            elif param.kind == Parameter.VAR_KEYWORD:
                vals = bound.arguments[name].values()
                desc = f"{name!r} **kwargs all"
            else:
                vals = [bound.arguments[name]]
                desc = f"{name!r} argument"

            for val in vals:
                if not isinstance(val, Stream):
                    msg = (
                        f"{desc} must be a `Stream`; "
                        f"got a {type(val)!r} instead; "
                        "did you forget to unpack the result of an operator "
                        "that returns multiple streams?"
                    )
                    raise TypeError(msg)

        step_id = bound.arguments["step_id"]
        if not isinstance(step_id, str):
            msg = "'step_id' must be a `str`"
            raise TypeError(msg)
        if "." in step_id:
            msg = "'step_id' can't contain any periods '.'"
            raise ValueError(msg)

        # Pack `Stream`s of *args and **kwargs into the special type
        # we can re-scope.
        for name, param in sig.parameters.items():
            val = bound.arguments[name]
            anno = sig_annos.get(name, Any)
            typ = _anno_to_typ(anno)
            if typ is not None and issubclass(typ, Stream):
                if param.kind == Parameter.VAR_POSITIONAL:
                    bound.arguments[name] = _MultiStream(dict(enumerate(val)))
                elif param.kind == Parameter.VAR_KEYWORD:
                    bound.arguments[name] = _MultiStream(val)

        outer_scopes = frozenset(
            itertools.chain.from_iterable(
                val._get_scopes()
                for val in bound.arguments.values()
                if isinstance(val, _HasScope)
            )
        )
        if len(outer_scopes) != 1:
            msg = (
                "inconsistent stream scoping; "
                f"found multiple scopes {outer_scopes!r}; "
                "expected one; "
                "possible invalid operator definition; "
                "might be nested `Stream` in arguments to this operator "
                "or return value from previous operator; "
                "see `bytewax.dataflow.operator` docstring for custom "
                "operator rules"
            )
            raise AssertionError(msg)
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
            anno = sig_annos.get(name, Any)
            typ = _anno_to_typ(anno)
            if typ is not None and issubclass(typ, Stream):
                if param.kind == Parameter.VAR_POSITIONAL:
                    bound.arguments[name] = tuple(val.streams.values())
                elif param.kind == Parameter.VAR_KEYWORD:
                    bound.arguments[name] = dict(val.streams)

        # Now call the builder to cause sub-steps to be built.
        out = builder(*bound.args, **bound.kwargs)

        # Now unwrap output values into the cls.
        if isinstance(out, Stream) or isinstance(out, _MultiStream):
            cls_vals["down"] = out
        elif isinstance(out, type(None)):
            pass
        elif dataclasses.is_dataclass(out) and not isinstance(out, type):
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
        existing_step_ids = frozenset(
            existing_step.step_id for existing_step in outer_scope.substeps
        )
        if step.step_id in existing_step_ids:
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
        elif dataclasses.is_dataclass(out) and not isinstance(out, type):
            vals = {}
            for fld in dataclasses.fields(out):
                val = getattr(out, fld.name)
                if isinstance(val, _HasScope):
                    vals[fld.name] = val._with_scope(outer_scope)
            out = dataclasses.replace(out, **vals)

        return out

    return fn


@overload
def operator(builder: F) -> F:
    ...


@overload
def operator(*, _core: bool = False) -> Callable[[F], F]:
    ...


def operator(builder=None, *, _core: bool = False) -> Callable:
    """Function decorator to define a new operator.

    See <project:#custom-operators> for how to use this.

    """

    def inner_deco(builder: FunctionType) -> Callable:
        sig = inspect.signature(builder)
        sig_annos = typing.get_type_hints(builder)
        cls = _gen_op_cls(builder, sig, sig_annos, _core)
        fn = _gen_op_fn(sig, sig_annos, builder, cls, _core)
        fn._op_cls = cls  # type: ignore[attr-defined]
        return fn

    if builder is not None:
        return inner_deco(builder)
    else:
        return inner_deco
