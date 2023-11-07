"""Data model for dataflows and custom operators.

See the `bytewax` module docstring for the basics of building and
running dataflows.

# Custom Operators

You can define new custom operators in terms of already existing
operators. To do this you define an **operator builder function** and
decorate it with `operator`.

>>> @operator()
... def add_to(up: Stream, step_id: str, y: int) -> Stream:
...     return up.map("shim_map", lambda x: x + y)

Once this operator is **loaded**, a **operator method** will be added
to the class annotated on the first argument. Generally using `Stream`
as this type enables a "fluent" API where you can string togehter
operators.

Each input or output `Stream` turns into a `Port` in the resulting
data model.

In order to generate the operator method, operator data model, and
proper nesting of operators, you must follow a few rules when writing
your builder function.

- The first argument must be annotated with class to load the operator
  method on to. The resulting method will look like as if you defined
  the function in the class body. In general, this is either `Stream`,
  `KeyedStream`, or `Dataflow`.

- There must be a `step_id: str` as the second argument, even if not
  used.

- All arguments that are `Stream`s or `MultiStream`s must have type
  annotations. We recommend annotating all the arguments. Argument
  names must not overlap with the fields defined on `Operator`.

- The return value must annoated and one of: `None`, a `Stream`, a
  `MultiStream`, or a dataclass.

- If the return value is a dataclass: No field names of it can overlap
  with argument names or the fields defined on `Operator`. All fields
  of it that are `Stream`s or `MultiStream`s must have type
  annotations. The dataclass can have non-`Stream` fields.

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

# Loading

Built-in operators in `bytewax.operators` are automatically loaded
when you `import bytewax` or any submodules, but custom operators that
you define or are defined in other modules or packages must be
**loaded** via `load_op` or `load_mod_ops`. This will add the operator
methods to the relevant `Stream` classes.

To load the example `add_to` operator above:

>>> load_op(add_to)

Now you can use the `add_to` which was loaded onto `Stream`:

>>> from bytewax.connectors.stdio import StdOutSink
>>> from bytewax.testing import TestingSource, run_main
>>> flow = Dataflow("my_flow")
>>> nums = flow.input("nums", TestingSource([1, 2, 3]))
>>> bigger_nums = nums.add_to("my_op", 3)
>>> bigger_nums.output("print", StdOutSink())
>>> run_main(flow)
4
5
6

"""
import dataclasses
import functools
import inspect
import itertools
import sys
import typing
from dataclasses import dataclass, field
from inspect import Parameter, Signature
from types import FunctionType, ModuleType, NoneType
from typing import (
    Any,
    Callable,
    ClassVar,
    Dict,
    Iterable,
    List,
    Optional,
    Protocol,
    Tuple,
    Type,
    runtime_checkable,
)


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

    You won't be instantiating this manually. The `bytewax.operator`
    builder methods will create these for you whenever a builder
    function takes or returns a `Stream`.

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

    You won't be instantiating this manually. The `bytewax.operator`
    builder methods will create these for you whenever a builder
    function takes or returns a `*args` of `Stream` or `**kwargs` of
    `Stream`s or a `MultiStream`.

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
    #: The class that the operator method should be loaded onto.
    extend_cls: ClassVar[Type]
    inp_names: ClassVar[List[str]]
    out_names: ClassVar[List[str]]


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

    Once you instantiate this, Use the `bytewax.operators` methods
    loaded onto this class (e.g. `bytewax.operators.input.input`) to
    create `Stream`s.

    Operator methods are not documented here since you need to
    dynamically `load_op` them. See `bytewax.operators` for all the
    operator documentation.

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

    def __getattr__(self, name):
        msg = f"no operator named {name!r}"

        for subcls in _rec_subclasses(Stream):
            if hasattr(subcls, name):
                msg = (
                    f"operator {name!r} can only be used on a {subcls!r}; "
                    "use `Dataflow.input` create an initial stream"
                )

        raise AttributeError(msg)


@dataclass(frozen=True)
class Stream:
    """Handle to a specific stream of items you can add steps to.

    You won't be instantiating this manually. Use the
    `bytewax.operators` methods loaded onto this class (e.g.
    `bytewax.operators.map.map`, `bytewax.operators.filter.filter`,
    `bytewax.operators.key_on.key_on`) to create `Stream`s.

    Operator methods are not documented here since you need to
    dynamically `load_op` them. See `bytewax.operators` for all the
    operator documentation.

    You can reference this stream multiple times to duplicate the data
    within.

    Operator builder functions take or return this if they want to
    create an input or output port.

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

    def _with_scope(self, scope: _Scope) -> "Stream":
        return dataclasses.replace(self, _scope=scope)

    def _to_ref(self, ref_id: str) -> SinglePort:
        return SinglePort(ref_id, self.stream_id)

    @staticmethod
    def _from_args(args: Tuple) -> "MultiStream":
        return MultiStream({str(i): stream for i, stream in enumerate(args)})

    @staticmethod
    def _into_args(obj: "MultiStream") -> Tuple:
        return tuple(obj.streams.values())

    @staticmethod
    def _from_kwargs(kwargs: Dict[str, "Stream"]) -> "MultiStream":
        return MultiStream(kwargs)

    @staticmethod
    def _into_kwargs(obj: "MultiStream") -> Dict[str, "Stream"]:
        return dict(obj.streams)

    @staticmethod
    def _help_msg() -> str:
        raise NotImplementedError()

    def __getattr__(self, name):
        msg = f"no operator named {name!r}"

        if hasattr(Dataflow, name):
            msg = f"operator {name!r} can only be used on the top-level {Dataflow!r}"

        for subcls in _rec_subclasses(Stream):
            if hasattr(subcls, name):
                msg = f"operator {name!r} can only be used on a {subcls!r}"
                try:
                    msg += f"; {subcls._help_msg()}"
                except NotImplementedError:
                    pass

        raise AttributeError(msg)


@dataclass(frozen=True)
class MultiStream:
    """A bundle of named `Stream`s.

    Operator builder functions take or return this if they want to
    create an input or output port that can recieve multiple named
    streams dynamically.

    This is also created internally whenever a builder function takes
    or returns a `*args` of `Stream` or `**kwargs` of `Stream`s.

    """

    streams: Dict[str, Stream]

    def _get_scopes(self) -> Iterable[_Scope]:
        return (stream._scope for stream in self.streams.values())

    def _with_scope(self, scope: _Scope) -> "MultiStream":
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


@dataclass(frozen=True)
class KeyedStream(Stream):
    """A `Stream` that specifically contains `(key, value)` pairs.

    Operators loaded onto this all require their upstream to have this
    shape. See `bytewax.operators.key_on.key_on` and
    `bytewax.operators.key_assert.key_assert` to create
    `KeyedStream`s.

    """

    @staticmethod
    def _help_msg() -> str:
        return (
            "use `key_on` to add a key or "
            "`key_assert` if the stream is already a `(key, value)`"
        )


_OPERATOR_BASE_NAMES = frozenset(typing.get_type_hints(_CoreOperator).keys())


def _gen_op_cls(
    builder: FunctionType,
    sig: Signature,
    sig_types: Dict[str, Any],
    core: bool,
) -> Type[Operator]:
    if "step_id" not in sig.parameters:
        msg = "builder function requires a 'step_id' parameter"
        raise TypeError(msg)

    # First add fields for all the input arguments.
    inp_fields = {}
    for name, param in sig.parameters.items():
        typ = sig_types.get(name, Any)

        as_typ = typ
        if inspect.isclass(typ) and issubclass(typ, _AsArgs):
            if param.kind == Parameter.VAR_POSITIONAL:
                method_types = typing.get_type_hints(typ._from_args)
                as_typ = method_types.get("return", Any)
            elif param.kind == Parameter.VAR_KEYWORD:
                method_types = typing.get_type_hints(typ._from_kwargs)
                as_typ = method_types.get("return", Any)

        inp_fields[name] = as_typ

    # Then add fields for the return values.
    out_fields = {}
    out_type = sig_types.get("return", Parameter.empty)
    if out_type == Parameter.empty:
        msg = (
            "builder function requires return type annotation; "
            "this is usually `bytewax.dataflow.Stream`"
        )
        raise TypeError(msg)
    elif inspect.isclass(out_type) and (
        issubclass(out_type, Stream) or issubclass(out_type, MultiStream)
    ):
        out_fields["down"] = out_type
    elif inspect.isclass(out_type) and issubclass(out_type, NoneType):
        pass
    elif dataclasses.is_dataclass(out_type):
        for fld in dataclasses.fields(out_type):
            if fld.name in inp_fields:
                msg = (
                    f"{fld.name!r} is both a build function parameter "
                    "and a return dataclass field name; rename one of them"
                )
                raise TypeError(msg)
            out_fields[fld.name] = fld.type
    else:
        out_fields["down"] = out_type

    cls_fields = inp_fields | out_fields

    # Now update the types to any that store references instead.
    for name, typ in cls_fields.items():
        if inspect.isclass(typ) and issubclass(typ, _ToRef):
            method_types = typing.get_type_hints(typ._to_ref)
            cls_fields[name] = method_types.get("return", Any)

    inp_names = []
    out_names = []
    for name, typ in cls_fields.items():
        if inspect.isclass(typ) and (
            issubclass(typ, SinglePort) or issubclass(typ, MultiPort)
        ):
            if name in inp_fields:
                inp_names.append(name)
            elif name in out_fields:
                out_names.append(name)

    # `step_id` is defined on the parent class.
    del cls_fields["step_id"]

    forbidden_fields = frozenset(cls_fields.keys()) & _OPERATOR_BASE_NAMES
    if len(forbidden_fields) > 0:
        fmt_fields = ", ".join(repr(name) for name in forbidden_fields)
        msg = (
            "builder function can't have parameters or return dataclass fields "
            "that shadow any of the field names in `bytewax.dataflow.Operator`; "
            f"rename the {fmt_fields} parameter or fields"
        )
        raise TypeError(msg)

    # Add the class to extend as a class variable.
    extend_cls = sig_types.get(next(iter(sig.parameters.keys())), Parameter.empty)
    if extend_cls is Parameter.empty:
        msg = (
            "builder function requires type annotation on the first "
            "parameter to know what class to load onto; "
            "this is usually `bytewax.dataflow.Stream`"
        )
        raise TypeError(msg)

    cls_doc = f"""`{builder.__name__}` operator definition.

    Use the `{builder.__name__}` method loaded onto
    `{extend_cls.__module__}.{extend_cls.__name__}` to create a step
    with this operator. See the method below for its documentation.

    """

    cls_ns = {
        "__doc__": cls_doc,
        "extend_cls": extend_cls,
        "inp_names": inp_names,
        "out_names": out_names,
    }

    cls = dataclasses.make_dataclass(
        builder.__name__,
        cls_fields.items(),
        bases=(_CoreOperator if core else Operator,),
        frozen=True,
        namespace=cls_ns,
    )
    cls.__module__ = builder.__module__

    # Hide all the dataclass instance variables. If someone plans on
    # instantiating an `Operator` instance on their own, they better
    # know what they're doing.i
    cls_mod = sys.modules[cls.__module__]
    if not hasattr(cls_mod, "__pdoc__"):
        cls_mod.__pdoc__ = {}  # type: ignore[attr-defined]
    # Don't document the operator fields.
    for name in cls_fields.keys():
        cls_mod.__pdoc__[f"{cls.__name__}.{name}"] = False

    return cls


def _gen_op_method(
    sig: Signature,
    sig_types: Dict[str, Any],
    builder: FunctionType,
    cls: Type[Operator],
    core: bool,
) -> Callable:
    # Wraps ensures that docstrings and type annotations are the same.
    @functools.wraps(builder)
    def method(*args, **kwargs):
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
            typ = sig_types.get(name, Parameter.empty)
            if inspect.isclass(typ) and issubclass(typ, _AsArgs):
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
        outer_scopes.discard(None)
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
            typ = sig_types.get(name, Parameter.empty)
            if inspect.isclass(typ) and issubclass(typ, _AsArgs):
                if param.kind == Parameter.VAR_POSITIONAL:
                    bound.arguments[name] = typ._into_args(val)
                elif param.kind == Parameter.VAR_KEYWORD:
                    bound.arguments[name] = typ._into_kwargs(val)

        # Now call the builder to cause sub-steps to be built.
        out = builder(*bound.args, **bound.kwargs)

        # Now unwrap output values into the cls.
        if isinstance(out, Stream) or isinstance(out, MultiStream):
            cls_vals["down"] = out
        elif isinstance(out, NoneType):
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

    return method


def operator(_detect_parens=None, *, _core: bool = False) -> Callable:
    """Function decorator to define a new operator.

    See `bytewax.dataflow` module docstring for how to use this.

    """
    if _detect_parens is not None:
        msg = (
            "operator decorator must be called with `()`; "
            "use `@operator()` instead of `@operator`"
        )
        raise TypeError(msg)

    def inner_deco(builder: FunctionType) -> Type:
        sig = inspect.signature(builder)
        sig_types = typing.get_type_hints(builder)
        cls = _gen_op_cls(builder, sig, sig_types, _core)
        op_method = _gen_op_method(sig, sig_types, builder, cls, _core)
        setattr(cls, op_method.__name__, op_method)
        # Note that although this is a function definition, a class will
        # be bound to the function name.
        return cls

    return inner_deco


def _monkey_patch(extend_cls: Type, name: str, op_method) -> None:
    # Monkey patch this method onto the relevent class. First check
    # that it isn't already set.
    try:
        old_method = getattr(extend_cls, name)
    except AttributeError:
        old_method = None

    if old_method is not None:
        # `repr(old_method)` doesn't show the module location, just a
        # memory address (strangely, unlike `repr(cls)`), so manually
        # construct a nicer repr.
        old_rep = f_repr(old_method)
        msg = (
            f"{extend_cls!r} already has an operator loaded named "
            f"{name!r} at {old_rep}; "
            "operators can't be overridden; "
            "manually `bytewax.dataflow.load_op` with a different "
            "`name` arg?"
        )
        raise AttributeError(msg)

    setattr(extend_cls, name, op_method)


def _erase_method_pdoc(extend_cls: Type, name: str, op_method) -> None:
    # `__module__` is just a string. Lookup the corresponding module.
    extend_cls_mod = sys.modules[extend_cls.__module__]
    # Ensure we have a way of overriding pdoc in the parent module of
    # the monkey patched class.
    if not hasattr(extend_cls_mod, "__pdoc__"):
        extend_cls_mod.__pdoc__ = {}  # type: ignore[attr-defined]
    # Ignore documentation of the method added to the class; it'll be
    # documented on the operator builder function.
    extend_cls_mod.__pdoc__[f"{extend_cls.__name__}.{name}"] = False


def load_op(op: Type[Operator], as_name: Optional[str] = None) -> None:
    """Load an operator method into its class.

    This needs to be done by hand so you can manually deal with
    operator method name clashes.

    Args:
        op: Operator definition. Created using the `operator`
            decorator.

        as_name: Override the method name. Defaults to the name of the
            operator.

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

    op_method = getattr(op, op.__name__)
    name = as_name if as_name is not None else op_method.__name__
    _monkey_patch(op.extend_cls, name, op_method)
    _erase_method_pdoc(op.extend_cls, name, op_method)


def load_mod_ops(mod: ModuleType) -> None:
    """Load all operators in a module into their classes.

    Use this to enable operators bundled in your own packages.

    >>> import my_package  # doctest: +SKIP
    >>> load_mod_ops(my_package) # doctest: +SKIP

    This is done by default for all built-in Bytewax operators. You do
    not need to call

    >>> import bytewax.operators  # doctest: +SKIP
    >>> load_mod_ops(bytewax.operators)  # doctest: +SKIP

    This needs to be done by hand so you can manually deal with
    operator method name clashes.

    Args:
        mod: Imported module to load all operators in.

    """
    for name in getattr(mod, "__all__", dir(mod)):
        obj = getattr(mod, name)
        if inspect.isclass(obj) and issubclass(obj, Operator):
            load_op(obj, name)
