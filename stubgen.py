"""Generate a stub `.pyi` file for a Python module.

PyO3 does not automatically generate stubs yet. Watch
https://github.com/PyO3/pyo3/issues/2454

mypy stubgen doesn't do deep inspection to get function signatures, so
everything is `f(*args, **kwargs)`. It also doesn't like generating
stubs for classes that have their cls.__module__ that is not the
current module, which is what we're doing with everything is defined
in `bytewax.bytewax` then we re-import elsewhere.
https://mypy.readthedocs.io/en/stable/stubgen.html

"""

import ast
import dataclasses
import importlib
import inspect
import itertools
from argparse import ArgumentParser
from dataclasses import dataclass
from inspect import Parameter, Signature
from types import (
    BuiltinFunctionType,
    BuiltinMethodType,
    FunctionType,
    GetSetDescriptorType,
    MethodType,
    ModuleType,
)
from typing import List, Tuple, Union

import graphlib  # novermin
from typing_extensions import Self, TypeVar

try:
    from ast import unparse  # novermin
except ImportError:
    from astor import to_source as unparse


_N = TypeVar("_N")

_INDENT_SPACES = 4


def _is_public(name: str) -> bool:
    return not name.startswith("_")


@dataclass(frozen=True)
class _Ctx:
    col_offset: int = -_INDENT_SPACES

    def idnt(self):
        return self.col_offset + _INDENT_SPACES

    def new_scope(self) -> Self:
        return dataclasses.replace(
            self,
            col_offset=self.col_offset + _INDENT_SPACES,
        )


@dataclass(frozen=True)
class _Meta:
    qualname: str
    deps: List[str]

    @classmethod
    def for_obj(cls, obj: object, deps: List[str]) -> Self:
        return cls(obj.__qualname__, deps)


def _raise_deps(children: List[Tuple[_Meta, _N]]) -> List[str]:
    return list(itertools.chain.from_iterable(m.deps for m, _ in children))


def _sort_children(children: List[Tuple[_Meta, _N]]) -> List[_N]:
    qualname_to_node = {m.qualname: node for m, node in children}
    body_order = graphlib.TopologicalSorter(
        {m.qualname: m.deps for m, _ in children}
    ).static_order()
    nodes = [qualname_to_node.get(qualname) for qualname in body_order]

    return [node for node in nodes if node is not None]


def _stub_func(
    ctx: _Ctx,
    f: Union[
        BuiltinFunctionType,
        BuiltinMethodType,
        FunctionType,
        MethodType,
    ],
) -> Tuple[_Meta, ast.FunctionDef]:
    sig = inspect.signature(f)

    posonly_args = []
    args = []
    defaults = []
    kwonly_args = []
    kwonly_defaults = []
    vararg = None
    kwarg = None
    for pname, param in sig.parameters.items():
        if param.kind == Parameter.POSITIONAL_ONLY:
            posonly_args.append(ast.arg(arg=pname))
            if param.default is not Parameter.empty:
                defaults.append(ast.Constant(param.default))

        elif param.kind == Parameter.POSITIONAL_OR_KEYWORD:
            args.append(ast.arg(arg=pname))
            if param.default is not Parameter.empty:
                defaults.append(ast.Constant(param.default))

        elif param.kind == Parameter.VAR_POSITIONAL:
            vararg = ast.arg(arg=pname)

        elif param.kind == Parameter.KEYWORD_ONLY:
            kwonly_args.append(ast.arg(arg=pname))
            if param.default is Parameter.empty:
                kwonly_defaults.append(None)
            else:
                kwonly_defaults.append(ast.Constant(param.default))

        elif param.kind == Parameter.VAR_KEYWORD:
            kwarg = ast.arg(arg=pname)

        else:
            raise ValueError()

    args = ast.arguments(
        posonly_args,
        args,
        vararg,
        kwonly_args,
        kwonly_defaults,
        kwarg,
        defaults,
    )

    body = []
    docstring = inspect.getdoc(f)
    if docstring:
        body += [ast.Expr(ast.Constant(docstring, col_offset=ctx.idnt()))]
    body += [ast.Expr(ast.Constant(...))]

    meta = _Meta.for_obj(f, [])
    node = ast.FunctionDef(
        name=f.__name__,
        args=args,
        body=body,
        decorator_list=[],
        returns=sig.return_annotation
        if sig.return_annotation is not Signature.empty
        else None,
        type_comment=None,
        type_params=[],
    )

    return (meta, node)


def _stub_cls(ctx: _Ctx, cls: type) -> Tuple[_Meta, ast.ClassDef]:
    deps = []
    bases = []
    for base in cls.__bases__:
        if base is not object:
            bases.append(ast.Name(base.__qualname__))
            deps.append(base.__module__ + base.__qualname__)

    body = []
    docstring = inspect.getdoc(cls)
    if docstring:
        body += [ast.Expr(ast.Constant(docstring, col_offset=ctx.idnt()))]
    body += [ast.Expr(ast.Constant(...))]

    children = [
        _stub_obj(ctx.new_scope(), obj)
        for n, obj
        # Do not list out inherited items.
        in cls.__dict__.items()
        if _is_public(n)
    ]

    deps += _raise_deps(children)
    body += _sort_children(children)

    meta = _Meta.for_obj(cls, deps)
    node = ast.ClassDef(
        name=cls.__name__,
        bases=bases,
        keywords=[],
        body=body,
        decorator_list=[],
        type_params=[],
    )

    return (meta, node)


def _stub_getsetdescriptor(
    ctx: _Ctx, gsd: GetSetDescriptorType
) -> Tuple[_Meta, ast.FunctionDef]:
    args = ast.arguments(
        posonlyargs=[],
        args=[ast.arg("self")],
        vararg=None,
        kwonlyargs=[],
        kwonly_defaults=[],
        kwarg=None,
        defaults=[],
    )

    body = []
    docstring = inspect.getdoc(gsd)
    if docstring:
        body += [ast.Expr(ast.Constant(docstring, col_offset=ctx.idnt()))]
    body += [ast.Expr(ast.Constant(...))]

    meta = _Meta.for_obj(gsd, [])
    node = ast.FunctionDef(
        name=gsd.__name__,
        args=args,
        body=body,
        decorator_list=[ast.Name("property", ast.Load())],
        returns=None,
        type_comment=None,
        type_params=[],
    )

    return (meta, node)


def _stub_obj(ctx: _Ctx, obj: object) -> Tuple[_Meta, ast.AST]:
    if (
        inspect.isfunction(obj)
        or
        # PyO3 native pyfunctions are "builtins".
        inspect.isbuiltin(obj)
        or
        # Supports class methods.
        inspect.ismethod(obj)
    ):
        return _stub_func(ctx, obj)
    elif inspect.isclass(obj):
        return _stub_cls(ctx, obj)
    # PyO3 `#[pyo3(get)]` produces these native "properties".
    elif inspect.isgetsetdescriptor(obj):
        return _stub_getsetdescriptor(ctx, obj)
    else:
        msg = f"unknown type in module: {type(obj)!r}"
        raise TypeError(msg)


def _stub_mod(mod: ModuleType) -> ast.Module:
    ctx = _Ctx()

    body = []
    docstring = inspect.getdoc(mod)
    if docstring:
        body += [ast.Expr(ast.Constant(docstring, col_offset=ctx.idnt()))]

    try:
        a = mod.__all__
    except NameError:
        a = list(filter(_is_public, dir(mod)))

    children = [
        _stub_obj(ctx.new_scope(), obj) for n, obj in inspect.getmembers(mod) if n in a
    ]
    body += _sort_children(children)

    # TODO: Handle imports if classes have superclasses that are
    # imported or functions have type hints that are imported.
    # Fortunately PyO3 doesn't support any of this yet so this won't
    # be needed.
    imports = []

    return ast.Module(body=imports + body, type_ignores=[])


def _indent_docstring(s: str, by_spaces: int) -> str:
    prefix = " " * by_spaces
    lines = s.splitlines()
    # Don't indent first line.
    lines[1:] = [prefix + line if line else line for line in lines[1:]]
    if len(lines) > 1:
        lines.append("")
        lines.append(prefix)
    return "\n".join(lines)


class _DocstringReIndenter(ast.NodeVisitor):
    def visit_Constant(self, node: ast.Constant):
        # There should be no other constant strings in the stub other
        # than docstrings.
        if isinstance(node.value, str):
            node.value = _indent_docstring(node.value, node.col_offset)


def _main():
    p = ArgumentParser(description="Generate a stub `.pyi` file for a Python module.")
    p.add_argument("module", help="module to import; not a file")
    p.add_argument(
        "-o", "--output", default="-", help="path to write; defaults to stdout"
    )
    args = p.parse_args()

    mod = importlib.import_module(args.module)

    stub_mod_ast = _stub_mod(mod)
    ast.fix_missing_locations(stub_mod_ast)
    _DocstringReIndenter().visit(stub_mod_ast)

    stub_src = f"# Programmatically generated stubs for `{args.module}`.\n\n"
    stub_src += unparse(stub_mod_ast)

    if args.output != "-":
        with open(args.output, "wt") as out:
            out.write(stub_src)
    else:
        print(stub_src)


if __name__ == "__main__":
    _main()
