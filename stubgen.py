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
from typing import Any, Union

import astor

_INDENT_SPACES = 4


def _is_public(name: str) -> bool:
    return not name.startswith("_")


@dataclass(frozen=True)
class _Ctx:
    col_offset: int = 0

    def idnt(self):
        return self.col_offset + _INDENT_SPACES

    def new_scope(self):
        return dataclasses.replace(
            self,
            col_offset=self.col_offset + _INDENT_SPACES,
        )


def _stub_func(
    f: Union[
        BuiltinFunctionType,
        BuiltinMethodType,
        FunctionType,
        MethodType,
    ],
    ctx: _Ctx,
) -> ast.FunctionDef:
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

    return ast.FunctionDef(
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


def _stub_cls(cls: type, ctx: _Ctx) -> ast.ClassDef:
    bases = []
    for base in cls.__bases__:
        if base is not object:
            bases.append(ast.Name(base.__name__))

    body = []
    docstring = inspect.getdoc(cls)
    if docstring:
        body += [ast.Expr(ast.Constant(docstring, col_offset=ctx.idnt()))]
    body += [ast.Expr(ast.Constant(...))]
    body += [
        _stub_obj(n, obj, ctx.new_scope())
        for n, obj
        # Stubs for inherited items will come through the base class.
        in cls.__dict__.items()
        if _is_public(n)
    ]

    return ast.ClassDef(
        name=cls.__name__,
        bases=bases,
        keywords=[],
        body=body,
        decorator_list=[],
        type_params=[],
    )


def _stub_getsetdescriptor(gsd: GetSetDescriptorType, ctx: _Ctx) -> ast.FunctionDef:
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

    return ast.FunctionDef(
        name=gsd.__name__,
        args=args,
        body=body,
        decorator_list=[ast.Name("property", ast.Load())],
        returns=None,
        type_comment=None,
        type_params=[],
    )


def _stub_obj(n: str, obj: Any, ctx: _Ctx) -> ast.AST:
    if (
        inspect.isfunction(obj)
        or
        # PyO3 native pyfunctions are "builtins".
        inspect.isbuiltin(obj)
        or
        # Supports class methods.
        inspect.ismethod(obj)
    ):
        return _stub_func(obj, ctx)
    elif inspect.isclass(obj):
        return _stub_cls(obj, ctx)
    # PyO3 `#[pyo3(get)]` produces these native "properties".
    elif inspect.isgetsetdescriptor(obj):
        return _stub_getsetdescriptor(obj, ctx)
    else:
        msg = f"unknown type in module: {n} is {type(obj)!r}"
        raise TypeError(msg)


def _stub_mod(mod: ModuleType, ctx: _Ctx) -> ast.Module:
    body = []
    docstring = inspect.getdoc(mod)
    if docstring:
        body += [ast.Expr(ast.Constant(docstring, col_offset=ctx.idnt()))]

    try:
        a = mod.__all__
    except NameError:
        a = filter(_is_public, dir(mod))

    ls = [(n, obj) for n, obj in inspect.getmembers(mod) if n in a]
    body += [_stub_obj(n, obj, ctx) for n, obj in ls]

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
    lines.append(prefix)
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

    ctx = _Ctx()
    stub_mod_ast = _stub_mod(mod, ctx)
    ast.fix_missing_locations(stub_mod_ast)
    _DocstringReIndenter().visit(stub_mod_ast)

    stub_src = f"# Programmatically generated stubs for `{args.module}`.\n\n"
    # If we drop support for 3.8, can use `ast.unparse` and not need
    # the `astor` dep.
    stub_src += astor.to_source(stub_mod_ast)

    if args.output != "-":
        with open(args.output, "wt") as out:
            out.write(stub_src)
    else:
        print(stub_src)


if __name__ == "__main__":
    _main()
