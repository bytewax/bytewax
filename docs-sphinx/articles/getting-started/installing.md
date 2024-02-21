(installing)=
# Installing

Bytewax currently supports the following versions of Python: 3.8, 3.9,
3.10 and 3.11.

## Installing with pip

We recommend creating a virtual environment for your project when
installing Bytewax.

For more information on setting up a virtual environment, see the
[official Python documentation for virtual
environments](inv:python#library/venv).

Once you have your environment set up, you can install the latest
version of Bytewax with:


```console
$ pip install bytewax
```

We recommend that you pin the version of Bytewax that you are using in
your project by specifying the version of Bytewax that you wish to
install:

```console
$ pip install bytewax==0.18
```

When upgrading between versions of Bytewax, be sure to review the
[Changelog](https://github.com/bytewax/bytewax/blob/main/CHANGELOG.md)
and our <project:#migration> before updating.

(lsp)=
## Support for Type Checking and LSPs

Bytewax includes support for type hints. You can use your editor or
IDEs integration with [mypy](https://mypy.readthedocs.io/en/stable/),
[Pyright](https://microsoft.github.io/pyright/), or [VSCode
Pylance](https://marketplace.visualstudio.com/items?itemName=ms-python.vscode-pylance)
to enable type checking while you are editing. Enabling type checking
can help you catch errors when developing your dataflows.

```python
import bytewax.operators as op

from bytewax.dataflow import Dataflow
from bytewax.testing import TestingSource

flow = Dataflow("integers")
# The return type of `op.input` is "bytewax.dataflow.Stream[builtins.str]"
stream = op.input("inp", flow, TestingSource(["one"]))

# mypy will error here: "Unsupported operand types for + ("str" and "int")"
stream = op.map("string", stream, lambda x: x + 1)
```
