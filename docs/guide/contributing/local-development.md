# Local Development

Here we'll describe how to get a reproducible local development
environment setup.

## Get Maturin and `pip-tools`

[Maturin](https://www.maturin.rs/) is [PyO3](https://pyo3.rs/)'s build
tool. It knows how to compile our Rust code and package it into a
binary Python package.

[`pip-tools`](https://pip-tools.readthedocs.io/en/stable/) is a small
set of tools on top of Python's built-in
[`pip`](https://pip.pypa.io/en/stable/) which allow fully reproducible
virtual environments which we use to ensure that the versions of
dependencies and test runners you use locally match what's in CI.

I recommend installing both of these globally using
[`pipx`](https://pipx.pypa.io/stable/) since the version of them
doesn't really matter. `pipx` provides a controlled way of having each
command live in it's own virtualenv so you don't pollute your global
system Python package space.

```console
$ pipx install maturin pip-tools
```

(xref-venv)=
## Setup a Virtual Environment

```console
$ take venv
$ python310 -m virtualenv venv/dev-3.10
```

```console
$ ./venv/dev-3.10/bin/activate
(dev-3.10) $
```

### Make Reproducible Virtual Environment

```console
(dev-3.10) $ pip-sync requirements/dev.txt
```

## Generating Stubs

```console
(dev-3.10) $ python stubgen.py bytewax._bytewax -o pysrc/bytewax/_bytewax.pyi
(dev-3.10) $ ruff pysrc/bytewax/_bytewax.pyi
```

## Adding Dependencies

:::{warning}

We're cheating and should have a dependency tree for every Python
version.

:::

Then check in all updates to the `requirements/` directory.

### For Library

To add new dependencies that the Bytewax library requires, add them to
the `dependencies` list in `pyproject.toml`. You should _not pin a
specific version here_, but instead bound by the lowest version that
the calling code in Bytewax supports. This is because Bytewax is a
library that other people will be including in their applications and
so they might have more specific version requirements for their
deployment.

Once you do that, we need to _pin specific versions_ of those
dependencies so that when we run CI, the environment is reproducible.
Notice this is the exact opposite of above. In a sense, our CI
infrastructure is a deployment. Use `pip-compile` to pin all of the
library's deps.

```console
(dev-3.10) $ pip-compile -o requirements/library.txt pyproject.toml
(dev-3.10) $ pip-compile requirements/*.in
```

#### Optional Dependencies



### For Development Environment

If you want to add a dependency that isn't required to use the Bytewax
library, but now is needed for a fully working development
environment, add a line to the appropriate file in
`requirements/*.in`. The files that end in `.in` are the "unpinned"
description of the top-level requirements.

Once you do that, you need to use `pip-compile` to again pin all the
specific versions to make the environment reproducible. The command is
similar to above. This will update all `requirements/*.txt` with the
new exact deps.

```console
(dev-3.10) $ pip-compile requirements/*.in
```

## Upgrading Dependencies
