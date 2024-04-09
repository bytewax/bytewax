# Local Development

Here we'll describe how to get a reproducible local development
environment setup.

## Installing `pyenv`

We want Bytewax to run under many different versions of the Python
interpreter, so we need a quick way to create virtualenvs in different
versions. I suggest using [`pyenv`](https://github.com/pyenv/pyenv).
Follow the instructions there to install it.

(xref-dev-venv)=
## Setup a Virtual Environment

I keep all of my different versioned virtualenvs in a `venv` dir. This
is already git-ignored.

```console
$ mkdir venv
$ python310 -m virtualenv venv/dev-3.10
```

[`pip-tools`](https://pip-tools.readthedocs.io/en/stable/) is a small
set of tools on top of Python's built-in
[`pip`](https://pip.pypa.io/en/stable/) which allow fully reproducible
virtual environments which we use to ensure that the versions of
dependencies and test runners you use locally match what's in CI.

```console
$ ./venv/dev-3.10/bin/activate
(dev-3.10) $ pip install pip-tools
```

### Make Reproducible Virtual Environment

Now lets install all of the exact dependencies that will be used.
`pip-tools` adds a `pip-sync` command that makes the virtualenv
perfectly match what is in this requirements file. This installs
everything to build Bytewax, run tests, build docs.

```console
(dev-3.10) $ pip-sync requirements/dev.txt
```

## Compiling Bytewax

To have Maturin compile the local version of your Rust code and build
the library and install it in the currently activated virtualenv run:

```console
(dev-3.10) $ maturin develop
```

## Generating Stubs

Some things like LSP auto-complete and docstrings need to come from a
stub file, which is Python file with no executable code, just
interfaces. Maturin / PyO3 can't generate these automatically, so we
have a reflection script which does it. You should update stubs
whenever you modify the Rust code:

```console
(dev-3.10) $ python stubgen.py bytewax._bytewax -o pysrc/bytewax/_bytewax.pyi
(dev-3.10) $ ruff pysrc/bytewax/_bytewax.pyi
```

Stubs should be committed to the repository.

(xref-dev-deps)=
## Adding Dependencies

:::{warning}

We're cheating and should have a dependency tree for every Python
version. It doesn't seem like any of our deps change depending on
Python version, so this seems ok, but in the future it might not be.

:::

### For Library

To add new dependencies that the Bytewax library requires, add them to
the `dependencies` list in `pyproject.toml`. You should _not pin a
specific version here_, but instead bound by the lowest version that
the calling code in Bytewax supports. This is because Bytewax is a
library that other people will be including in their applications and
so they might have more specific version requirements for their
deployment.

After updating, you need to re-compile the deps and commit changes.

#### Optional Dependencies



### For Development Environment

If you want to add a dependency that isn't required to use the Bytewax
library, but now is needed for a fully working development
environment, add a line to the appropriate file in
`requirements/*.in`. The files that end in `.in` are the "unpinned"
description of the top-level requirements.

After updating, you need to re-compile the deps and commit changes.

## Upgrading Dependencies

See [`pip-compile`
documentation](https://pip-tools.readthedocs.io/en/stable/#updating-requirements)
for how to update a dep to a newer version. We should do this
periodically to check compatibility with new versions. Note that if
you want to say the we require a minimum version, you have to follow
the instructions in <project:#xref-dev-deps> to adjust the
constraints.

## Compiling Dependencies

Whenever you update dependency constraints, we need to _pin specific
versions_ of those dependencies so that when we run CI, the
environment is reproducible. In a sense, our CI infrastructure is a
deployment. We have a script that re-compiles all the deps in the
correct order.

```console
(dev-3.10) $ cd requirements/
(dev-3.10) $ ./compile.sh
```

Then check in all updates to the `requirements/` directory.
