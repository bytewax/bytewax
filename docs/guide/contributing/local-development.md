(xref-dev-env)=
# Local Development

Here we'll describe how to get a reproducible local development
environment setup.

## Environment Tools

### Install Rustup

Install a Rust compiler stack. See the [Rust Project's installation
documentation](https://www.rust-lang.org/learn/get-started) on how to
do that using `rustup`.

You don't have to actually install a specific version of the
toolchain, just install `rustup`. The build process below will ensure
you use the correct version.

### Install `just`

We use [`just`](https://just.systems/man/en/) as a command runner for
actions / recipes related to developing Bytewax. Please follow [the
installation
instructions](https://github.com/casey/just?tab=readme-ov-file#installation).
There's probably a package for your OS already.

### Install `pyenv` and Python 3.12

We want Bytewax to run under many different versions of the Python
interpreter, so we need a quick way to create virtualenvs in different
versions. I suggest using [`pyenv`](https://github.com/pyenv/pyenv)
because it will let you re-compile Python easily with benchmarking
flags. Follow [the installation
instructions](https://github.com/pyenv/pyenv?tab=readme-ov-file#installation).

You can also use your OS's package manager to get access to different
Python versions.

Our blessed version of Python for doing development is Python 3.12
(although the Bytewax library is designed to run on versions 3.8+).

Ensure that you have Python 3.12 installed and available as a "global
shim" so that it can be run anywhere. The following will make plain
`python` run your OS-wide interpreter, but will make 3.12 available
via `python3.12`.

```console
$ pyenv global system 3.12
```

### Install `uv`

We use [`uv`](https://github.com/astral-sh/uv) as a virtual
environment creator, package installer, and dependency pin-er. There
are [a few different ways to install
it](https://github.com/astral-sh/uv?tab=readme-ov-file#getting-started),
but I recommend installing it through either
[`brew`](https://brew.sh/) on macOS or
[`pipx`](https://pipx.pypa.io/stable/).

(xref-dev-venv)=
## Setup the Reproducible Development Virtual Environment

We have a "getting started" `just` recipe that will:

1. Set up a venv in `venvs/dev/`.

2. Install all development dependencies into it in a reproducible way.

3. Install Git [pre-commit](https://pre-commit.com/) hooks that use
   that venv.

```console
$ just get-started
```

If it detects anything wrong, follow the instructions.

Whenever you work on Bytewax, you must have the `dev` venv activated.
Most of the other `just` recipes require this. They will error if the
venv is not activated.

:::{note}

Prompts here will be prefixed with `(dev)` to show that the venv is
active.

:::

```console
$ . venvs/dev/bin/activate
(dev) $
```

## LSP and Editors

Since all the development tools are installed into the `dev` venv,
find a way to point VSCode, PyCharm, or whatever IDE / LSP you are
using at the venv located in the `venvs/dev/` directory. It is
important you do this because `pyproject.toml` has settings for how
the LSP should handle various cases and will format or lint or give
type hints incorrectly if a different environment is used.

## Common Actions

Most of our common actions for developing on the library are `just`
recipes.

### Build Library

This will compile the Rust code for the Bytewax library, generate new
stubs, and install it into the current venv. You can then use
`ipython`, or run a dataflow, or run unit tests to try it out.

```console
(dev) $ just develop
```

This will compile the library in _debug mode_ and will have poor
performance. See <project:#xref-profiling> for how to compile for
proper local benchmarking.

### Running a Dataflow

Once you've compiled a changed version of Bytewax, you can run
dataflows in exactly the same way as someone who has installed the
library normally. See <project:#xref-execution>.

### Running Tests

To run the Python or Rust tests use the following recipes.

```console
(dev) $ just test-py
(dev) $ just test-rs
```

### Linting Code

This will run a series of lints on the code:

- Ensure all Python code is compatible with at least Python 3.8.

- Use [`ruff`](https://docs.astral.sh/ruff/) to lint the Python code.

- Run [`mypy`](https://mypy.readthedocs.io/en/stable/) to find type
  errors in the Python code.

- Run [`cargo clippy`](https://doc.rust-lang.org/clippy/) to lint the
  Rust code.

```console
(dev) $ just lint
```

### Prepare for CI

CI automatically will run lints, pre-commit hooks, tests, and
benchmarks. If you want to run them all locally so there (probably)
won't be any surprises when you push you change, you can use the
following recipe.

```console
(dev) $ just ci-pre
```

(xref-dev-docs)=
### Writing Docs

We have a recipe that will build the docs locally, start up a web
server hosting them, and will watch for any changes on Markdown files
and re-build the docs.

```console
(dev) $ just doc-autobuild
Running Sphinx v7.2.6
...
[I 240124 12:03:11 server:335] Serving on http://127.0.0.1:8000
```

The temporary built HTML files are put in `docs/_build/`. This
directory _should not be checked in_. Production docs are built using
Read the Docs and served from them.

This starts a web server on <http://localhost:8000/> with the built
docs and will watch the source files and rebuild on any change.

:::{warning}

The watching mechanism sometimes gets confused and trapped in an
infinite loop, constantly rebuilding the docs on no changes. I think
it has something to do with the fact that the Sphinx build process
generates Markdown files for the API docs.

If you `C-c` it and start it again, it will stop.

:::

(xref-dev-deps)=
## Adding Dependencies

### For Library

To add new dependencies that the Bytewax library requires, add them to
the `dependencies` list in `pyproject.toml`. You should _not pin a
specific version here_, but instead bound by the lowest version that
the calling code in Bytewax supports. This is because Bytewax is a
library that other people will be including in their applications and
so they might have more specific version requirements for their
deployment.

After updating, you need to [re-compile the deps](#xref-dev-compile)
and commit the changes.

#### Optional Dependencies

If there is a feature of the Bytewax library that not everyone will
use and requires a new dependency, you can add it as an **extra**
dependency. First add a new **extra name** to `pyproject.toml` under
the `extras` list. Then add a list of dependencies under
`project.optional-dependencies`. E.g. for adding Kafka functionality:

```yaml
extras = [
    "kafka",
]

[project.optional-dependencies]
kafka = [
    "requests>=2.0",
    "fastavro>=1.8",
    "confluent-kafka>=2.0.2",
]
```

### Doctest Dependencies

If your documentation relies on other dependencies, you
need to add them under `requirements/dev.in`, then
[re-compile the dependencies](#xref-dev-compile) and
commit the changes.

### For Development Environment

If you want to add a dependency that isn't required to use the Bytewax
library, but now is needed for a fully working development
environment, add a line to the appropriate file in
`requirements/dev.in`. The files that end in `.in` are the "unpinned"
description of the top-level requirements.

After updating, you need to [re-compile the deps](#xref-dev-compile)
and commit the changes.

(xref-dev-compile)=
## Compiling Dependencies

Whenever you update dependency constraints, we need to _lock specific
versions_ of those dependencies so that when we run CI, the
environment is reproducible. In a sense, our CI infrastructure is a
deployment. This is called **compiling** the dependencies and we use
`uv` to do that. We have a `just` recipe that re-compiles all the deps
in the correct order.

```console
(dev) $ just venv-compile-all
(dev) $ git add requirements/
```

Then commit all changes in the `requirements/*.txt` files.

## Upgrading Dependencies

`uv` will not modify a `requirements/*.txt` file just to bump a
package version; it will only modify existing locked versions if they
no longer pass the constraints of the `*.in` files, which only specify
the coarsest constraints on the package versions. If you want to
upgrade all packages to their latest version, you should blow-away all
the `*.txt` files and re-compile them.

```console
(dev) $ rm requirements/*.txt
(dev) $ just venv-compile-all
(dev) $ git add requirements/
```
