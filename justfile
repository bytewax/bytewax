# Show this help list
help:
    @echo 'If you are new here, run `just get-started` to init a development env.'
    @echo 'See https://docs.bytewax.io/stable/guide/contributing/contributing.html for a more detailed tutorial.'
    @just --list

# Init a development env
get-started:
    @echo 'Checking that you have `uv` installed'
    @echo 'If you need it, I recommend installing `pipx` from https://pipx.pypa.io/stable/ then `pipx install uv`'
    uv --version
    @echo 'Checking that you have Python 3.12 installed'
    @echo 'If you need it, I recommend installing `pyenv` from https://github.com/pyenv/pyenv then `pyenv install 3.12`'
    @echo 'You also might need to activate the global shim with `pyenv global system 3.12`'
    python3.12 --version
    @echo 'Creating the development virtual env in `venvs/dev`'
    mkdir -p venvs
    test -d venvs/dev/ || uv venv -p 3.12 venvs/dev/
    @echo 'Installing all the tools and dependencies'
    just sync dev
    @echo 'Ensuring pre-commit hooks are installed'
    venvs/dev/bin/pre-commit install
    @echo 'All done!'
    @echo 'Each time before you do any work in this repo you should run `. venvs/dev/bin/activate`'
    @echo 'Once the `dev` venv is activated, run:'
    @echo
    @echo '`just develop` to re-build Bytewax and install it in the venv'
    @echo '`just py-test` to run the Python test suite'
    @echo '`just lint` to lint the source code'
    @echo '`just --list` to show more advanced recipes'


# Re-build the Rust portion of the library and install in the local venv
develop: && stubgen
    # You never need to run with `-E` / `--extras`; the `dev` and test
    # virtualenvs already have the optional dependencies pinned.
    maturin develop

# Re-generate stub file of PyO3 parts of the library
stubgen:
    python stubgen.py bytewax._bytewax -o pysrc/bytewax/_bytewax.pyi
    ruff format pysrc/bytewax/_bytewax.pyi

# Lint the code in the repo; run in CI
lint:
    vermin pysrc/ pytests/ docs/ examples/ *.py
    ruff check pysrc/ pytests/ docs/ examples/ *.py
    # TODO: Add `examples/` to mypy checking. Will require a lot of
    # fixup?
    mypy pysrc/ pytests/ docs/ *.py
    cargo clippy

# Run the Rust tests; run in CI
test-rs:
    cargo test --no-default-features

# Run the Python tests; run in CI
test-py:
    pytest --benchmark-skip pytests/

# Run the Python benchmarks; run in CI
test-benchmark:
    pytest --codspeed pytests/

# Test all code in the documentation; run in CI
test-doc:
    sphinx-build -b doctest -E docs/ docs/_build/

# Run all the checks that will be run in CI locally
ci-pre: lint test-py test-rs test-doc test-benchmark

# CI uses this command to run all non-Python tests
_test-repo: lint test-rs test-doc

# Start an auto-refreshing doc development server
doc-autobuild:
    sphinx-autobuild -E docs/ docs/_build/

# Build the docs into static HTML files
doc-build:
    sphinx-build -b html -E docs/ docs/_build/

# Init the Read the Docs venv; only use if you are debugging RtD; use `just doc-autobuild` to write docs locally
venv-init-doc:
    mkdir -p venvs
    test -d venvs/doc/ || uv venv -p 3.12 venvs/doc/

# Init the CI Python test venvs; only use if you are debugging CI test dependencies; use `just py-test` to run tests locally
venv-init-test:
    mkdir -p venvs
    test -d venvs/test-py3.8/ || uv venv -p 3.8 venvs/test-py3.8/
    test -d venvs/test-py3.9/ || uv venv -p 3.9 venvs/test-py3.9/
    test -d venvs/test-py3.10/ || uv venv -p 3.10 venvs/test-py3.10/
    test -d venvs/test-py3.11/ || uv venv -p 3.11 venvs/test-py3.11/
    test -d venvs/test-py3.12/ || uv venv -p 3.12 venvs/test-py3.12/

# Sync the given venv; e.g. `dev` or `test-py3.10`
venv-sync venv:
    VIRTUAL_ENV={{justfile_directory()}}/venvs/{{venv}} uv pip sync --strict requirements/{{venv}}.txt

# Sync all venvs
venv-sync-all: (venv-sync "doc") (venv-sync "test-py3.8") (venv-sync "test-py3.9") (venv-sync "test-py3.10") (venv-sync "test-py3.11") (venv-sync "test-py3.12") (venv-sync "dev")

# Pin / compile all dependences for reproducible venvs; re-run this if you update any library deps or `.in` files
venv-compile-all:
    uv pip compile --generate-hashes -p 3.12 requirements/doc.in -o requirements/doc.txt

    uv pip compile --generate-hashes -p 3.8 --all-extras pyproject.toml -o requirements/lib-py3.8.txt
    uv pip compile --generate-hashes -p 3.9 --all-extras pyproject.toml -o requirements/lib-py3.9.txt
    uv pip compile --generate-hashes -p 3.10 --all-extras pyproject.toml -o requirements/lib-py3.10.txt
    uv pip compile --generate-hashes -p 3.11 --all-extras pyproject.toml -o requirements/lib-py3.11.txt
    uv pip compile --generate-hashes -p 3.12 --all-extras pyproject.toml -o requirements/lib-py3.12.txt

    uv pip compile --generate-hashes -p 3.8 requirements/test.in requirements/lib-py3.8.txt -o requirements/test-py3.8.txt
    uv pip compile --generate-hashes -p 3.9 requirements/test.in requirements/lib-py3.9.txt -o requirements/test-py3.9.txt
    uv pip compile --generate-hashes -p 3.10 requirements/test.in requirements/lib-py3.10.txt -o requirements/test-py3.10.txt
    uv pip compile --generate-hashes -p 3.11 requirements/test.in requirements/lib-py3.11.txt -o requirements/test-py3.11.txt
    uv pip compile --generate-hashes -p 3.12 requirements/test.in requirements/lib-py3.12.txt -o requirements/test-py3.12.txt

    uv pip compile --generate-hashes -p 3.12 requirements/dev.in requirements/lib-py3.12.txt -o requirements/dev.txt
