# Show this help list
help:
    @echo 'If you are new here, run `just get-started` to init a development env.'
    @echo 'See https://docs.bytewax.io/stable/guide/contributing/local-development.html for a more detailed tutorial.'
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
    @echo 'Creating the development virtual env in `venvs/dev/`'
    mkdir -p venvs
    test -d venvs/dev/ || uv venv -p 3.12 venvs/dev/
    @echo 'Installing all the tools and dependencies'
    just venv-sync dev
    @echo 'Ensuring Git pre-commit hooks are installed'
    venvs/dev/bin/pre-commit install
    @echo 'All done!'
    @echo 'Each time before you do any work in this repo you should run `. venvs/dev/bin/activate`'
    @echo 'Once the `dev` venv is activated, run:'
    @echo
    @echo '`just develop` to re-build Bytewax and install it in the venv'
    @echo '`just py-test` to run the Python test suite'
    @echo '`just lint` to lint the source code'
    @echo '`just --list` to show more advanced recipes'

# Assert we are in a venv.
_assert-venv:
    #!/usr/bin/env python
    import sys
    p = sys.prefix
    if not (p.endswith("venvs/dev") or p.endswith("venv")):
        print("You must activate the `dev` venv with `. venvs/dev/bin/activate` before running this command", file=sys.stderr)
        sys.exit(1)

# Re-build the Rust portion of the library and install it
develop: _assert-venv && stubgen
    @# You never need to run with `-E` / `--extras`; the `dev` and test
    @# virtualenvs already have the optional dependencies pinned.
    maturin develop

# Build a release wheel for the current Python version and put it in `dist/`
build: _assert-venv
    RUSTFLAGS="-C force-frame-pointers=yes" maturin build --release -o dist/

# Re-generate stub file of PyO3 parts of the library; automatically runs as part of `just develop`
stubgen: _assert-venv
    python stubgen.py bytewax._bytewax -o pysrc/bytewax/_bytewax.pyi
    ruff format pysrc/bytewax/_bytewax.pyi

# Format a Python file; automatically run via pre-commit
fmt-py *files: _assert-venv
    ruff format {{files}}

# Format code blocks in a Markdown file; automatically run via pre-commit
fmt-md *files: _assert-venv
    cbfmt -w {{files}}

# Lint the code in the repo; runs in CI
lint: _assert-venv
    vermin --config-file vermin-lib.ini pysrc/ pytests/ examples/
    vermin --config-file vermin-dev.ini docs/ *.py
    ruff check pysrc/ pytests/ docs/ examples/ *.py
    # TODO: Add `examples/` to mypy checking. Will require a lot of
    # fixup?
    mypy pysrc/ pytests/ docs/ *.py
    cargo clippy

# Manually check that all pre-commit hooks pass; runs in CI
lint-pc: _assert-venv
    pre-commit run --all-files --show-diff-on-failure

# Run the Rust tests; runs in CI
test-rs:
    cargo test --no-default-features

pytests := 'pytests/'

# Run the Python tests; runs in CI
test-py tests=pytests: _assert-venv
    pytest --benchmark-skip {{tests}}

# Run the Python benchmarks; runs in CI
test-benchmark:
    pytest --codspeed pytests/

# Test all code in the documentation; runs in CI
test-doc: _assert-venv
    cd docs/fixtures/ && sphinx-build -b doctest -E .. ../_build/

# Run all the checks that will be run in CI locally
ci-pre: lint test-py test-rs test-doc test-benchmark

# Start an auto-refreshing doc development server
doc-autobuild: _assert-venv
    sphinx-autobuild -T --ignore '**/.*' -E docs/ docs/_build/

# Build the docs into static HTML files in `docs/_build/`
doc-build: _assert-venv
    sphinx-build -T -b html -E docs/ docs/_build/

# Init the Read the Docs venv; only use if you are debugging RtD; use `just doc-autobuild` to write docs locally
venv-init-doc:
    mkdir -p venvs
    test -d venvs/doc/ || uv venv -p 3.12 venvs/doc/

# Init the CI Python build venvs; only use if you are debugging CI build dependencies
venv-init-build:
    mkdir -p venvs
    test -d venvs/build-py3.8/ || uv venv -p 3.8 venvs/build-py3.8/
    test -d venvs/build-py3.9/ || uv venv -p 3.9 venvs/build-py3.9/
    test -d venvs/build-py3.10/ || uv venv -p 3.10 venvs/build-py3.10/
    test -d venvs/build-py3.11/ || uv venv -p 3.11 venvs/build-py3.11/
    test -d venvs/build-py3.12/ || uv venv -p 3.12 venvs/build-py3.12/

# Sync the given venv; e.g. `dev` or `build-py3.10`
venv-sync venv:
    VIRTUAL_ENV={{justfile_directory()}}/venvs/{{venv}} uv pip sync --strict requirements/{{venv}}.txt

# Sync all venvs
venv-sync-all: (venv-sync "doc") (venv-sync "build-py3.8") (venv-sync "build-py3.9") (venv-sync "build-py3.10") (venv-sync "build-py3.11") (venv-sync "build-py3.12") (venv-sync "dev")

# Pin / compile all dependences for reproducible venvs; re-run this if you update any library deps or `.in` files
venv-compile-all:
    # TODO: Disabling the generation of hashes until we don't depend on a VCS url for
    # autodoc2
    uv pip compile -p 3.12 requirements/doc.in -o requirements/doc.txt

    uv pip compile --generate-hashes -p 3.8 --all-extras pyproject.toml -o requirements/lib-py3.8.txt
    uv pip compile --generate-hashes -p 3.9 --all-extras pyproject.toml -o requirements/lib-py3.9.txt
    uv pip compile --generate-hashes -p 3.10 --all-extras pyproject.toml -o requirements/lib-py3.10.txt
    uv pip compile --generate-hashes -p 3.11 --all-extras pyproject.toml -o requirements/lib-py3.11.txt
    uv pip compile --generate-hashes -p 3.12 --all-extras pyproject.toml -o requirements/lib-py3.12.txt

    uv pip compile --generate-hashes -p 3.8 requirements/build.in requirements/lib-py3.8.txt -o requirements/build-py3.8.txt
    uv pip compile --generate-hashes -p 3.9 requirements/build.in requirements/lib-py3.9.txt -o requirements/build-py3.9.txt
    uv pip compile --generate-hashes -p 3.10 requirements/build.in requirements/lib-py3.10.txt -o requirements/build-py3.10.txt
    uv pip compile --generate-hashes -p 3.11 requirements/build.in requirements/lib-py3.11.txt -o requirements/build-py3.11.txt
    uv pip compile --generate-hashes -p 3.12 requirements/build.in requirements/lib-py3.12.txt -o requirements/build-py3.12.txt

    uv pip compile --generate-hashes -p 3.12 requirements/dev.in requirements/lib-py3.12.txt -o requirements/dev.txt
