#!/bin/sh

set -e

# Ignore any currently activated venv and just use the pipenv managed
# one.
export PIPENV_IGNORE_VIRTUALENVS=1

# Run from /apidocs dir.
pipenv sync --dev
pipenv run sphinx-autobuild --watch ../pysrc --ignore apidocs -E . _build
