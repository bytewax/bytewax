#!/bin/sh

set -ex

pip-compile docs.in

pip-compile -o library.txt ../pyproject.toml
pip-compile test.in
pip-compile dev.in

pip-compile pre-commit.in
