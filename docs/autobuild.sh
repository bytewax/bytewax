#!/bin/sh

set -ex

sphinx-autobuild --watch ../pysrc --ignore api -E . _build
