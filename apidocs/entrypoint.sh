# Entrypoint to rebuild docs in the docker image
# created with apidocs/Dockerfile
# Move to the /bytewax directory, where the folder containing
# this repo should be mounted
cd /bytewax
# Activate the virtualenv
. /venv/bin/activate
# Install bytewax and all the dependencies
pip install .[dev]
# Rebuild docs
cd apidocs
pdoc --html --template-dir ./templates -o ./html bytewax
