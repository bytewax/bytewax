cd /bytewax
. /venv/bin/activate
pip install .[dev]
cd apidocs
rm -rf html
mkdir html
pdoc --html --template-dir ./templates -o ./html bytewax
