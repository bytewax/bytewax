docker build -f Dockerfile.pdocs -t bytewax-pdocs .
rm -rf apidocs/html
mkdir apidocs/html
docker run --rm --mount type=bind,source="$(pwd)"/apidocs/html,target=/bytewax/apidocs/html
