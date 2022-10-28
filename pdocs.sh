# Build the apidocs image
docker build -f apidocs/Dockerfile -t bytewax-pdocs .
# Clean up apidocs/html so we can mount it
rm -rf apidocs/html
mkdir apidocs/html
# Run the image, the entrypoint will do the rest
docker run --rm -v "$(pwd)":/bytewax bytewax-pdocs:latest
