#!/bin/sh

cd $BYTEWAX_WORKDIR
/venv/bin/python $BYTEWAX_PYTHON_FILE_PATH

echo 'Process ended.'

if [ "$BYTEWAX_KEEP_CONTAINER_ALIVE" = true ]
then
    echo 'Keeping container alive...';
    while :; do sleep 1; done
fi
