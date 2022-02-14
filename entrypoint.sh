#!/bin/sh

cd $BYTEWAX_WORKDIR
/venv/bin/python $BYTEWAX_PYTHON_FILE_PATH -w $BYTEWAX_WORKERS_PER_PROCESS -h $BYTEWAX_HOSTFILE_PATH -n $BYTEWAX_REPLICAS -p $(echo $BYTEWAX_POD_NAME | sed "s/$BYTEWAX_STATEFULSET_NAME-//g")

echo 'Process ended.'

if [ "$BYTEWAX_KEEP_CONTAINER_ALIVE" = true ]
then
    echo 'Keeping container alive...';
    while :; do sleep 1; done
fi
