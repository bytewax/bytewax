#!/bin/sh

/venv/bin/python $TINYDANCER_PYTHON_FILE_PATH -w $TINYDANCER_WORKERS_PER_PROCESS -h $HOSTFILE_PATH -n $REPLICAS -p $(echo $POD_NAME | sed "s/tiny-dancer-//g")