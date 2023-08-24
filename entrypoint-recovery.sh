#!/bin/sh

FILE=$BYTEWAX_RECOVERY_DIRECTORY/part-0.sqlite3
if [ -f "$FILE" ]; then
    echo "$FILE exists."
else
  . /venv/bin/activate
  python -m bytewax.recovery $BYTEWAX_RECOVERY_DIRECTORY $BYTEWAX_RECOVERY_PARTS
fi
