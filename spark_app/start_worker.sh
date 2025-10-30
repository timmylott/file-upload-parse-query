#!/bin/bash
# Start RQ worker for 'file_tasks' queue
# This will keep running in foreground

export PYTHONPATH=/home/iceberg/app:$PYTHONPATH
rq worker -u redis://redis:6379 file_tasks
