#!/bin/bash

cd $(git rev-parse --show-toplevel)

celery -A tasks \
       multi stop tasks_worker \
       -l INFO \
       --pidfile='data/celery/pid/%n.pid' \
       --logfile='data/celery/logs/%n%I.log'
