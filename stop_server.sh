#!/bin/bash

celery -A tasks \
       multi stop tasks_worker \
       -l INFO \
       --pidfile='data/celery/pid/%n.pid' \
       --logfile='data/celery/logs/%n%I.log'
