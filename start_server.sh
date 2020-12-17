#!/bin/bash

mkdir -p data/celery/logs
rm -rf data/celery/pid
mkdir -p data/celery/pid

rabbitmq-server -detached

celery -A tasks \
       multi start tasks_worker \
       -l INFO \
       --pidfile='data/celery/pid/%n.pid' \
       --logfile='data/celery/logs/%n%I.log'

python3 server.py
