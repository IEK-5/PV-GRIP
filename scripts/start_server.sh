#!/bin/bash

cd $(git rev-parse --show-toplevel)

rm -rf data/celery/pid
mkdir -p data/celery/logs
mkdir -p data/celery/pid
mkdir -p data/redis
cp redis/redis.conf data/redis/.

redis-server data/redis/redis.conf

celery -A open_elevation.celery_tasks \
       multi start tasks_worker \
       -l INFO \
       --pidfile='data/celery/pid/%n.pid' \
       --logfile='data/celery/logs/%n%I.log'

python3 server.py
