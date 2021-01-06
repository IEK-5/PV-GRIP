#!/bin/bash

cd $(git rev-parse --show-toplevel)

rm -rf data/celery/pid
rm -rf data/memcached/pid
mkdir -p data/celery/logs
mkdir -p data/celery/pid
mkdir -p data/memcached/

rabbitmq-server -detached

memcached -d -u memcache -P data/memcached/pid  \
          -m 2048 -c 1024 \
          -l 127.0.0.1 \
          -o modern,drop_privileges

celery -A open_elevation.celery_tasks \
       multi start tasks_worker \
       -l INFO \
       --pidfile='data/celery/pid/%n.pid' \
       --logfile='data/celery/logs/%n%I.log'

python3 server.py
