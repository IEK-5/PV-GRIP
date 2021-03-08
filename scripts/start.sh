#!/bin/bash

cd $(git rev-parse --show-toplevel)


function set_defaults {
    what="master"
    action="start"
}


function print_help {
    echo "Usage: $0 [OPTIONS]"
    echo
    echo "Start master/minion nodes"
    echo
    echo "  -h,--help         print this page"
    echo
    echo "  --what            type of node to start (master or minion)"
    echo "                    Default: ${what}"
    echo
    echo "  --action          start/stop"
    echo "                    Default: ${action}"
    echo
}


function parse_args {
    for i in "$@"
    do
        case "${i}" in
            --what=*)
                what="${i#*=}"
                shift
                ;;
            -h|--help)
                print_help
                exit
        esac
    done
}


function init_dirs {
    rm -rf data/celery/pid
    mkdir -p data/celery/logs
    mkdir -p data/celery/pid
    mkdir -p data/redis
    cp configs/redis.conf data/redis/.
}


function start_redis {
    redis-server data/redis/redis.conf
}


function start_celery {
    celery -A open_elevation.celery_tasks \
           multi start tasks_worker \
           --concurrency=$(python3 scripts/get_config.py server celery_workers) \
           -l INFO \
           --pidfile='data/celery/pid/%n.pid' \
           --logfile='data/celery/logs/%n%I.log'
}


function stop_celery {
    celery -A open_elevation.celery_tasks \
           multi stop tasks_worker \
           -l INFO \
           --pidfile='data/celery/pid/%n.pid' \
           --logfile='data/celery/logs/%n%I.log'
}


function start_server {
    python3 open_elevation/server.py
}


set_defaults
parse_args $@
init_dirs


if [ "master" == "${what}" ] && [ "start" == "${action}" ]
then
    start_redis
    start_celery
    start_server
else
    start_celery
fi


if [ "stop" == "${action}" ]
then
    stop_celery
fi
