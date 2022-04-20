#!/bin/bash

cd $(git rev-parse --show-toplevel)


function set_defaults {
    what="webserver"
}


function print_help {
    echo "Usage: $0 [OPTIONS]"
    echo
    echo "Start webserver/worker nodes"
    echo
    echo "  -h,--help         print this page"
    echo
    echo "  --what            type of node to start (webserver, worker)"
    echo "                    Default: ${what}"
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
}


function _start_celery_string {
    echo --autoscale=$(python3 \
                           scripts/get_config.py \
                           server celery_workers),0 \
         --max-memory-per-child=$(python3 \
                                      scripts/get_config.py \
                                      server \
                                      max_memory_worker) \
         -l $(python3 scripts/get_config.py \
                      server logging_level) \
         --logfile="data/celery/logs/%n$1%I.log" \
         --events
}


function start_worker {
    celery -A pvgrip \
           worker \
           $(_start_celery_string)
}


function start_worker_requests {
    celery -A pvgrip \
           worker \
           -Q requests \
           $(_start_celery_string "_requests")
}


function start_webserver {
    python3 pvgrip/webserver/server.py
}


function start_flower {
    redis_ip=$(python3 scripts/get_config.py redis ip)
    celery -A pvgrip \
           --broker="redis://${redis_ip}:6379/0" \
           flower \
           --persistent=True \
           --db="/code/data/flower"
}


set_defaults
parse_args $@
init_dirs

case "${what}" in
    worker)
        start_worker
        ;;
    worker_requests)
        start_worker_requests
        ;;
    webserver)
        start_webserver
        ;;
    flower)
        start_flower
        ;;
    *)
        ;;
esac
