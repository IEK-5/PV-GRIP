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
         --logfile='data/celery/logs/%n%I.log'
}


function start_worker {
    celery -A open_elevation.celery_tasks \
           worker \
           $(_start_celery_string)
}


function start_webserver {
    python3 open_elevation/server.py
}


set_defaults
parse_args $@
init_dirs


case "${what}" in
    worker)
        start_worker
        ;;
    webserver)
        start_webserver
        ;;
    *)
        ;;
esac
