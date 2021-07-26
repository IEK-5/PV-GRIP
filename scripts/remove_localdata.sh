#!/bin/bash

cd $(git rev-parse --show-toplevel)

rm -rf \
   data/celery/logs \
   data/server.log \
   data/tempfiles \
   data/results_cache
