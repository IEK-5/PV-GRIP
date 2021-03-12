#!/bin/bash

cd $(git rev-parse --show-toplevel)

interface=$(python3 scripts/get_config.py server interface)
bind_ip=$(ip -f inet addr show "${interface}" | awk '/inet/ {print $2}' | cut -d/ -f1)

docker run -d -t -i \
       --name "pvgrip" \
       -v $(pwd)/data:/code/data \
       -p "${bind_ip}":8080:8080 \
       -p "${bind_ip}":6379:6379 \
       elevation $@
