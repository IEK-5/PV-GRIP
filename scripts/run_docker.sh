#!/bin/bash

cd $(git rev-parse --show-toplevel)

docker run -t -i \
       -v $(pwd)/data:/code/data \
       -p 8080:8080 \
       elevation $@
