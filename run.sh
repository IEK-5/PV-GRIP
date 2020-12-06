#!/bin/bash

docker run -t -i \
       -v $(pwd)/data:/code/data \
       -p 8080:8080 \
       elevation $@
