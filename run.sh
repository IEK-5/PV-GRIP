#!/bin/bash

docker run -t -i \
       --mount type=bind,source=$(pwd),target=/src \
       -v $(pwd)/data:/code/data \
       -p 8080:8080 \
       elevation
