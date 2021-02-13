#!/bin/bash

cd $(git rev-parse --show-toplevel)

apt install librsvg2-dev -y
make -C open_elevation/smrender
make install -C open_elevation/smrender
ldconfig