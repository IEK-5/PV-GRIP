#!/bin/bash

cd $(git rev-parse --show-toplevel)

apt install -y libreadline-dev

cd open_elevation/ssdp

chmod 700 autogen.sh
./autogen.sh
./configure CFLAGS=' -lm -ldl -O2 -pipe -fno-plt -flto -march=native ' \
            --enable-openmp
make
make install
ldconfig
