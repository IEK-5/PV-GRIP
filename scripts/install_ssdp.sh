#!/bin/bash

cd open_elevation/ssdp

chmod 700 autogen.sh
./autogen.sh
./configure CFLAGS=' -lm -ldl -O2 -pipe -fno-plt -flto -march=x86-64 ' \
            --enable-openmp
make
make install
ldconfig
