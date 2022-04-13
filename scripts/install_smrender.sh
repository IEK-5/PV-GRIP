#!/bin/bash

builddir=$(mktemp -d -p .)

git clone -b v4.3.0 https://github.com/rahra/smrender.git "${builddir}/src"
cd "${builddir}/src"
./autoconf.sh
./configure
make
make install
ldconfig

rm -rf builddir
