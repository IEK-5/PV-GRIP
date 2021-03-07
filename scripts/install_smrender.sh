#!/bin/bash

cd $(git rev-parse --show-toplevel)

builddir=$(mktemp -d -p .)

git clone https://github.com/rahra/smrender.git "${builddir}/src"
cd "${builddir}/src"
./autoconf.sh
./configure
make
make install
ldconfig

rm -rf builddir
