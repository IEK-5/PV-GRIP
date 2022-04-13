#!/bin/bash

name="autoconf"
version="2.71"

builddir=$(mktemp -d -p .)

mkdir -p "${builddir}"
cd "${builddir}"

src="https://ftp.gnu.org/pub/gnu/${name}/${name}-${version}.tar.xz"
md5sum="12cfa1687ffa2606337efe1a64416106"

wget "${src}"
tar -xvf "${name}-${version}.tar.xz"
cd "${name}-${version}"
./configure
make
make install
ldconfig

rm -rf builddir
