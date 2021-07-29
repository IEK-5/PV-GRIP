#!/bin/bash

cd /code

ipfsdir=$(mktemp -d -p .)
ipfsclusterdir=$(mktemp -d -p .)

# install ipfs binary
cd "${ipfsdir}"
wget "https://dist.ipfs.io/go-ipfs/v0.9.0/go-ipfs_v0.9.0_linux-amd64.tar.gz"
tar -xvzf "go-ipfs_v0.9.0_linux-amd64.tar.gz"
cd go-ipfs
bash install.sh
cd /code

# install ipfs-cluster-ctl
cd "${ipfsclusterdir}"
wget "https://dist.ipfs.io/ipfs-cluster-ctl/v0.14.0/ipfs-cluster-ctl_v0.14.0_linux-amd64.tar.gz"
tar -xvzf "ipfs-cluster-ctl_v0.14.0_linux-amd64.tar.gz"
install -Dm755 ipfs-cluster-ctl/ipfs-cluster-ctl /usr/local/bin/ipfs-cluster-ctl
cd /code

rm -rf "${ipfsdir}" "${ipfsclusterdir}"
