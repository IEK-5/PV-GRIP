#!/bin/bash

cd /code

ipfsdir=$(mktemp -d -p .)
ipfsclusterdir=$(mktemp -d -p .)

version_ipfs="0.10.0"
version_ipfscluster="0.14.1"

# install ipfs binary
cd "${ipfsdir}"
wget "https://dist.ipfs.io/go-ipfs/v${version_ipfs}/go-ipfs_v${version_ipfs}_linux-amd64.tar.gz"
tar -xvzf "go-ipfs_v${version_ipfs}_linux-amd64.tar.gz"
cd go-ipfs
bash install.sh
cd /code

# install ipfs-cluster-ctl
cd "${ipfsclusterdir}"
wget "https://dist.ipfs.io/ipfs-cluster-ctl/v${version_ipfscluster}/ipfs-cluster-ctl_v${version_ipfscluster}_linux-amd64.tar.gz"
tar -xvzf "ipfs-cluster-ctl_v${version_ipfscluster}_linux-amd64.tar.gz"
install -Dm755 ipfs-cluster-ctl/ipfs-cluster-ctl /usr/local/bin/ipfs-cluster-ctl
cd /code

rm -rf "${ipfsdir}" "${ipfsclusterdir}"
