#!/usr/bin/env python3

import logging

import pandas as pd

from tqdm import tqdm

from pvgrip.globals \
    import get_IPFS_STORAGE, get_LOCAL_STORAGE

IPFS = get_IPFS_STORAGE()
LOCAL = get_LOCAL_STORAGE("localmount_hdf")


def upload_chunk(files):
    first = files[0]

    if first not in IPFS:
        raise RuntimeError("{} not in IPFS!".format(first))

    timestamp = IPFS.get_timestamp(first)

    if first not in LOCAL:
        IPFS.download(first, './ipfs2local_file')
        LOCAL.upload('./ipfs2local_file', first,
                     timestamp = timestamp)

    for x in files[1:]:
        if x not in LOCAL:
            LOCAL.link(first, x, timestamp = timestamp)


if __name__ == '__main__':
    data = pd.read_csv('files.csv.gz')

    for cid, chunk in tqdm(data.groupby('ipfs_cid')):
        try:
            upload_chunk(chunk['filename'].to_numpy())
        except Exception as e:
            logging.error("error = {}".format(e))
            continue
