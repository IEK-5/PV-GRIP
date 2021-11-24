#!/usr/bin/env python3

import logging

import pandas as pd

from tqdm import tqdm

from pvgrip.globals \
   import get_LOCAL_STORAGE


STORAGE = get_LOCAL_STORAGE("localmount_hdf")


def iterate(fn, chunksize=10000):
    for chunk in tqdm(pd.read_csv(fn, chunksize=chunksize,
                                  escapechar='\\')):
        for _, row in tqdm(chunk.iterrows()):
            yield row


if __name__ == '__main__':
   for row in iterate('movefiles.csv.gz'):
      try:
         if row['src'] not in STORAGE:
            continue

         timestamp = STORAGE.get_timestamp(row['src'])
         STORAGE.link(row['src'],row['dst'],
                      timestamp = timestamp)
      except Exception as e:
         logging.error("error = {}".format(e))
         continue
