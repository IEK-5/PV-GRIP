#!/usr/bin/env python3

import logging

from tqdm import tqdm

from pvgrip.globals \
   import get_LOCAL_STORAGE

from pvgrip.utils.iterate_csv \
    import iterate_csv


STORAGE = get_LOCAL_STORAGE("localmount_hdf")


if __name__ == '__main__':
   for row in tqdm(iterate_csv('movefiles.csv.gz')):
      try:
         if row['src'] not in STORAGE:
            continue

         timestamp = STORAGE.get_timestamp(row['src'])
         STORAGE.link(row['src'],row['dst'],
                      timestamp = timestamp)
      except Exception as e:
         logging.error("error = {}".format(e))
         continue
