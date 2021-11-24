#!/usr/bin/env python3

import os
import re
import json
import shutil
import logging

import pandas as pd

from tqdm import tqdm

from pvgrip.utils.float_hash \
    import float_hash

from pvgrip.lidar.tasks \
    import download_laz
from pvgrip.utils.cache_fn_results \
    import _compute_ofn

from pvgrip.utils.files \
    import get_tempdir

from pvgrip.utils.run_command \
    import run_command

from pvgrip.globals \
   import get_LOCAL_STORAGE


STORAGE = get_LOCAL_STORAGE("localmount_hdf")

URLRE = re.compile(r'https:.*_32_([0-9]*)_([0-9]*)_1_nw.*')
ROOTURL = "https://www.opengeodata.nrw.de/produkte/geobasis/hm/3dm_l_las/3dm_l_las/3dm_32_{}_{}_1_nw.laz"

LAS_PREFIX=os.path.join('/code/data/current/NRW_Las_Data','data')


def _laz2url(lazfn):
    wdir = get_tempdir()

    try:
        tmplaz = os.path.join(wdir,'src.laz')
        STORAGE.download(lazfn, tmplaz)
        bounds = json.loads\
            (run_command\
             (what = ['pdal','info',tmplaz,'--summary'],
              cwd = wdir,
              return_stdout = True))['summary']['bounds']
        lon = bounds['minx'] // 1000
        lat = bounds['miny'] // 1000
        return ROOTURL.format(lon,lat)
    finally:
        shutil.rmtree(wdir)


def _newfn(url, prefix):
    lon = int(URLRE.sub(r'\1', url))
    lat = int(URLRE.sub(r'\2', url))
    newkey = ("nrw_las", url, lon, lat)
    return os.path.join(prefix, float_hash(newkey))


def _process_data(row):
    data = json.loads(row['data'])

    if 'remote_meta' not in data:
        return None

    dst = _newfn(data['url'], prefix=os.path.join(data['remote_meta'],'data'))
    src = data['file']

    if not data['if_compute_las']:
        dst = os.path.join(dst,'src')
    else:
        dst = os.path.join\
            (dst,"{}_{:.8f}".format(data['stat'],0.3))

    return {'src': src, 'dst': dst}


def _process_laz(row):
    src = row[0]
    dst = _newfn(_laz2url(row[0]), prefix=LAS_PREFIX)
    dst = os.path.join(dst,'src')
    return {'src': src, 'dst': dst}


def iterate(fn, chunksize=10000):
    for chunk in tqdm(pd.read_csv(fn, chunksize=chunksize,
                                  escapechar='\\')):
        for _, row in tqdm(chunk.iterrows()):
            yield row


def process(how, ifn, ofn='movefiles.csv.gz', header=True, chunksize=10000):
    res = []

    if header:
        try:
            os.unlink(ofn)
        except:
            pass

    for row in iterate(ifn):
        try:
            x = how(row)
        except Exception as e:
            logging.error("error: {}".format(e))
            continue

        if x is None:
            continue
        res += [x]

        if len(res) > chunksize:
            pd.DataFrame(res).to_csv(ofn, header=header, mode='a',
                                     index=False, compression='gzip')
            res = []
            header = False

    pd.DataFrame(res).to_csv(ofn, header=header, mode='a',
                             index=False, compression='gzip')


if __name__ == '__main__':
    process(how=_process_data, ifn='data.csv.gz',
            header=True, ofn='movefiles.csv.gz')

    process(how=_process_laz, ifn='lazfiles.csv.gz',
            header=False, ofn='movefiles.csv.gz')
