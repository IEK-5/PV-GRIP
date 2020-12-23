import os
import re
import json
import shutil

import numpy as np

from rtree import index

from nrw_las import \
    _Files_LRUCache, NRWData_Cache, NRWData


def touch(fname, times=None):
    with open(fname, 'a'):
        os.utime(fname, times)


def list_files(path, regex = r'.*'):
    regex = re.compile(regex)
    res = []
    for dp, dn, filenames in os.walk(path):
        for f in filenames:
            fn = os.path.join(dp,f)
            if regex.match(fn):
                res += [fn]

    return res


def test_Files_LRUCache(N = 10):
    path="test_Files_LRUCache"
    try:
        os.makedirs(path, exist_ok = True)
        cache = _Files_LRUCache(maxsize = N, path = path)

        for i in np.random.rand(2*N):
            p = os.path.join(path, str(i) + "_test_file")
            touch(p)
            cache.add(p)

        assert N == len(cache)
        assert N == len(list_files(path, regex=r'.*_test_file'))
    finally:
        shutil.rmtree(path)


def test_Files_LRUCache_contains():
    path="test_Files_LRUCache_contains"
    try:
        os.makedirs(path, exist_ok = True)
        cache = _Files_LRUCache(maxsize = 2, path = path)

        a = os.path.join(path, "a")
        touch(a)
        cache.add(a)

        b = os.path.join(path, "b")
        touch(b)
        cache.add(b)

        # here "one" is the first to evict
        assert False == (a not in cache)

        # after checking if "one" was there it should be "two"
        assert b == cache.popleft()
    finally:
        shutil.rmtree(path)


def test_NRWData_Cache(N = 10):
    path="test_NRWData_Cache"
    try:
        os.makedirs(path, exist_ok = True)

        X = NRWData_Cache(path = path,
                          regex = r'(.*)_(.*)\.tif',
                          fmt = '%d_%d.tif',
                          maxsize = N)

        assert X.coord2fn((100,20)) == os.path.join(path,"100_20.tif")

        x = X.coord2fn((1,2))
        touch(x)
        X.add(x)

        x = X.coord2fn((3,4))
        touch(x)
        X.add(x)

        X = NRWData_Cache(path = path,
                          regex = r'(.*)_(.*)\.tif',
                          fmt = '%d_%d.tif',
                          maxsize = N)

        assert 2 == len(X.list_paths())
        assert X.coord2fn((1,2)) == sorted(X.list_paths())[0]
        assert X.coord2fn((3,4)) == sorted(X.list_paths())[1]
        assert X.coord2fn((1,2)) in X
        assert X.coord2fn((3,4)) in X

        for i in range(N*2):
            x = X.coord2fn((i*13,i*17))
            touch(x)
            X.add(x)

        assert N == len(X.list_paths())
    finally:
        shutil.rmtree(path)


def test_NRWData():
    meta = {
        "root_url": "https://www.opengeodata.nrw.de/produkte/geobasis/hm/3dm_l_las/3dm_l_las/3dm_32_%s_%s_1_nw.laz",
        "step": 1000,
        "box_resolution": 1,
        "epsg": 25832,
        "box_step": 1,
        "meta_entry_regex": "^3dm_32_(.*)_(.*)_1_nw.*$",
        "meta_url": "https://www.opengeodata.nrw.de/produkte/geobasis/hm/3dm_l_las/3dm_l_las/index.json",
        "pdal_resolution": 1
    }
    path = 'test_NRWData'
    try:
        os.makedirs(path, exist_ok = True)
        with open(os.path.join(path, 'las_meta.json'),'w') as f:
            json.dump(meta, f)

        X = NRWData(path = path, max_saved = 100)
    finally:
        shutil.rmtree(path)
