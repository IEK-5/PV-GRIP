import os
import json
import shutil

from rtree import index

from nrw_las import \
    _Files_LRUCache, NRWData_Cache, NRWData


def touch(fname, times=None):
    with open(fname, 'a'):
        os.utime(fname, times)


def list_files(path):
    res = []
    for dp, dn, filenames in os.walk(path):
        for f in filenames:
            res += [os.path.join(dp,f)]

    return res


def test_Files_LRUCache(N = 10):
    cache = _Files_LRUCache(maxsize = N)
    path="test_Files_LRUCache"
    os.makedirs(path, exist_ok = True)

    for i in range(2*N):
        p = os.path.join(path, str(i))
        touch(p)
        cache[i] = p

    assert N == len(list_files(path))

    shutil.rmtree(path)


def test_NRWData_Cache(N = 10):
    path="test_NRWData_Cache"
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

    shutil.rmtree(path)


def test_NRWData():
    meta = {
        "root_url": "https://www.opengeodata.nrw.de/produkte/geobasis/hm/3dm_l_las/3dm_l_las/3dm_32_%s_%s_1_nw.laz",
        "step": 1000,
        "box_resolution": 1,
        "epsg": 25832,
        "box_step": 1,
        "fn_meta": "las_meta.csv",
        "meta_entry_regex": "^test_(.*)_(.*)_.*$",
        "pdal_resolution": 1
    }
    path = 'test_NRWData'
    os.makedirs(path, exist_ok = True)
    with open(os.path.join(path, 'las_meta.json'),'w') as f:
        json.dump(meta, f)

    with open(os.path.join(path, 'las_meta.csv'), 'w') as f:
        f.write('test_459_5810_dsfg\n')
        f.write('test_458_5806_dsfg\n')
        f.write('does_not_match\n')

    X = NRWData(path = path, max_saved = 100)

    assert 2 == len(X._files.keys())

    a = list(X._files.keys())[0]
    assert a == X.get_path(a)

    shutil.rmtree(path)
