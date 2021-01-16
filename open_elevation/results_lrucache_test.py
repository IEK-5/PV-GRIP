import os
import shutil

from .files_lrucache_test import touch, list_files
from .results_lrucache import ResultFiles_LRUCache, float_hash


def test_ResultFiles_LRUCache(N = 10):
    path="test_TempFiles_LRUCache"
    try:
        os.makedirs(path, exist_ok = True)
        cache = ResultFiles_LRUCache\
            (maxsize = (N*1024)/(1024**3),
             path = path)

        for i in range(2*N):
            p = cache.add(i)
            touch(p)
        cache.check_content()
        # this will cause cleanup of cache
        p = cache.add((1,2,3))
        touch(p)

        assert p == cache.get((1,2,3))
        assert N == len(cache)
        assert N*1024 == cache.size()
        assert N == len(list_files(path, regex=r'.*tmp.*'))
    finally:
        shutil.rmtree(path)


def test_hash():
    assert '6a1920a4589859fec38f6b11174657d3' \
        == float_hash({1:2,2:([1,2,3],"a")})
