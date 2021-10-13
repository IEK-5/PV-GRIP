import os
import diskcache


class LocalLock:
    """Implements singe node locking mechanism

    """

    def __init__(self, key, path, expire = None):
        self._cache = diskcache.Cache\
            (directory = os.path.join(path, '_locallock'),
             size_limit = (1024**3))
        self._lock = diskcache.RLock(cache = self._cache,
                                     key = key,
                                     expire = expire)


    def __enter__(self):
        self._lock.acquire()


    def __exit__(self, type, value, traceback):
        self._lock.release()
