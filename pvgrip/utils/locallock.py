import os
import logging
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
        logging.debug("""LocalLock: __init__
        key = {}
        path = {}
        """.format(key, path))

    def __enter__(self):
        logging.debug("LocalLock: acquire start __enter__")
        self._lock.acquire()
        logging.debug("LocalLock: acquire end __enter__")


    def __exit__(self, type, value, traceback):
        logging.debug("LocalLock: release start __exit__")
        self._lock.release()
        logging.debug("LocalLock: release end __exit__")
