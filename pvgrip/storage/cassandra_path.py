import re

REGEX = re.compile(r'^cassandra_path://(.*)')


def is_cassandra_path(path):
    if not isinstance(path, str):
        return False

    return REGEX.match(path) is not None


class Cassandra_Path:

    def __init__(self, path):
        if not REGEX.match(path):
            self._path = "cassandra_path://%s" % path
        else:
            self._path = path


    def __str__(self):
        return self._path


    def __repr__(self):
        return self._path


    def get_path(self):
        return REGEX.match(self._path).groups()[0]


    def in_cassandra(self):
        from pvgrip.globals \
            import get_CASSANDRA_STORAGE
        CASSANDRA_STORAGE = get_CASSANDRA_STORAGE()
        return self.get_path() in CASSANDRA_STORAGE


    def get_timestamp(self):
        from pvgrip.globals \
            import get_CASSANDRA_STORAGE
        fn = self.get_path()
        CASSANDRA_STORAGE = get_CASSANDRA_STORAGE()
        return CASSANDRA_STORAGE.get_timestamp(fn)


    def get_locally(self):
        from pvgrip.globals \
            import get_CASSANDRA_STORAGE, get_RESULTS_CACHE
        CASSANDRA_STORAGE = get_CASSANDRA_STORAGE()
        RESULTS_CACHE = get_RESULTS_CACHE()
        fn = self.get_path()
        if fn not in CASSANDRA_STORAGE:
            raise RuntimeError\
                ("%s not in CASSANDRA_STORAGE" % fn)

        if fn in RESULTS_CACHE:
            return fn

        CASSANDRA_STORAGE.download(fn, fn)
        RESULTS_CACHE.add(fn)
        return fn
