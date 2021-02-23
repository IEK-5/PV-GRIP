

class Cassandra_Path(str):

    def in_cassandra(self):
        from open_elevation.globals \
            import get_CASSANDRA_STORAGE
        CASSANDRA_STORAGE = get_CASSANDRA_STORAGE()
        return str(self) in CASSANDRA_STORAGE


    def get_locally(self):
        from open_elevation.globals \
            import get_CASSANDRA_STORAGE, get_RESULTS_CACHE
        CASSANDRA_STORAGE = get_CASSANDRA_STORAGE()
        RESULTS_CACHE = get_RESULTS_CACHE()
        fn = str(self)
        if fn not in CASSANDRA_STORAGE:
            raise RuntimeError\
                ("%s not in CASSANDRA_STORAGE" % fn)

        if fn in RESULTS_CACHE:
            return fn

        CASSANDRA_STORAGE.download(fn, fn)
        return RESULTS_CACHE.add(fn)
