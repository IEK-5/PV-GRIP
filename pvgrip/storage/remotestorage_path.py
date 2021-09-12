import re

from pvgrip.globals \
    import ALLOWED_REMOTE

REGEX = re.compile(r'^(.*)://(.*)')


def is_remote_path(path):
    if not isinstance(path, str):
        return False

    if not REGEX.match(path):
        return False

    remotetype = REGEX.match(path).groups()[0]
    if remotetype not in ALLOWED_REMOTE:
        return False

    return True


def searchandget_locally(fn):
    """Look through remote and get locally

    :fn: filepath

    """
    for remotetype in ALLOWED_REMOTE:
        rpath = RemoteStoragePath\
            (fn, remotetype = remotetype)
        if rpath.in_storage():
            return rpath.get_locally()

    raise RuntimeError\
        ("{path} not any remote storage!"\
         .format(path=fn))


def searchif_instorage(fn):
    for remotetype in ALLOWED_REMOTE:
        rpath = RemoteStoragePath\
            (fn, remotetype = remotetype)
        if rpath.in_storage(True):
            return True

    return False


def search_determineremote(fn, ignore_remote):
    for remotetype in ALLOWED_REMOTE:
        if remotetype == ignore_remote:
            continue

        rpath = RemoteStoragePath\
            (fn, remotetype = remotetype)
        if rpath.in_storage():
            return rpath

    return None


class RemoteStoragePath:

    def __init__(self, path, remotetype = 'cassandra_path'):
        if not REGEX.match(path):
            self._path = "{remote_type}://{path}"\
                .format(remote_type = remotetype, path = path)
        else:
            self._path = path

        if self.remotetype not in ALLOWED_REMOTE:
            raise RuntimeError("Unsupported remotetype = {}!"\
                               .format(self.remotetype))



    def __str__(self):
        return self._path


    def __repr__(self):
        return self._path


    @property
    def path(self):
        return REGEX.match(self._path).groups()[1]


    @property
    def remotetype(self):
        return REGEX.match(self._path).groups()[0]


    @property
    def _storage(self):
        if self.remotetype == 'cassandra_path':
            from pvgrip.globals \
                import get_CASSANDRA_STORAGE
            return get_CASSANDRA_STORAGE()

        if self.remotetype == 'ipfs_path':
            from pvgrip.globals \
                import get_IPFS_STORAGE
            return get_IPFS_STORAGE()


    @property
    def _localcache(self):
        from pvgrip.globals import get_RESULTS_CACHE
        return get_RESULTS_CACHE()


    def in_storage(self, ignoreiflocal = False):
        """Check if self in storage

        :ignoreiflocal: if True then remote storage is not even
        checked
        """
        if ignoreiflocal:
            if self.path in self._localcache:
                return True

        return self.path in self._storage


    def get_cid(self):
        if self.remotetype != 'ipfs_path':
            raise RuntimeError\
                ('get_cid is meaningless for {}'\
                 .format(self.remotetype))

        return self._storage._get_ipfs_cid(self.path)


    def get_timestamp(self):
        return self._storage.get_timestamp(self.path)


    def get_locally(self):
        if self.path in self._localcache:
            return self.path

        if self.path not in self._storage:
            raise RuntimeError\
                ("{path} not a {remotetype}!"\
                 .format(path=self.path,
                         remotetype=self.remotetype))

        try:
            self._storage.download(self.path, self.path)
        except Exception as e:
            raise RuntimeError\
                ("""Failed to download file: {}
                error: {}
                remotetype: {}
                """.format(self.path,str(e),self.remotetype))

        self._localcache.add(self.path)
        return self.path


    def upload(self):
        try:
            self._storage.upload(self.path, self.path)
        except Exception as e:
            raise RuntimeError\
                ("""Failed to upload file: {}
                error: {}
                remotetype: {}
                """.format(self.path,str(e),self.remotetype))

        self._localcache.add(self.path)
