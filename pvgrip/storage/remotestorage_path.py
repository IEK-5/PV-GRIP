import os
import re
import logging
import filelock

from pvgrip.globals \
    import ALLOWED_REMOTE, RESULTS_PATH

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
    def _lock_fn(self):
        fn = os.path.join(RESULTS_PATH, 'locks',
                          self.path.lstrip(os.path.sep))
        os.makedirs(os.path.dirname(fn), exist_ok = True)
        return fn


    @property
    def remotetype(self):
        return REGEX.match(self._path).groups()[0]


    @property
    def _storage(self):
        if self.remotetype == 'ipfs_path':
            from pvgrip.globals \
                import get_IPFS_STORAGE
            return get_IPFS_STORAGE()
        elif re.match(r'localmount_.*',self.remotetype):
            from pvgrip.globals \
                import get_LOCAL_STORAGE
            return get_LOCAL_STORAGE(self.remotetype)
        else:
            raise RuntimeError\
                ("unknown remotetype = {}".format(self.remotetype))


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
        os.makedirs(os.path.dirname(self.path), exist_ok=True)

        with filelock.FileLock(self._lock_fn):
            logging.debug("get_locally: got the lock")
            if self.path in self._localcache:
                return self.path

            logging.debug("get_locally: check local")
            if self.path not in self._storage:
                raise RuntimeError\
                    ("{path} not a {remotetype}!"\
                     .format(path=self.path,
                             remotetype=self.remotetype))

            logging.debug("get_locally: check remote")
            try:
                logging.debug("get_locally: start downloading")
                self._storage.download(self.path, self.path)
                logging.debug("get_locally: finish downloading")
            except Exception as e:
                logging.error("""Failed to download file: {}
                error: {}
                remotetype: {}
                """.format(self.path,str(e),self.remotetype))
                raise e

            logging.debug("get_locally: add to localcache")
            self._localcache.add(self.path)
            self._localcache.add(self._lock_fn)
            return self.path


    def upload(self):
        try:
            self._storage.upload(self.path, self.path)
        except Exception as e:
            logging.error("""Failed to upload file: {}
            error: {}
            remotetype: {}
            """.format(self.path,str(e),self.remotetype))
            raise e

        self._localcache.add(self.path)


    def link(self, src):
        try:
            self._storage.link(src, self.path)
        except Exception as e:
            logging.error("""Failed to link file!
            source: {}
            destination: {}
            error: {}
            remotetype: {}
            """.format(src, self.path,
                       str(e),self.remotetype))
            raise e

        if os.path.exists(self.path):
            self._localcache.add(self.path)
