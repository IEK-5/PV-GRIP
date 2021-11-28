import os
import time
import shutil
import logging

from pvgrip.utils.redis.lock \
    import RedisLock


def _touch(fn, times = None):
    f = open(fn, 'a')
    try:
        os.utime(fn, times)
    finally:
        f.close()


def _unlink(fn):
    try:
        os.unlink(fn)
    except:
        pass


def _mkdir(fn):
    try:
        os.makedirs(os.path.dirname(fn), exist_ok = True)
    except:
        pass


def _copy_or_link(src, dst):
    _mkdir(dst)

    try:
        os.link(src, dst)
    except OSError as e:
        pass

    try:
        shutil.copyfile(src, dst)
    except shutil.SameFileError as e:
        pass


class LOCALIO_Files:

    def __init__(self, root, redis_url,
                 lock_expire = 600, lock_sleep = 1):
        """init

        :root: path to the root of data directory

        :redis_url: url describing how to reach redis

        :lock_expire: expiration time in seconds for a transaction
        lock. 600 seconds are just okay for a transaction of 600MB at
        1MB/s

        :lock_sleep: seconds in sleep before attempts

        """
        self._root = os.path.realpath(root)
        self._redis_url = redis_url
        self._lock_expire = lock_expire
        self._lock_sleep = lock_sleep
        self._sanityfn = os.path.join(self._root,'localio.sanity')


    def _sanity(self):
        if not os.path.exists(self._sanityfn):
            msg = "{} file is missing! Did you lose the storage?"\
                .format(self._sanityfn)
            logging.error(msg)
            raise RuntimeError(msg)


    def _storage_fn(self, fn):
        return os.path.join(self._root, "data", fn.lstrip(os.path.sep))


    def _check_fn(self, storage_fn):
        res = os.path.join(self._root, "failchecks", storage_fn.lstrip(os.path.sep))
        _mkdir(res)
        return res


    def _check(self, storage_fn):
        cfn = self._check_fn(storage_fn)
        sfn = self._storage_fn(storage_fn)

        if os.path.exists(cfn):
            _unlink(sfn)
            _unlink(cfn)
            msg = """a check file for {} exists!
            removing {} and {}
            """.format(storage_fn, sfn, cfn)
            logging.error(msg)
            raise RuntimeError(msg)


    def _lock(self, storage_fn):
        return RedisLock(redis_url = self._redis_url,
                         key = "localio_files_{}".format(storage_fn),
                         timeout = self._lock_expire)


    def __contains__(self, storage_fn):
        with self._lock(storage_fn):
            self._sanity()

            try:
                self._check(storage_fn)
            except:
                return False

            return os.path.isfile(self._storage_fn(storage_fn))


    def _set_timestamp(self, storage_fn, timestamp):
        if timestamp is None:
            return

        sfn = self._storage_fn(storage_fn)
        os.utime(sfn, (timestamp, timestamp))


    def get_timestamp(self, storage_fn):
        if storage_fn not in self:
            return None

        return os.stat(self._storage_fn(storage_fn)).st_mtime


    def update_timestamp(self, storage_fn):
        if storage_fn not in self:
            return None

        self._set_timestamp(storage_fn, time.time())


    def download(self, storage_fn, ofn):
        """download file locally

        :storage_fn: path relative to the storage root

        :ofn: output file path

        """
        if storage_fn not in self:
            raise RuntimeError('{} not in storage!'\
                               .format(storage_fn))

        with self._lock(storage_fn):
            _copy_or_link(self._storage_fn(storage_fn), ofn)


    def upload(self, ifn, storage_fn, timestamp = None):
        """upload file to the storage

        :ifn: path to upload

        :storage_fn: path relative to the storage root

        :timestamp: optionally set specific timestamp. If None,
        timestamp is not set (actual time is used)

        """
        with self._lock(storage_fn):
            self._sanity()

            chck = self._check_fn(storage_fn)
            _touch(chck)

            _copy_or_link(ifn, self._storage_fn(storage_fn))
            self._set_timestamp(storage_fn, timestamp)

            _unlink(chck)


    def link(self, src, dst, timestamp = None):
        """hardlink files within storage

        :src,dst: path relative to the storage root

        :timestamp: optionally set specific timestamp. If None,
        timestamp is not set. If '-1' timestamp of the source is
        set

        """
        if src not in self:
            raise RuntimeError('{} not in storage!'\
                               .format(src))

        if -1 == timestamp:
            timestamp = self.get_timestamp(src)

        with self._lock(src) and self._lock(dst):
            chck = self._check_fn(dst)
            _touch(chck)

            sdst = self._storage_fn(dst)
            if os.path.isfile(sdst):
                _unlink(sdst)
            _copy_or_link(self._storage_fn(src), sdst)
            self._set_timestamp(dst, timestamp)

            _unlink(chck)


    def delete(self, storage_fn):
        if storage_fn not in self:
            logging.warning("""something is fishy!
            tried to delete: {}
            ignoring...
            """.format(storage_fn))
            return

        with self._lock(storage_fn):
            _unlink(self._storage_fn(storage_fn))
            _unlink(self._check_fn(storage_fn))
