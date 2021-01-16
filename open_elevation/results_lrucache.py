import os
import hashlib
import tempfile

from open_elevation.files_lrucache import Files_LRUCache


def float_hash(key, digits = 6):
    h = hashlib.md5()
    if isinstance(key, (tuple, list)):
        for x in key:
            h.update(float_hash(x, digits).encode('utf-8'))
        return h.hexdigest()

    if isinstance(key, dict):
        for k,v in key.items():
            h.update(float_hash(k, digits).encode('utf-8'))
            h.update(float_hash(v, digits).encode('utf-8'))
        return h.hexdigest()

    if isinstance(key, float):
        key = ('%.' + str(digits) + 'f') % key

    h.update(str(key).encode('utf-8'))
    return h.hexdigest()


class ResultFiles_LRUCache(Files_LRUCache):
    """Get tempfiles indexes by any key

    """

    def __init__(self, digits = 6, *args, **kwargs):
        """
        :digits: number of digits to use to hash computation of floats
        """
        super().__init__(*args, **kwargs)
        self._digits = digits


    def _get_fn(self, key):
        hash_value = float_hash(key, self._digits)
        return os.path.join(self.path, 'tmp_' + hash_value)


    def add(self, key):
        fn = self._get_fn(key)
        super().add(fn)
        return fn


    def add_file(self, fn):
        super().add(fn)
        return fn


    def __contains__(self, key):
        return super().__contains__\
            (self._get_fn(key))


    def file_in(self, fn):
        return super().__contains__(fn)


    def get(self, key, check = True):
        # this check will update LRU order
        if check and not self.__contains__(key):
            return None

        return self._get_fn(key)
