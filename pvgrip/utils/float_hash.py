import os
import types
import hashlib


def float_hash(key, digits = 8):
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

    if isinstance(key, types.FunctionType):
        key = key.__name__

    h.update(str(key).encode('utf-8'))
    return h.hexdigest()
