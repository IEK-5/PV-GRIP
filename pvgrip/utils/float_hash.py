import os
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

    h.update(str(key).encode('utf-8'))
    return h.hexdigest()


def float_hash_fn(key, digits = 8):
    from pvgrip.globals \
        import RESULTS_PATH
    return os.path.join\
        (RESULTS_PATH,
         "tmp_" + \
         float_hash(key,
                    digits = digits))
