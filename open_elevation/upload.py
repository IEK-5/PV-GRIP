import os
import hashlib

from open_elevation.cache_fn_results \
    import cache_fn_results

from open_elevation.utils \
    import get_tempfile, remove_file

from open_elevation.globals \
    import RESULTS_PATH


def _hash_file(fn, chunk_size = 4096):
    h = hashlib.md5()

    with open(fn, 'rb') as f:
        for chunk in iter(lambda: f.read(chunk_size), b''):
            h.update(chunk)

    return h.hexdigest()


@cache_fn_results(keys = None, link = True, ofn_arg = 'name')
def _upload_file(fn, name):
    return fn


def upload(request_data):
    ofn = get_tempfile()

    request_data.save(ofn, overwrite = True)
    name = os.path.join\
        (RESULTS_PATH,
         "upload_" + _hash_file(ofn))
    _upload_file(fn = ofn, name = name)

    remove_file(ofn)
    return {'storage_fn': name}
