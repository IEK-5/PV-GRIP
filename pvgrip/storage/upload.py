import os
import hashlib

from pvgrip.utils.cache_fn_results \
    import cache_fn_results

from pvgrip.utils.files \
    import get_tempfile, remove_file

from pvgrip.globals \
    import RESULTS_PATH


def _hash_file(fn, chunk_size = 4096):
    h = hashlib.md5()

    with open(fn, 'rb') as f:
        for chunk in iter(lambda: f.read(chunk_size), b''):
            h.update(chunk)

    return h.hexdigest()


@cache_fn_results(keys = ['name'], link = True, ofn_arg = 'name')
def _upload_file(fn, name):
    return fn


def upload(request_data):
    ofn = get_tempfile()

    request_data.save(ofn, overwrite = True)
    name = os.path.join\
        (RESULTS_PATH, "upload", _hash_file(ofn))
    name = _upload_file(fn = ofn, name = name)

    remove_file(ofn)
    return {'storage_fn': name}


class Saveas_Requestdata:
    """just a dummy class that has .save method

    """

    def __init__(self, df):
        self._df = df


    def save(self, ofn, overwrite):
        self._df.to_csv(ofn, sep='\t', index = False)
