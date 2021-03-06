import os
import shutil
import requests
import logging

from pvgrip.lidar.utils \
    import write_pdaljson

from pvgrip.utils.run_command \
    import run_command

from pvgrip \
    import CELERY_APP
from pvgrip.utils.cache_fn_results \
    import cache_fn_results
from pvgrip.utils.celery_one_instance \
    import one_instance
from pvgrip.utils.basetask \
    import WithRetry

from pvgrip.utils.files \
    import remove_file, get_tempdir
from pvgrip.utils.format_dictionary \
    import format_dictionary

from pvgrip.globals \
    import OPENGEODATA_CREDENTIALS


@CELERY_APP.task(bind=True, base=WithRetry)
@cache_fn_results(ofn_arg='ofn')
@one_instance(expire = 60*5)
def download_laz(self, url, ofn):
    logging.debug("download_laz\n{}"\
                  .format(format_dictionary(locals())))
    _ = OPENGEODATA_CREDENTIALS()

    r = requests.get(url, allow_redirects=True)

    if 200 != r.status_code:
        raise RuntimeError("""
        cannot download data!
        url: %s
        """ % url)

    try:
        with open(ofn, 'wb') as f:
            f.write(r.content)
        return ofn
    except Exception as e:
        logging.error("error on writing! {}".format(e))
        remove_file(ofn)


@CELERY_APP.task(bind=True, base=WithRetry)
@cache_fn_results(ofn_arg='ofn')
@one_instance(expire = 60*20)
def run_pdal(self, laz_fn, resolution, what, ofn):
    logging.debug("run_pdal\n{}"\
                  .format(format_dictionary(locals())))
    wdir = get_tempdir()
    try:
        pdalfn = os.path.join(wdir,'run_pdal.json')
        write_pdaljson(laz_fn = laz_fn, ofn = ofn,
                       resolution = resolution, what = what,
                       json_ofn = pdalfn)
        run_command\
            (what = ['pdal','pipeline',pdalfn],
             cwd = wdir)
        return ofn
    except Exception as e:
        logging.error("error on writing! {}".format(e))
        remove_file(ofn)
    finally:
        shutil.rmtree(wdir)
