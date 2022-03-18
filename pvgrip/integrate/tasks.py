import os
import shutil
import pickle
import logging


from pvgrip \
    import CELERY_APP
from pvgrip.utils.cache_fn_results \
    import cache_fn_results
from pvgrip.utils.celery_one_instance \
    import one_instance
from pvgrip.utils.basetask \
    import WithRetry

from pvgrip.utils.format_dictionary \
    import format_dictionary
from pvgrip.utils.files \
    import get_tempfile, remove_file, get_tempdir

from pvgrip.ssdp.generate_script \
    import poa_integrate
from pvgrip.ssdp.utils \
    import pickle2ssdp_topography, \
    call_ssdp, array_1d_2pickle

from pvgrip.integrate.utils \
    import write_irrtimes


@CELERY_APP.task(bind=True, base=WithRetry)
@cache_fn_results(minage=1647003564, path_prefix='integrate')
@one_instance(expire = 60*10)
def integrate_irradiance(self, ifn, times_fn,
                         lat, lon, albedo,
                         offset, azimuth, zenith, nsky):
    logging.debug("integrate_irradiance\n{}"\
                  .format(format_dictionary(locals())))
    wdir = get_tempdir()
    ofn = get_tempfile()

    ssdp_ifn = os.path.join(wdir, 'ssdp_ifn')
    ssdp_ofn = os.path.join(wdir, 'ssdp_ofn')
    time_ghi_dhi = os.path.join(wdir, 'time_ghi_dhi')

    try:
        ssdp_ifn, data, grid = \
            pickle2ssdp_topography(ifn, ssdp_ifn)

        write_irrtimes(ifn = times_fn,
                       ofn = time_ghi_dhi)

        call = poa_integrate\
            (topography_fname = ssdp_ifn,
             albedo = albedo,
             offset = offset,
             azimuth = azimuth,
             zenith = zenith,
             nsky = nsky,
             ofn = ssdp_ofn,
             irrtimes = time_ghi_dhi,
             grid = grid,
             lat = lat,
             lon = lon)

        call_ssdp(call)
        return array_1d_2pickle(ssdp_ofn, data, ofn)
    except Exception as e:
        remove_file(ofn)
        raise e
    finally:
        shutil.rmtree(wdir)


@CELERY_APP.task(bind=True, base=WithRetry)
@cache_fn_results(minage=1647003564, path_prefix='integrate')
@one_instance(expire = 60*10)
def sum_pickle(self, pickle_files):
    logging.info("sum_pickle\n{}"\
                 .format(format_dictionary(locals())))

    if not isinstance(pickle_files, list):
        logging.error("pickle_files = {} is not a list!"\
                      .format(pickle_files))
        pickle_files = [pickle_files]

    if not len(pickle_files):
        raise RuntimeError("pickle_files is an empty list!")

    with open(pickle_files[0],'rb') as f:
        res = pickle.load(f)
    for fn in pickle_files[1:]:
        with open(fn, 'rb') as f:
            x = pickle.load(f)
        res['raster'] += x['raster']

    ofn = get_tempfile()
    try:
        with open(ofn, 'wb') as f:
            pickle.dump(res, f)
    except Exception as e:
        remove_file(ofn)
        raise e
    return ofn
