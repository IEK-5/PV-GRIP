import os
import shutil
import logging


from pvgrip \
    import CELERY_APP
from pvgrip.utils.cache_fn_results \
    import cache_fn_results
from pvgrip.utils.celery_one_instance \
    import one_instance
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


@CELERY_APP.task(bind=True)
@cache_fn_results()
@one_instance(expire = 60*10)
def integrate_irradiance(self, ifn, times_fn,
                         lat, lon,
                         albedo, nsky):
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
