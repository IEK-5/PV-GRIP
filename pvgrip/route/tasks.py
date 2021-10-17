import os
import logging
import shutil

import pandas as pd

from pvgrip.route.utils \
    import write_result, write_locations

from pvgrip.ssdp.utils \
    import pickle2ssdp_topography, call_ssdp
from pvgrip.ssdp.generate_script \
    import poa_route

from pvgrip \
    import CELERY_APP
from pvgrip.globals \
    import get_SPATIAL_DATA
from pvgrip.utils.cache_fn_results \
    import cache_fn_results
from pvgrip.utils.celery_one_instance \
    import one_instance

from pvgrip.utils.files \
    import get_tempfile, remove_file, get_tempdir
from pvgrip.utils.format_dictionary \
    import format_dictionary


@CELERY_APP.task(bind=True)
@cache_fn_results()
@one_instance(expire = 400)
def merge_tsv(self, tsv_files):
    logging.debug("merge_tsv\n{}"\
                  .format(format_dictionary(locals())))
    ofn = get_tempfile()

    try:
         res = pd.concat([pd.read_csv(fn, sep=None, engine='python') \
                          for fn in tsv_files])
         res.to_csv(ofn, sep='\t', index=False)
    except Exception as e:
        remove_file(ofn)
        raise e

    return ofn


@CELERY_APP.task(bind=True)
@cache_fn_results()
@one_instance(expire = 60*10)
def compute_route(self, ifn, route, lat, lon,
                  ghi_default, dhi_default,
                  time_default, albedo, nsky):
    logging.debug("compute_route\n{}"\
                  .format(format_dictionary(locals())))
    wdir = get_tempdir()
    ofn = get_tempfile()

    ssdp_ifn = os.path.join(wdir, 'ssdp_ifn')
    ssdp_ofn = os.path.join(wdir, 'ssdp_ofn')
    route_fn = os.path.join(wdir, 'route_fn')
    locations_fn = os.path.join(wdir, 'locations_fn')

    try:
        ssdp_ifn, data, grid = \
            pickle2ssdp_topography(ifn, ssdp_ifn)

        route = write_locations(route = route,
                                ghi_default = ghi_default,
                                dhi_default = dhi_default,
                                time_default = time_default,
                                locations_fn = locations_fn)

        call = poa_route\
            (topography_fname = ssdp_ifn,
             albedo = albedo,
             nsky = nsky,
             ofn = ssdp_ofn,
             locations_fn = locations_fn,
             grid = grid,
             lat = lat,
             lon = lon)

        call_ssdp(call)
        write_result(route = route,
                     ssdp_ofn = ssdp_ofn,
                     ofn = ofn)
        return ofn
    except Exception as e:
        remove_file(ofn)
        raise e
    finally:
        shutil.rmtree(wdir)
