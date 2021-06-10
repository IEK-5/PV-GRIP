import os
import shutil
import logging

import pandas as pd

from open_elevation.celery_tasks \
    import CELERY_APP
from open_elevation.cache_fn_results \
    import cache_fn_results
from open_elevation.celery_one_instance \
    import one_instance

from open_elevation.ssdp \
    import poa_integrate

from open_elevation.celery_tasks.ssdp \
    import pickle2ssdp_topography, \
    timestr2utc_time, call_ssdp, \
    centre_of_box, array_1d_2pickle

from open_elevation.celery_tasks.sample_raster_box \
    import sample_raster, convert2output_type

from open_elevation.utils \
    import get_tempfile, remove_file, \
    run_command, get_tempdir, format_dictionary


def write_irrtimes(ifn, ofn):
    data = pd.read_csv(ifn, sep=None, engine='python')
    data.columns = [x.lower() for x in data.columns]

    if 'timestr' not in data \
       or 'ghi' not in data \
       or 'dhi' not in data:
        raise RuntimeError\
            ('timestr, ghi or dhi columns are missing!')

    data['utctime'] = data['timestr'].apply(timestr2utc_time)

    data[['utctime','ghi','dhi']].to_csv(ofn, sep='\t',
                                         index = False,
                                         header = False)


@CELERY_APP.task()
@cache_fn_results()
@one_instance(expire = 60*10)
def integrate_irradiance(ifn, times_fn,
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


def ssdp_integrate(tsvfn_uploaded,
                   albedo, nsky, **kwargs):
    output_type = kwargs['output_type']
    kwargs['output_type'] = 'pickle'
    kwargs['mesh_type'] = 'metric'

    lon, lat = centre_of_box(kwargs['box'])

    tasks = sample_raster(**kwargs)
    tasks |= integrate_irradiance.signature\
        ((),{'times_fn': tsvfn_uploaded,
             'albedo': albedo,
             'nsky': nsky,
             'lon': lon,
             'lat': lat})

    return convert2output_type(tasks,
                               output_type = output_type)
