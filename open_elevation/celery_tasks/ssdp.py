import os
import celery
import pickle
import shutil
import logging
import itertools

import numpy as np

from datetime import datetime
from pytz import timezone

from open_elevation.globals \
    import SSDP, SSDP_NJOBS
from open_elevation.celery_tasks \
    import CELERY_APP
from open_elevation.celery_tasks.sample_raster_box \
    import sample_raster, convert2output_type
from open_elevation.cache_fn_results \
    import cache_fn_results
from open_elevation.celery_one_instance \
    import one_instance
from open_elevation.ssdp \
    import poa_raster

from open_elevation.utils \
    import get_tempfile, remove_file, \
    run_command, get_tempdir, \
    format_dictionary


def call_ssdp(what):
    wdir = get_tempdir()

    logging.debug("""ssdp call:
    %s
    """ % what)

    try:
        ifn = os.path.join(wdir,'script.ssdp')
        with open(ifn,'w') as f:
            f.write(what)

        run_command\
            (what = \
             [SSDP,
              '-n',str(SSDP_NJOBS),
              '-f',ifn],
             cwd = wdir)
    finally:
        shutil.rmtree(wdir)


def pickle2ssdp_topography(ifn, ofn):
    """Convert pickle raster data to ssdp topography format

    :ifn: input pickle file (output of sample_raster)

    :return: ofn output file in the ssdp tsv format
    """
    with open(ifn, 'rb') as f:
        data = pickle.load(f)

    box = data['mesh']['raster_box']
    step = data['mesh']['step']
    nlon = len(data['mesh']['mesh'][0])
    nlat = len(data['mesh']['mesh'][1])
    a = np.transpose(data['raster'][::-1,:,:],axes=(1,0,2))\
          .reshape(nlon*nlat)

    grid = (nlon, nlat)\
        + (box[1],
           box[0],
           box[1]+step*nlat,
           box[0]+step*nlon)

    with open(ofn, 'w') as f:
        for i in range(nlon*nlat):
            f.write(str(a[i]) + '\n')

    return ofn, data, grid


def array_1d_2pickle(ssdp_fn, data, ofn):
    with open(ssdp_fn, 'r') as f:
        ssdp_data = [float(line.rstrip()) for line in f]

    ssdp_data = np.array(ssdp_data)
    data['raster'] = np.transpose\
        (ssdp_data.reshape\
         (np.transpose(data['raster'],
                       axes=(1,0,2))\
          .shape), axes=(1,0,2))[::-1,:,:]

    with open(ofn, 'wb') as f:
        pickle.dump(data,f)

    return ofn


@CELERY_APP.task()
@cache_fn_results()
@one_instance(expire = 60*10)
def compute_irradiance(ifn,
                       utc_time, lat, lon,
                       ghi, dhi, albedo,
                       nsky):
    logging.debug("compute_irradiance_ssdp\n{}"\
                  .format(format_dictionary(locals())))
    ssdp_ifn = get_tempfile()
    ssdp_ofn = get_tempfile()
    ofn = get_tempfile()
    try:
        ssdp_ifn, data, grid = \
            pickle2ssdp_topography(ifn, ssdp_ifn)

        call = poa_raster\
            (topography_fname = ssdp_ifn,
             utc_time = utc_time,
             albedo = albedo,
             lat = lat, lon = lon,
             ghi = ghi, dhi = dhi,
             nsky = nsky, ofn = ssdp_ofn,
             grid = grid)

        call_ssdp(call)
        return array_1d_2pickle(ssdp_ofn, data, ofn)
    except Exception as e:
        remove_file(ofn)
        raise e
    finally:
        remove_file(ssdp_ifn)
        remove_file(ssdp_ofn)


def timestr2utc_time(timestr):
    tz = timezone("UTC")
    dt = datetime.strptime(timestr, '%Y-%m-%d_%H:%M:%S')
    dt = tz.localize(dt)
    return int(dt.timestamp())


def centre_of_box(box):
    return (box[1] + (box[3]-box[1])/2,
            box[0] + (box[2]-box[0])/2)


def ssdp_irradiance(timestr, ghi, dhi, albedo, nsky,
                    **kwargs):
    """Start the irradiance

    :timestr: time string, format: %Y-%m-%d_%H:%M:%S

    :ghi: global horizontal irradiance

    :dhi: diffused horizontal irradiance

    :nsky: number of zenith discretizations (see `man ssdp`)

    :kwargs: arguments passed to sample_raster

    """
    output_type = kwargs['output_type']
    kwargs['output_type'] = 'pickle'

    utc_time = timestr2utc_time(timestr)
    lon, lat = centre_of_box(kwargs['box'])

    tasks = sample_raster(**kwargs)
    tasks |= compute_irradiance.signature\
        ((),{'ghi':ghi,'dhi':dhi,'nsky':nsky,
             'lon':lon,'lat':lat,'albedo':albedo,
             'utc_time':utc_time})

    return convert2output_type(tasks,
                               output_type = output_type)
