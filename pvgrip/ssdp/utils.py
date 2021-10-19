import os
import pytz
import shutil
import pickle
import logging
import datetime

import numpy as np


from pvgrip.utils.files \
    import get_tempfile, remove_file, get_tempdir
from pvgrip.utils.run_command \
    import run_command
from pvgrip.globals \
    import SSDP, SSDP_NJOBS


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
    a = data['raster'][::-1,:,:].reshape(nlon*nlat,order='F')

    grid = (nlon, nlat)\
        + (box[0],
           box[1],
           box[0]+step*nlon,
           box[1]+step*nlat)

    with open(ofn, 'w') as f:
        for i in range(nlon*nlat):
            f.write(str(a[i]) + '\n')

    return ofn, data, grid


def array_1d_2pickle(ssdp_fn, data, ofn):
    """Convert ssdp output array to a sample_raster pickle

    :ssdp_fn: ssdp output file with 1d array

    :data: second element of what pickle2ssdp_topography returns

    :ofn: filename where to write pickle file
    """
    with open(ssdp_fn, 'r') as f:
        ssdp_data = [float(line.rstrip()) for line in f]

    ssdp_data = np.array(ssdp_data)
    data['raster'] = ssdp_data.reshape\
        (data['raster'].shape,
         order='F')[::-1,:,:]

    with open(ofn, 'wb') as f:
        pickle.dump(data,f)

    return ofn


def timestr2utc_time(timestr):
    """Convert timestr to utc_time

    :timestr: time string in a format '%Y-%m-%d_%H:%M:%S'

    :return: integer

    """
    tz = pytz.timezone("UTC")
    dt = datetime.datetime.strptime\
        (timestr, '%Y-%m-%d_%H:%M:%S')
    dt = tz.localize(dt)
    return int(dt.timestamp())


def centre_of_box(box):
    """Compute centre of a box

    :box: [lat_min,lon_min,lat_max,lon_max]

    :return: (lon_centre,lat_centre)
    """
    return (box[1] + (box[3]-box[1])/2,
            box[0] + (box[2]-box[0])/2)
