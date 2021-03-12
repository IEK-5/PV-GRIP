import os

from math import prod

from open_elevation.globals \
    import GRASS, PVGRIP_CONFIGS
from open_elevation.utils \
    import run_command


def upload_grass_data(wdir, geotiff_fn, grass_fn):
    """Upload data to a GRASS format

    :wdir: where the GRASS database is kept

    :geotiff: path to geotiff file

    :grass_fn: how to save the grass file

    :return: wdir
    """
    grass_path = os.path.join(wdir,'grass','PERMANENT')

    run_command\
        (what = [GRASS,'-c',geotiff_fn,'-e',
                 os.path.join(wdir,'grass')],
         cwd = wdir)
    run_command\
        (what = [GRASS, grass_path,
                 '--exec','r.external',
                 'input=' + geotiff_fn,
                 'output=' + grass_fn],
         cwd = wdir)

    return wdir


def download_grass_data(wdir, grass_fn, geotiff_fn):
    """Download data from a GRASS format

    :wdir: where the GRASS database is kept

    :geotiff: path to geotiff file

    :grass_fn: how to save the grass file

    :return: geotiff_fn
    """
    grass_path = os.path.join(wdir,'grass','PERMANENT')

    run_command\
        (what = [GRASS, grass_path,
                 '--exec','r.out.gdal',
                 'input=%s' % grass_fn,
                 'output=output.tif'],
         cwd = wdir)
    os.rename(os.path.join(wdir, 'output.tif'), geotiff_fn)

    return geotiff_fn
