import os

from math import prod

from pvgrip.globals \
    import GRASS, PVGRIP_CONFIGS
from pvgrip.utils.files \
    import get_tempfile, remove_file
from pvgrip.raster.io \
    import join_tif
from pvgrip.utils.run_command \
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


def upload_grass_many(wdir, **kwargs):
    """Upload multiple files to the grass storage

    :wdir: working directory

    :kwargs: a dictionary where keys are used as grass storage names,
    and values are the path to the geotiff files

    """
    res = {}
    for k, v in kwargs.items():
        if v is None:
            res[k] = v
            continue

        upload_grass_data(wdir = wdir,
                          geotiff_fn = v,
                          grass_fn = k)
        res[k] = k

    return res


def combine_grass_many(wdir, ofn, grass_outputs):
    """Combine multiple outputs in grass and export tiff

    :wdir: working directory

    :ofn: output filename

    :grass_outputs: list of grass output files

    """
    grass_path = os.path.join(wdir, 'grass','PERMANENT')

    ofns = []
    try:
        for fn in grass_outputs:
            x = get_tempfile()
            ofns += [x]
            download_grass_data(wdir = wdir,
                                grass_fn = fn,
                                geotiff_fn = x)
        join_tif(ofn = ofn, ifns = ofns)
    finally:
        for x in ofns:
            remove_file(x)

    return ofn
