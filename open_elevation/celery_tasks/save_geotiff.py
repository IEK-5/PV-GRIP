import pickle
import logging
import numpy as np

from osgeo import gdal
from osgeo import gdal_array
from osgeo import osr

from open_elevation.celery_tasks \
    import CELERY_APP
from open_elevation.cache_fn_results \
    import cache_fn_results
from open_elevation.celery_one_instance \
    import one_instance
from open_elevation.utils \
    import get_tempfile, remove_file, format_dictionary


def save_gdal(ofn, array, geotransform, epsg):
    nrows, ncols, nchannels = array.shape
    output_raster = gdal.GetDriverByName('GTiff')\
                        .Create(ofn, ncols, nrows,
                                nchannels, gdal.GDT_Float32)
    output_raster.SetGeoTransform(geotransform)
    srs = osr.SpatialReference()
    srs.ImportFromEPSG(epsg)

    output_raster.SetProjection(srs.ExportToWkt())
    for channel in range(nchannels):
        output_raster.GetRasterBand(channel+1)\
                     .WriteArray(array[:,:,channel])
    output_raster.FlushCache()


def _save_geotiff(data, ofn):
    """Save data array to geotiff

    taken from: https://gis.stackexchange.com/a/37431
    see also: http://osgeo-org.1560.x6.nabble.com/gdal-dev-numpy-array-to-raster-td4354924.html

    :data: output of _sample_from_box

    :ofn: name of the output file
    """
    array = data['raster']

    xmin, ymin, xmax, ymax = data['mesh']['raster_box']
    nrows, ncols, nchannels = array.shape
    xres = (xmax - xmin)/float(ncols)
    yres = (ymax - ymin)/float(nrows)
    geotransform = (xmin, xres, 0, ymax, 0, -yres)
    save_gdal(ofn, data['raster'],
              geotransform, data['mesh']['epsg'])


@CELERY_APP.task()
@cache_fn_results()
@one_instance(expire = 10)
def save_geotiff(pickle_fn):
    logging.debug("save_geotiff\n{}"\
                  .format(format_dictionary(locals())))
    with open(pickle_fn, 'rb') as f:
        data = pickle.load(f)
    ofn = get_tempfile()
    try:
        _save_geotiff(data, ofn)
    except Exception as e:
        remove_file(ofn)
        raise e
    return ofn
