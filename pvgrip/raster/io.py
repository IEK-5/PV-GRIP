import os
import cv2
import shutil

import numpy as np

from osgeo import gdal
from osgeo import gdal_array
from osgeo import osr

from pvgrip.raster.gdalinterface \
    import GDALInterface
from pvgrip.utils.files \
    import get_tempdir
from pvgrip.utils.run_command \
    import run_command


def save_gdal(ofn, array, geotransform, epsg):
    """Save array to a geotiff format

    :ofn: name of the output file

    :array: number array

    :geotransform,epsg: whatever required by gdal

    :return: None
    """
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


def save_geotiff(data, ofn):
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


def save_png(data, ofn, normalize):
    """Save png from raster pickle data

    :data: whatever sample_raster outputs

    :ofn: output filename

    :normalize: if normalize image

    :return: ofn
    """
    arr = data['raster']
    if normalize:
        arr = 255*(arr - arr.min())/(arr.max() - arr.min())

    wdir = get_tempdir()
    tmpfn = os.path.join(wdir,'save.png')
    try:
        cv2.imwrite(tmpfn, arr)
        os.rename(tmpfn, ofn)
    finally:
        shutil.rmtree(wdir)

    return ofn


def save_binary_png_from_tif(ifn, ofn):
    """Save a binary tiff as a binary png

    :ifn: input tiff filename

    :ofn: output png filename
    """
    wdir = get_tempdir()

    try:
        run_command\
            (what = ['gdal_translate',
                     '-scale','0','1','0','255',
                     '-of','png',
                     ifn,'mask.png'],
             cwd = wdir)
        os.rename(os.path.join(wdir, 'mask.png'), ofn)
    finally:
        shutil.rmtree(wdir)


def save_pnghillshade(ifn, ofn):
    """Compute the hillshade image of the

    :ifn: input file (GeoTIFF)
    :ofn: output file (png)
    """
    wdir = get_tempdir()

    try:
        run_command\
            (what = ['gdaldem','hillshade',
                     '-of','png',ifn, 'hillshade.png'],
             cwd = wdir)
        os.rename(os.path.join(wdir, 'hillshade.png'), ofn)
    finally:
        shutil.rmtree(wdir)


def join_tif(ofn, ifns):
    """Join multiple tifs together

    :ofn: output filename

    :ifns: list of tifs

    :return: None
    """
    cur = GDALInterface(ifns[0])
    geotransform = cur.geo_transform
    epsg = cur.epsg
    res = cur.points_array
    for fn in ifns[1:]:
        res = np.concatenate\
            ((res, GDALInterface(fn).points_array),
             axis=2)
    save_gdal(ofn = ofn, array = res,
              geotransform = geotransform,
              epsg = epsg)
