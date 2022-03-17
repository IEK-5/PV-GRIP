import os
import cv2
import shutil
import pickle

import numpy as np

from osgeo import gdal
from osgeo import gdal_array
from osgeo import osr

from pvgrip.raster.gdalinterface \
    import GDALInterface
from pvgrip.raster.addscale \
    import addscale
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
    if 'raster' not in data:
        raise RuntimeError('"raster" field is not in pickle!')
    if 'mesh' not in data:
        raise RuntimeError('"mesh" field is not in pickle!')
    if 'raster_box' not in data['mesh']:
        raise RuntimeError\
            ('"raster_box" field is not in data["mesh"]!')
    if 'epsg' not in data['mesh']:
        raise RuntimeError\
            ('"epsg" field is not in data["mesh"]!')

    array = data['raster']

    xmin, ymin, xmax, ymax = data['mesh']['raster_box']
    nrows, ncols, nchannels = array.shape
    xres = (xmax - xmin)/float(ncols)
    yres = (ymax - ymin)/float(nrows)
    geotransform = (xmin, xres, 0, ymax, 0, -yres)
    save_gdal(ofn, data['raster'],
              geotransform, data['mesh']['epsg'])


def save_pickle(geotiff_fn, ofn):
    """Save pickle from a geotiff

    :geotiff_fn: input filename

    :ofn: output filename. pickle in the same format as
    _sample_from_box

    """
    mesh = {}
    raster = GDALInterface(geotiff_fn)
    xmin, xres, _, ymax, _, yres = raster.geo_transform
    nrows, ncols, nchannels = raster.get_shape()
    xmax = xres*ncols + xmin
    ymin = ymax - yres*nrows
    mesh['raster_box'] = xmin, ymin, xmax, ymax
    mesh['step'] = max(xres,yres)
    mesh['epsg'] = 4326

    with open(ofn,'wb') as f:
        pickle.dump({'raster': raster.points_array,
                     'mesh': mesh}, f)


def save_png(data, ofn, normalize,
             scale, scale_name, scale_constant):
    """Save png from raster pickle data

    :data: whatever sample_raster outputs

    :ofn: output filename

    :normalize: if normalize image

    :scale, scale_name, scale_constant: legend arguments

    :return: ofn
    """
    wdir = get_tempdir()

    arr = data['raster']

    if scale:
        arr_scale = addscale(img = arr, title = scale_name,
                             constant = scale_constant,
                             wdir = wdir)

    if normalize:
        arr = 255*(arr - arr.min())/(arr.max() - arr.min())

    if scale:
        arr = np.column_stack([arr, arr_scale])

    tmpfn = os.path.join(wdir,'save.png')
    try:
        cv2.imwrite(tmpfn, arr)
        os.rename(tmpfn, ofn)
    finally:
        shutil.rmtree(wdir)

    return ofn


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
