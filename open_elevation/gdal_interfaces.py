import os
import re
import sys
import json
import logging
import tempfile
import threading

import numpy as np
import osgeo.gdal as gdal
import osgeo.osr as osr

from cachetools import LRUCache
from lazy import lazy

from tqdm import tqdm

import open_elevation.polygon_index as polygon_index
import open_elevation.nrw_las as nrw_las
import open_elevation.utils as utils


def _polygon_from_box(box):
    return [(box[1],box[0]),
            (box[3],box[0]),
            (box[3],box[2]),
            (box[1],box[2])]


def in_directory(fn, paths):
    fn = os.path.abspath(fn)

    for path in paths:
        path = os.path.abspath(path)

        if os.path.commonprefix([fn, path]) == path:
            return path

    return None


def choose_highest_resolution(nearest):
    if not nearest:
        raise Exception('No data for coordinate exist')

    if len(nearest) == 1:
        return nearest[0]

    return nearest[np.argmin([np.prod(x['resolution']) \
                              for x in nearest])]


# Originally based on https://stackoverflow.com/questions/13439357/extract-point-from-raster-in-gdal
class GDALInterface(object):
    SEA_LEVEL = 0
    def __init__(self, path):
        super(GDALInterface, self).__init__()
        self.path = path
        self.loadMetadata()


    def get_corner_coords(self):
        ulx, xres, _, uly, _, yres = self.geo_transform
        lrx = ulx + (self.src.RasterXSize * xres)
        lry = uly + (self.src.RasterYSize * yres)
        return {
            'TOP_LEFT': \
            self._coordinate_transform_inv\
            .TransformPoint(ulx, uly, 0),
            'TOP_RIGHT': \
            self._coordinate_transform_inv\
            .TransformPoint(lrx, uly, 0),
            'BOTTOM_LEFT': \
            self._coordinate_transform_inv\
            .TransformPoint(ulx, lry, 0),
            'BOTTOM_RIGHT': \
            self._coordinate_transform_inv\
            .TransformPoint(lrx, lry, 0),
        }


    def get_resolution(self):
        _, xres, _, _, _, yres = self.geo_transform
        return (abs(xres), abs(yres))


    def loadMetadata(self):
        # open the raster and its spatial reference
        self.src = gdal.Open(self.path)

        if self.src is None:
            raise Exception\
                ('Could not load GDAL file "%s"' % self.path)
        spatial_reference_raster = osr.SpatialReference\
            (self.src.GetProjection())
        spatial_reference_raster.SetAxisMappingStrategy\
            (osr.OAMS_TRADITIONAL_GIS_ORDER)

        spatial_reference = osr.SpatialReference()
        spatial_reference.ImportFromEPSG(4326) # WGS84
        spatial_reference.SetAxisMappingStrategy\
            (osr.OAMS_TRADITIONAL_GIS_ORDER)

        # coordinate transformation
        self._coordinate_transform = \
            osr.CoordinateTransformation\
            (spatial_reference, spatial_reference_raster)
        self._coordinate_transform_inv = \
            osr.CoordinateTransformation\
            (spatial_reference_raster, spatial_reference)
        gt = self.geo_transform = self.src.GetGeoTransform()
        dev = (gt[1] * gt[5] - gt[2] * gt[4])
        self.geo_transform_inv = \
            (gt[0], gt[5] / dev, -gt[2] / dev,
             gt[3], -gt[4] / dev, gt[1] / dev)


    @lazy
    def points_array(self):
        b = self.src.GetRasterBand(1)
        return b.ReadAsArray()


    def print_statistics(self):
        print(self.src.GetRasterBand(1)\
              .GetStatistics(True, True))


    def _get_pixel(self, lat, lon):
        # get coordinate of the raster
        xgeo, ygeo, zgeo = self._coordinate_transform\
                               .TransformPoint(lon, lat, 0)

        # convert it to pixel/line on band
        u = xgeo - self.geo_transform_inv[0]
        v = ygeo - self.geo_transform_inv[3]
        # FIXME this int() is probably bad idea, there should be half cell size thing needed
        xpix = int(self.geo_transform_inv[1] * u + \
                   self.geo_transform_inv[2] * v)
        ylin = int(self.geo_transform_inv[4] * u + \
                   self.geo_transform_inv[5] * v)

        return ylin, xpix


    def lookup(self, lat, lon):
        try:
            ylin, xpix = self._get_pixel(lat = lat, lon = lon)

            # look the value up
            v = self.points_array[ylin, xpix]

            return v if v > -5000 else self.SEA_LEVEL
        except Exception as e:
            logging.error("""
            cannot lookup coordinate!
            lon = %f
            lat = %f
            error: %s
            """ % (lon, lat, str(e)))
            return self.SEA_LEVEL


    def close(self):
        self.src = None

    def __enter__(self):
        return self

    def __exit__(self, type, value, traceback):
        self.close()


class Interface_LRUCache(LRUCache):
    def popitem(self):
        key, value = super().popitem()
        value.close()

        logging.info("""
        pop GDAL Interface from memory: %s
        """ % key)
        return key, value


class GDALTileInterface(object):
    def __init__(self, tiles_folder, index_file,
                 open_interfaces_size=5,
                 use_only_index = False):
        """The class keeps cache of the read files

        :open_interfaces_size: size of the LRU cache of the loaded interfaces. The interfaces are read lazily.

        :use_only_index: if True, then tiles_folder are ignored
        and files are not searched. Instead the index is loaded directly from the index_file path. The index_file is not saved

        """
        super(GDALTileInterface, self).__init__()
        self.path = tiles_folder
        self._index_fn = index_file
        self._index = polygon_index.Polygon_File_Index()
        self._interfaces = Interface_LRUCache\
            (maxsize = open_interfaces_size)
        self._interfaces_lock = threading.RLock()

        self._las_dirs = dict()
        self._all_coords = []

        if use_only_index:
            self.path = os.path.dirname(self._index_fn)
            self._data_from_index()
            return

        self._data_from_files()


    def _data_from_files(self):
        self._find_las_dirs(path = self.path)
        self._fill_all_coords()

        if os.path.exists(self._index_fn) \
           and self._files_timestamp() < \
           os.stat(self._index_fn).st_mtime:
            self._index.load(self._index_fn)
            return

        self._build_index()
        self._index.save(self._index_fn)


    def _data_from_index(self):
        self._index.load(self._index_fn)

        path = os.path.commonprefix\
            ([x for x in self._index.files()
              if x is not None] + \
             [x for x in self._index.files(what = 'las_meta')
              if x is not None])
        self._find_las_dirs(path)


    def _files_timestamp(self):
        files = {}
        res = 0
        for x in self._all_coords:
            fn = os.path.dirname\
                (os.path.abspath(x['file']))
            if fn in files:
                continue
            mtime = os.stat(fn).st_mtime
            files[fn] = mtime
            if mtime > res:
                res = mtime

        for path, _ in self._las_dirs.items():
            if path in files:
                continue
            mtime = os.stat(path).st_mtime
            files[path] = mtime
            if mtime > res:
                res = mtime

        return res


    def _find_las_dirs(self, path):
        for fn in utils.list_files\
            (path = path,
             regex = '.*/las_meta\.json$'):
            dn = os.path.abspath(os.path.dirname(fn))
            self._las_dirs[dn] = nrw_las.NRWData(path = dn)


    def print_used_las_space(self):
        res = []
        for path, las in self._las_dirs.items():
            res += [("Size of cache in %s = %s GB)" %
                     (path,las._cache.size()/(1024**3)))]
        return '\n'.join(res)


    def open_gdal_interface(self, path):
        if path not in self._interfaces:
            las_path = in_directory(path, self._las_dirs.keys())
            if las_path:
                path = self._las_dirs[las_path].get_path(path)

            with self._interfaces_lock:
                self._interfaces[path] = GDALInterface(path)

        return self._interfaces[path]


    def _get_index_data(self, fn, interface):
        coords = interface.get_corner_coords()
        return {'file': fn,
                'resolution': interface.get_resolution(),
                'polygon': \
                [ coords['BOTTOM_LEFT'],
                  coords['TOP_LEFT'],
                  coords['TOP_RIGHT'],
                  coords['BOTTOM_RIGHT'],
                ]}


    def _fill_all_coords(self):
        for fn in tqdm(utils.list_files(self.path, regex = '.*'),
                       desc = "Searching for Geo files"):
            # ignore las directories
            if in_directory(fn, self._las_dirs.keys()):
                continue

            try:
                i = self.open_gdal_interface(fn)
                coords = i.get_corner_coords()
                self._all_coords += \
                    [self._get_index_data(fn, i)]
            except Exception as e:
                logging.error("""
                Could not process file: %s
                Error: %s
                Skipping...
                """ % (fn, str(e)))
                continue


    def get_directories(self):
        res = list(set([os.path.dirname(fn['file']) \
                        for fn in self._all_coords]))
        res += list(self._las_dirs.keys())
        return res


    def lookup(self, lat, lon, data_re):
        nearest = list(self._index.nearest((lon, lat)))

        if data_re:
            data_re = re.compile(data_re)
            nearest = [x for x in nearest \
                       if data_re.match(x['file'])]

        coords = choose_highest_resolution(nearest)
        gdal_interface = self.open_gdal_interface\
            (coords['file'])
        return {'elevation': float(gdal_interface.lookup(lat, lon)),
                'resolution': coords['resolution']}


    def subset(self, box, data_re):
        index = self._index.intersect\
            (regex = data_re,
             polygon = _polygon_from_box(box))
        fd = tempfile.NamedTemporaryFile\
            (dir = self.path, delete = False)
        fn = os.path.join(self.path,fd.name)
        index.save(fn)
        return fn


    def _build_index(self):
        for e in tqdm(self._all_coords,
                      desc = "Building index"):
            self._index.insert(data=e)

        for _,v in self._las_dirs.items():
            self._index = v.update_index(self._index)
