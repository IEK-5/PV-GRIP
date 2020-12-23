import json
import os
import re
import sys
import threading

import numpy as np
import osgeo.gdal as gdal
import osgeo.osr as osr

from cachetools import LRUCache
from lazy import lazy
from pprint import pprint

from tqdm import tqdm

from rtree_uniq import \
    SpatialFileIndex, save_index_json
from nrw_las import \
    list_files, NRWData


def in_directory(fn, paths):
    fn = os.path.abspath(fn)

    for path in paths:
        path = os.path.abspath(path)

        if os.path.commonprefix([fn, path]) == path:
            return path

    return None


# Originally based on https://stackoverflow.com/questions/13439357/extract-point-from-raster-in-gdal
class GDALInterface(object):
    SEA_LEVEL = 0
    def __init__(self, path):
        super(GDALInterface, self).__init__()
        self.path = path
        self.loadMetadata()


    def get_corner_coords(self):
        ulx, xres, xskew, uly, yskew, yres = self.geo_transform
        lrx = ulx + (self.src.RasterXSize * xres)
        lry = uly + (self.src.RasterYSize * yres)
        return {
            'TOP_LEFT': (ulx, uly),
            'TOP_RIGHT': (lrx, uly),
            'BOTTOM_LEFT': (ulx, lry),
            'BOTTOM_RIGHT': (lrx, lry),
        }


    def get_resolution(self):
        _, xres, _, _, _, yres = self.geo_transform
        return (abs(xres), abs(yres))


    def loadMetadata(self):
        # open the raster and its spatial reference
        self.src = gdal.Open(self.path)

        if self.src is None:
            raise Exception('Could not load GDAL file "%s"' % self.path)
        spatial_reference_raster = osr.SpatialReference(self.src.GetProjection())
        spatial_reference_raster.SetAxisMappingStrategy\
            (osr.OAMS_TRADITIONAL_GIS_ORDER)

        spatial_reference = osr.SpatialReference()
        spatial_reference.ImportFromEPSG(4326) # WGS84
        spatial_reference.SetAxisMappingStrategy\
            (osr.OAMS_TRADITIONAL_GIS_ORDER)

        # coordinate transformation
        self.coordinate_transform = osr.CoordinateTransformation(spatial_reference, spatial_reference_raster)
        gt = self.geo_transform = self.src.GetGeoTransform()
        dev = (gt[1] * gt[5] - gt[2] * gt[4])
        self.geo_transform_inv = (gt[0], gt[5] / dev, -gt[2] / dev,
                                  gt[3], -gt[4] / dev, gt[1] / dev)


    @lazy
    def points_array(self):
        b = self.src.GetRasterBand(1)
        return b.ReadAsArray()


    def print_statistics(self):
        print(self.src.GetRasterBand(1).GetStatistics(True, True))


    def lookup(self, lat, lon):
        try:

            # get coordinate of the raster
            xgeo, ygeo, zgeo = self.coordinate_transform.TransformPoint(lon, lat, 0)

            # convert it to pixel/line on band
            u = xgeo - self.geo_transform_inv[0]
            v = ygeo - self.geo_transform_inv[3]
            # FIXME this int() is probably bad idea, there should be half cell size thing needed
            xpix = int(self.geo_transform_inv[1] * u + self.geo_transform_inv[2] * v)
            ylin = int(self.geo_transform_inv[4] * u + self.geo_transform_inv[5] * v)

            # look the value up
            v = self.points_array[ylin, xpix]

            return v if v != -32768 else self.SEA_LEVEL
        except Exception as e:
            print(e)
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

        return key, value


class GDALTileInterface(object):
    def __init__(self, tiles_folder, index_file, open_interfaces_size=5):
        super(GDALTileInterface, self).__init__()
        self.path = tiles_folder
        self._index = SpatialFileIndex()
        self._interfaces = Interface_LRUCache\
            (maxsize = open_interfaces_size)
        self._interfaces_lock = threading.RLock()

        self._las_dirs = dict()
        self._find_las_dirs()

        self._all_coords = []
        self._fill_all_coords()

        self._build_index()
        save_index_json(self._index, index_file)


    def _find_las_dirs(self):
        for fn in list_files(path = self.path,
                             regex = '.*/las_meta\.json$'):
            dn = os.path.abspath(os.path.dirname(fn))
            self._las_dirs[dn] = NRWData(path = dn)


    def _open_gdal_interface(self, path):
        if path not in self._interfaces:
            las_path = in_directory(path, self._las_dirs.keys())
            if las_path:
                path = self._las_dirs[las_path].get_path(path)

            with self._interfaces_lock:
                self._interfaces[path] = GDALInterface(path)

        return self._interfaces[path]


    def _fill_all_coords(self):
        for fn in tqdm(list_files(self.path, regex = '.*'),
                       desc = "Searching for Geo files"):
            # ignore las directories
            if in_directory(fn, self._las_dirs.keys()):
                continue

            try:
                i = self._open_gdal_interface(fn)
                coords = i.get_corner_coords()
                self._all_coords += [
                    {
                        'file': fn,
                        'coords': \
                        ( coords['BOTTOM_RIGHT'][1],  # latitude min
                          coords['TOP_RIGHT'][1],     # latitude max
                          coords['TOP_LEFT'][0],      # longitude min
                          coords['TOP_RIGHT'][0],     # longitude max
                        ),
                        'resolution': i.get_resolution()
                    }
                ]
            except Exception as e:
                print("""
Could not process file:
        %s
Error:
        %s
Skipping...""" % (fn, str(e)),
                      file = sys.stderr)
                continue


    def get_directories(self):
        res = list(set([os.path.dirname(fn['file']) \
                        for fn in self._all_coords]))
        res += list(self._las_dirs.keys())
        return res


    def lookup(self, lat, lng, data_re):
        nearest = list(self._index.nearest((lat, lng), 1, objects='raw'))

        if data_re:
            data_re = re.compile(data_re)
            nearest = [x for x in nearest if data_re.match(x['file'])]

        if not nearest:
            raise Exception('Invalid latitude/longitude')

        if len(nearest) > 1:
            idx = np.argmin([np.prod(x['resolution']) \
                             for x in nearest])
        else:
            idx = 0
        coords = nearest[idx]

        gdal_interface = self._open_gdal_interface(coords['file'])
        return {'elevation': float(gdal_interface.lookup(lat, lng)),
                'resolution': coords['resolution']}


    def _build_index(self):
        for e in tqdm(self._all_coords,
                      desc = "Building index"):
            left, bottom, right, top = (e['coords'][0], e['coords'][2], e['coords'][1], e['coords'][3])
            self._index.insert( 0, (left, bottom, right, top), obj=e)

        for _,v in self._las_dirs.items():
            self._index = v.update_index(self._index)
