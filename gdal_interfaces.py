import json
import os
import re
import sys

import numpy as np
import osgeo.gdal as gdal
import osgeo.osr as osr

from cachetools import LRUCache
from lazy import lazy
from pprint import pprint
from rtree import index

# Originally based on https://stackoverflow.com/questions/13439357/extract-point-from-raster-in-gdal
class GDALInterface(object):
    SEA_LEVEL = 0
    def __init__(self, tif_path):
        super(GDALInterface, self).__init__()
        self.tif_path = tif_path
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
        self.src = gdal.Open(self.tif_path)

        if self.src is None:
            raise Exception('Could not load GDAL file "%s"' % self.tif_path)
        spatial_reference_raster = osr.SpatialReference(self.src.GetProjection())

        # get the WGS84 spatial reference
        spatial_reference = osr.SpatialReference()
        spatial_reference.ImportFromEPSG(4326)  # WGS84

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

class GDALTileInterface(object):
    def __init__(self, tiles_folder, summary_file, open_interfaces_size=5):
        super(GDALTileInterface, self).__init__()
        self.tiles_folder = tiles_folder
        self.summary_file = summary_file
        self.index = index.Index()
        self.interfaces = LRUCache(maxsize = open_interfaces_size)


    def _open_gdal_interface(self, path):
        if path not in self.interfaces:
            # close cleanly the oldest interface
            if self.interfaces.currsize >= self.interfaces.maxsize:
                oldest = self.interfaces.popitem()[1]
                oldest.close()

            self.interfaces[path] = GDALInterface(path)

        return self.interfaces[path]


    def _all_files(self):
        return [os.path.join(dp, f) \
                for dp, dn, filenames in \
                os.walk(self.tiles_folder) \
                for f in filenames]


    def create_summary_json(self):
        all_coords = []
        for fn in self._all_files():
            try:
                i = self._open_gdal_interface(fn)
                coords = i.get_corner_coords()
                all_coords += [
                    {
                        'file': fn,
                        'coords': ( coords['BOTTOM_RIGHT'][1],  # latitude min
                                    coords['TOP_RIGHT'][1],  # latitude max
                                    coords['TOP_LEFT'][0],  # longitude min
                                    coords['TOP_RIGHT'][0],  # longitude max

                        ),
                        'resolution': i.get_resolution()
                    }
                ]
            except:
                print("""
                      Could not process file:
                          %s
                      Skipping...""" % fn,
                      file = sys.stderr)
                continue

        with open(self.summary_file, 'w') as f:
            json.dump(all_coords, f)

        self.all_coords = all_coords

        self._build_index()


    def read_summary_json(self):
        with open(self.summary_file) as f:
            self.all_coords = json.load(f)

        self._build_index()


    def get_directories(self):
        return list(set([os.path.dirname(fn['file']) \
                         for fn in self.all_coords]))


    def lookup(self, lat, lng, data_re):

        nearest = list(self.index.nearest((lat, lng), 1, objects='raw'))

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
        return {'elevation': int(gdal_interface.lookup(lat, lng)),
                'resolution': coords['resolution']}

    def _build_index(self):
        index_id = 1
        for e in self.all_coords:
            e['index_id'] = index_id
            left, bottom, right, top = (e['coords'][0], e['coords'][2], e['coords'][1], e['coords'][3])
            self.index.insert( index_id, (left, bottom, right, top), obj=e)
