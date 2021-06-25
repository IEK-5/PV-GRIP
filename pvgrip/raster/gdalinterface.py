import numpy as np
from lazy import lazy


# Originally based on https://stackoverflow.com/questions/13439357/extract-point-from-raster-in-gdal
class GDALInterface(object):
    SEA_LEVEL = -9999


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


    def get_centre(self):
        ulx, xres, _, uly, _, yres = self.geo_transform
        cx = ulx + (self.src.RasterXSize/2 * xres)
        cy = uly + (self.src.RasterYSize/2 * yres)
        res = self._coordinate_transform_inv\
                  .TransformPoint(cx, cy, 0)
        return {'lon': res[0],
                'lat': res[1]}


    def get_shape(self):
        return (self.src.RasterYSize,
                self.src.RasterXSize,
                self.src.RasterCount)


    def get_resolution(self):
        _, xres, _, _, _, yres = self.geo_transform
        return (abs(xres), abs(yres))


    def loadMetadata(self):
        # open the raster and its spatial reference
        import osgeo.gdal as gdal
        self.src = gdal.Open(self.path)

        if self.src is None:
            raise Exception\
                ('Could not load GDAL file "%s"' % self.path)
        import osgeo.osr as osr
        spatial_reference_raster = osr.SpatialReference\
            (self.src.GetProjection())
        spatial_reference_raster.SetAxisMappingStrategy\
            (osr.OAMS_TRADITIONAL_GIS_ORDER)

        self.epsg = int(spatial_reference_raster\
                        .GetAttrValue('AUTHORITY',1))

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


    def _make3channels(self, arr):
        if 3 == len(arr.shape):
            # make channels last
            return np.transpose(arr, axes = (1,2,0))

        if 2 == len(arr.shape):
            arr = np.expand_dims(arr, axis = 2)

        return arr


    @lazy
    def points_array(self):
        res = self.src.ReadAsArray()
        return self._make3channels(res)


    def _slice_array(self, *args, **kwargs):
        res = self.src.ReadAsArray(*args, **kwargs)
        return self._make3channels(res)


    def _get_pixels(self, points):
        data = self._coordinate_transform\
                   .TransformPoints(points)
        data = [(u - self.geo_transform_inv[0],
                 v - self.geo_transform_inv[3])
                for u,v,_ in data]
        data = [(int(self.geo_transform_inv[4] * u +\
                     self.geo_transform_inv[5] * v),
                 int(self.geo_transform_inv[1] * u + \
                     self.geo_transform_inv[2] * v)) \
                for u,v in data]
        return data


    def _get_box(self, box):
        """Get a minimal raster box from degree box

        :box: degree box
        """
        box = self._get_pixels([(box[1],box[0]),
                                (box[3],box[2]),
                                (box[1],box[2]),
                                (box[3],box[0])])
        box = [min(box[1][0],box[2][0]),
               min(box[0][1],box[2][1]),
               max(box[0][0],box[3][0]),
               max(box[1][1],box[3][1])]
        args = {'xoff': box[1],
                'yoff': box[0],
                'xsize': box[3]-box[1] + 1,
                'ysize':box[2]-box[0] + 1}
        if args['xoff'] < 0:
            args['xoff'] = 0
        if args['yoff'] < 0:
            args['yoff'] = 0
        if args['xsize'] + args['xoff'] > self.src.RasterXSize:
            args['xsize'] = self.src.RasterXSize - args['xoff']
        if args['ysize'] + args['yoff'] > self.src.RasterYSize:
            args['ysize'] = self.src.RasterYSize - args['yoff']

        return args


    def lookup(self, points, box = None):
        """Lookup values of points

        :points: list of tuples (lon, lat)

        :box: optional box [lat_min,lon_min,lat_max,lon_max] where all
        points belong to

        :return: a list of values
        """
        points = self._get_pixels(points)
        if box:
            args = self._get_box(box)
        else:
            args = {'xoff': 0, 'yoff': 0,
                    'xsize': self.src.RasterXSize,
                    'ysize': self.src.RasterYSize}
        raster = self._slice_array(**args)

        res = []
        for y,x in points:
            y -= args['yoff']
            x -= args['xoff']
            if 0 <= y < raster.shape[0] \
               and 0 <= x < raster.shape[1]:
                res += [raster[y,x]]
            else:
                res += [self.SEA_LEVEL*np.ones((self.src.RasterCount,))]

        return res


    def lookup_one(self, lat, lon):
        return self.lookup([(lon, lat)], box=[lat,lon,lat,lon])[0]


    def close(self):
        self.src = None


    def __enter__(self):
        return self


    def __exit__(self, type, value, traceback):
        self.close()
