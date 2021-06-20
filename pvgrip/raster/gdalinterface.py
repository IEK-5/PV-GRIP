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


    @lazy
    def points_array(self):
        res = self.src.ReadAsArray()
        if 3 == len(res.shape):
            return np.transpose(res, axes = (1,2,0))

        if 2 == len(res.shape):
            res = np.expand_dims(res, axis = 2)

        return res


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


    def lookup(self, points):
        points = self._get_pixels(points)
        res = []
        for y,x in points:
            if 0 <= y < self.src.RasterYSize \
               and 0 <= x < self.src.RasterXSize:
                res += [self.points_array[y,x]]
            else:
                res += [self.SEA_LEVEL*np.ones((self.src.RasterCount,))]

        return res


    def lookup_one(self, lat, lon):
        return self.lookup([lon, lat])[0]


    def close(self):
        self.src = None


    def __enter__(self):
        return self


    def __exit__(self, type, value, traceback):
        self.close()
