import os
import shutil

import numpy as np

import pvgrip.raster.io as io

from pvgrip.raster.gdalinterface \
    import GDALInterface

from pvgrip.utils.files \
    import get_tempdir


def pickle_lookup(raster, points, box):
    wdir = get_tempdir()
    try:
        tifffn = os.path.join(wdir, 'geotiff')
        io.save_geotiff(raster, tifffn)
        interface = GDALInterface(tifffn)
        return np.array(interface.lookup(points = points,
                                         box = box))
    finally:
        shutil.rmtree(wdir)
