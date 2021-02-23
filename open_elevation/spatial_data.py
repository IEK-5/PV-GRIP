import os
import re

from tqdm import tqdm

from open_elevation.utils\
    import list_files

from open_elevation.nrw_las \
    import NRWData
from open_elevation.gdalinterface \
    import GDALInterface

from open_elevation.cassandra_datasets \
    import Datasets
from cassandra_io.files \
    import Cassandra_Files
from cassandra_io.spatial_index \
    import Cassandra_Spatial_Index


def _in_directory(fn, paths):
    fn = os.path.abspath(fn)

    for path in paths:
        path = os.path.abspath(path)

        if os.path.commonprefix([fn, path]) == path:
            return path

    return None


def _polygon_from_box(box):
    return [(box[1],box[0]),
            (box[3],box[0]),
            (box[3],box[2]),
            (box[1],box[2])]


def _subset_filter_how(x, data_re, stat):
    data_re = re.compile(data_re)

    if 'stat' not in x and \
       not data_re.match(x['file']):
        return False

    if 'stat' in x and \
       not data_re.match(x['remote_meta']):
        return False

    if 'stat' in x and \
       stat != x['stat']:
        return False

    return True


class Spatial_Data:
    """An object to upload and query spatial data

    """

    def __init__(self, cfs):
        self.cfs = cfs
        self.index = Cassandra_Spatial_Index\
            (cluster_ips = self.cfs._cluster_ips)
        self.datasets = Datasets\
            (cluster_ips = self.cfs._cluster_ips)


    def _get_index_data(self, fn):
        interface = GDALInterface(fn)
        coords = interface.get_corner_coords()
        return {'file': fn,
                'resolution': interface.get_resolution(),
                'polygon': \
                [ coords['BOTTOM_LEFT'],
                  coords['TOP_LEFT'],
                  coords['TOP_RIGHT'],
                  coords['BOTTOM_RIGHT'],
                 ]}


    def _upload_raster_data(self, path, las_dirs):

        for fn in tqdm(list_files(os.path.realpath(path),
                                  regex = '.*'),
                       desc = "Processing Geo files in %s"\
                       % path):
            try:
                if _in_directory(fn, las_dirs):
                    continue

                data = self._get_index_data(fn)

                self.cfs.upload(ifn = fn,
                                cassandra_fn = fn)
                self.index.insert(data = data)
                self.datasets.add(os.path.dirname(fn))
            except Exception as e:
                logging.error("""
                Could not process file: %s
                Error: %s
                Skipping...
                """% (fn, str(e)))
                continue


    def _upload_nrw_remote(self, path, nrw_data):
        for data in tqdm(nrw_data.search_meta(),
                         desc = "Reading %s index"\
                         % path):
            try:
                self.index.insert(data = data)
            except Exception as e:
                logging.error("""
                Could not process file: %s
                Error: %s
                Skipping...
                """% (data['file'], str(e)))
                continue

        self.datasets.add(path)


    def _find_las_dirs(self, path):
        las_dirs = {}
        for fn in list_files\
            (path = path,
             regex = '.*/remote_meta\.json$'):
            dn = os.path.abspath(os.path.dirname(fn))
            las_dirs[dn] = NRWData(path = dn)

        return las_dirs


    def upload(self, path):
        """Search data and upload data

        :path: directory where new data reside

        """
        las_dirs = self._find_las_dirs(path)
        for p, v in las_dirs.items():
            self._upload_nrw_remote(p, v)

        self._upload_raster_data(path, las_dirs.keys())


    def subset(self, box, data_re, stat,
               raise_on_empty = True):
        pg = _polygon_from_box(box)
        index = self.index.intersect(polygon = pg)
        index = index.filter\
            (how = lambda x:
             _subset_filter_how(x, data_re, stat))

        if raise_on_empty and 0 == index.size():
            raise RuntimeError\
                ("no data available for the selected location")

        return index


    def get_datasets(self):
        return list(self.datasets.list())
