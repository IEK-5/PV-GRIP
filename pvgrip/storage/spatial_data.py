import os
import re
import logging

from tqdm import tqdm

from shapely import geometry

from pvgrip.utils.files\
    import list_files

from pvgrip.lidar.nrw_las \
    import NRWData

from pvgrip.raster.gdalinterface \
    import GDALInterface

from pvgrip.storage.cassandra_datasets \
    import Datasets

from cassandra_io.spatial_index \
    import Cassandra_Spatial_Index

from pvgrip.utils.iterate_csv \
    import iterate_csv


def _in_directory(fn, paths):
    fn = os.path.abspath(fn)

    for path in paths:
        path = os.path.abspath(path)

        if os.path.commonprefix([fn, path]) == path:
            return path

    return None


def _polygon_from_box(box):
    return geometry.Polygon([(box[1],box[0]),
                             (box[3],box[0]),
                             (box[3],box[2]),
                             (box[1],box[2])])


def _polygon_from_list_rasters(rasters):
    polygon = None

    for x in rasters:
        if not polygon:
            polygon = _polygon_from_box(x['box'])
            continue

        polygon = polygon.union(_polygon_from_box(x['box']))

    return polygon


def _subset_filter_how(x, data_re):
    data_re = re.compile(data_re)

    if not data_re.match(x['file']):
        return False

    return True


class Spatial_Data:
    """An object to upload and query spatial data

    """

    def __init__(self, cassandra_ips, storage,
                 index_args, base_args):
        """init

        :cassandra_ips: ips for cassandra connection

        :storage: remote storage to use

        :kwargs: arguments used for Cassandra_Spatial_Index

        """
        self.storage = storage
        self._cassandra_ips = cassandra_ips
        self.index = Cassandra_Spatial_Index\
            (cluster_ips = self._cassandra_ips,
             **index_args, **base_args)
        logging.debug("Spatial_Data: after self.index = ")
        self.datasets = Datasets\
            (cluster_ips = self._cassandra_ips,
             **base_args)
        logging.debug("Spatial_Data: after self.datasets = ")


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

                if fn not in self.storage:
                    self.storage.upload(fn, fn)
                self.index.insert(data = data)
                self.datasets.add(os.path.dirname(fn))
            except Exception as e:
                logging.error("""
                Could not process file: %s
                Error: %s
                Skipping...
                """% (fn, str(e)))
                continue


    def upload_from_csv(self, csvfn):
        from pvgrip.storage.remotestorage_path \
            import RemoteStoragePath, is_remote_path

        for row in tqdm(iterate_csv(csvfn, header=None)):
            fn = row[0]
            try:
                if not is_remote_path(fn):
                    logging.warning("{} is not a remote_path")
                    continue

                fn = RemoteStoragePath(fn).get_locally()
                data = self._get_index_data(fn)
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


    def subset(self, data_re, box=None, rasters = None):
        """Generate a subset index containing required data

        :data_re: regular expression to match filenames

        :box: a box describing coordinates of a box, if route is not
        None box gives positions of a box relative to each coordinate
        in a route

        :rasters: a list of dictionaries containing 'box' field. If
        not None, 'box' argument is ignored, and a polygon is formed
        using the provided list of boxes in the 'rasters' list

        :return: Polygon_File_Index
        """
        if rasters:
            pg = _polygon_from_list_rasters(rasters)
        else:
            pg = _polygon_from_box(box)

        index = self.index.intersect(polygons = pg)
        index = index.filter\
            (how = lambda x:
             _subset_filter_how(x, data_re))

        if 0 == index.size():
            raise RuntimeError\
                ("no data available for the selected location")

        return index


    def get_datasets(self):
        return list(self.datasets.list())
