import re
import os
import csv
import json
import shutil
import pyproj
import requests
import tempfile
import threading
import itertools
import subprocess

from cachetools import LRUCache
from collections import defaultdict


def list_files(path, regex):
    r = re.compile(regex)
    return [os.path.join(dp, f) \
            for dp, dn, filenames in \
            os.walk(self.path) \
            for f in filenames \
            if r.match(os.path.join(dp, f))]


class _Files_LRUCache(LRUCache):
    """Treat values of cache as values

    On popitem try to remove file.

    """

    def __init__(self, *args, **kwargs):
        super(_Files_LRUCache, self)\
            .__init__(*args, **kwargs)


    def popitem(self):
        key, value = super().popitem()

        try:
            os.remove(value)
        except:
            pass

        return key, value


class NRWData_Cache:
    """Keep track of stored files of the processes lidar data

    The class is thread-safe

    """

    def __init__(self, path, regex, fmt, maxsize):
        """

        :path: path where files are stored

        :regex: regex which matches each stored file, with grouping corresponding to the coordinates

        :fmt: describes how to form a filename from a tuple of coordinates

        :maxsize: maximum number of files to store

        """
        self.cache = _Files_LRUCache(maxsize = maxsize)
        self.path = path
        self.regex = re.compile(r'.*/' + regex)
        self.fmt = os.path.join(self.path, fmt)
        self._lock = threading.RLock()
        os.makedirs(self.path, exist_ok = True)
        self._fill_cache()


    def _fill_cache(self):
        for dp, dn, filenames in os.walk(self.path):
            for f in filenames:
                p = os.path.join(dp,f)
                if self.regex.match(p):
                    self.add(p)


    def coord2fn(self, coord):
        return self.fmt % coord


    def fn2coord(self, fn):
        return self.regex.findall(fn)[0]


    def add(self, path):
        if not self.regex.match(path):
            raise RuntimeError\
                ("provided path does not match regex")

        with self._lock:
            self.cache[path] = path


    def __contains__(self, path):
        if not self.regex.match(path):
            raise RuntimeError\
                ("provided path does not match regex")

        return path in self.cache


    def list_paths(self):
        return list(self.cache.keys())


class NRWData:
    """Get and process NRW data

    """

    def __init__(self, path, max_saved = 1000):
        """

        :path: where meta.json file is located.

        meta.json should contain fields: 'root_url' (format string), 'step' the integer is multiplied to get coordinate value, 'resolution' resolution to use in sampling the lidar data, 'epsg' coordinate system, 'box_step' size of each rectangle data, 'fn_meta' path to the meta csv file, 'meta_entry_regex' regex how to match coordinates

        the data is stored in the subdirectory 'cache'

        :max_saved: maximum number of saved processed files

        """
        self._cache = NRWData_Cache\
            (path = os.path.join(path,'cache'),
             regex = r'(.*)_(.*)\.tif',
             fmt = '%d_%d.tif',
             maxsize = max_saved)
        self._processing_lock = defaultdict(threading.RLock)

        self.path = path

        self._meta = self._read_meta()

        self._proj_from = pyproj\
            .Transformer.from_crs(self._meta['epsg'], 4326)

        self._files = dict()
        self._fill_files()


    def _read_meta(self):
        with open(os.path.join\
                  (self.path,'meta.json'),'r') as f:
            return json.load(f)


    def _coord_to_index_data(self, lat, lon):
        res = {}
        step = self._meta['step']
        box_step = self._meta['box_step']
        resolution = self._meta['box_resolution']

        res['file'] = self._cache.coord2fn((lon,lat))

        cmin = self._proj_from.transform\
            (lon*step,lat*step)
        cmax = self._proj_from.transform\
            ((lon+box_step)*step,(lat+box_step)*step)
        r = self._proj_from.transform\
            (lon*step+resolution,lat*step+resolution)
        res['coords'] = (cmin[0], cmax[0], cmin[1], cmax[1])
        res['resolution'] = \
            (abs(r[0] - cmin[0]),
             abs(r[1] - cmin[1]))

        return res, (res['coords'][0],
                     res['coords'][2],
                     res['coords'][1],
                     res['coords'][3])


    def _search_meta(self):
        regex = re.compile(self._meta['meta_entry_regex'])

        with open(os.path.join\
                  (self.path,
                   self._meta['fn_meta']),'r') as f:
            for line in f:
                if not regex.match(line):
                    continue

                lat = int(regex.sub(r'\2', line))
                lon = int(regex.sub(r'\1', line))
                yield self._coord_to_index_data\
                    (lat=lat, lon=lon)


    def _search_cache(self):
        for fn in self._cache.list_paths():
            lon, lat = self._cache.fn2coord(fn)
            yield self._coord_to_index_data\
                (lat = lat, lon = lon)


    def _fill_files(self):
        for data, box in self._search_meta():
            self._files[data['file']] = \
                {
                    'data': data,
                    'box': box
                }

        for data, box in self._search_cache():
            self._files[data['file']] = \
                {
                    'data': data,
                    'box': box
                }


    def update_index(self, index):
        for _, data in self._files.items():
            index.insert(0, data['box'],
                         obj = data['data'])
        return index


    def _write_pdaljson(self, path):
        data = {}
        data['pipeline'] = [
            'src.laz',
            {
                'filename': 'dtm_laz.tif',
                'gdaldriver': 'GTiff',
                'output_type': 'all',
                'resolution': self._meta['pdal_resolution'],
                'type': 'writers.gdal'
            }
        ]

        with open(os.path.join(path, 'pdal.json'), 'w') as f:
            json.dump(data, f)


    def _download_laz(self, url, path):
        r = requests.get(url, allow_redirects=True)
        open(os.path.join(path, 'src.laz'), 'wb')\
            .write(r.content)


    def _run_pdal(self, path):
        subprocess.run(['pdal','pipeline','pdal.json'],
                       cwd = path)


    def _convert_wgs84(self, path):
        subprocess.run(['gdalwarp',
                        'dtm_laz.tif',
                        'res.tif',
                        '-t_srs',
                        '+proj=longlat +ellps=WGS84'],
                       cwd = path)


    def _processing_path(self, path):
        # if a thread reacquired already processed path, do nothing
        if path in self._cache:
            return

        coord = self._cache.fn2coord(path)
        url = self._meta['root_url'] % coord

        wdir = tempfile.mkdtemp(dir=self.path)

        self._download_laz(url = url, path = wdir)
        self._run_pdal(path = wdir)
        self._convert_wgs84(path = wdir)
        os.rename(os.path.join(wdir,'res.tif'), path)

        shutil.rmtree(wdir)


    def get_path(self, path):
        if path not in self._files:
            return path

        if path not in self._cache:
            with self._processing_lock[path]:
                self._processing_path(path)
                # adding to cache in lock, as anothre thread can start
                # processing, before the cache is updated.
                self._cache.add(path)

        return path
