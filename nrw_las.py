import re
import os
import csv
import json
import pyproj
import requests
import diskcache
import itertools

from tqdm import tqdm

from tasks import task_las_processing

class TASK_RUNNING(Exception):
    pass


def list_files(path, regex):
    r = re.compile(regex)
    return [os.path.join(dp, f) \
            for dp, dn, filenames in \
            os.walk(path) \
            for f in filenames \
            if r.match(os.path.join(dp, f))]


class _Files_LRUCache:


    def __init__(self, maxsize, path = '.'):
        """Implements LRU list of file paths

        :maxsize: maximum number of files to store

        :path: path where to store diskcache db

        """
        self.maxsize = maxsize
        self._deque = diskcache.Deque\
            (directory = os.path.join\
             (path,"_Files_LRUCache_Deque"))


    def _update(self, item):
        if item in self._deque:
            self._deque.remove(item)
        self._deque.append(item)


    def add(self, item):
        with self._deque.transact():
            if len(self._deque) >= self.maxsize:
                self.popleft()
            self._update(item)


    def __contains__(self, item):
        res = item in self._deque

        if res:
            self._update(item)

        return res


    def __len__(self):
        return len(self._deque)


    def popleft(self):
        with self._deque.transact():
            item = self._deque.popleft()
            try:
                os.remove(item)
            except:
                pass
            return item


class NRWData_Cache:
    """Keep track of stored files of the processes lidar data

    The class is thread-/ and process-safe

    """

    def __init__(self, path, regex, fmt, maxsize):
        """

        :path: path where files are stored

        :regex: regex which matches each stored file, with grouping corresponding to the coordinates

        :fmt: describes how to form a filename from a tuple of coordinates

        :maxsize: maximum number of files to store

        """
        self.cache = _Files_LRUCache(maxsize = maxsize, path = path)
        self.path = path
        self.regex = re.compile(r'.*/' + regex)
        self.fmt = os.path.join(self.path, fmt)
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
        return self.regex.findall(fn)[0][0:2]


    def path2pathfmt(self, path):
        return self.coord2fn\
            (tuple([int(x) for x in self.fn2coord(path)]) \
             + ('%s',))


    def add(self, path):
        if not self.regex.match(path):
            raise RuntimeError\
                ("provided path does not match regex")

        self.cache.add(path)


    def __contains__(self, path):
        if not self.regex.match(path):
            raise RuntimeError\
                ("provided path does not match regex")

        return path in self.cache


    def list_paths(self):
        with self.cache._deque.transact():
            res = list(self.cache._deque)
        return res


class NRWData:
    """Get and process NRW data

    """

    def __init__(self, path, max_saved = 1000):
        """

        :path: where las_meta.json file is located.

        las_meta.json should contain fields: 'root_url' (format string), 'step' the integer is multiplied to get coordinate value, 'resolution' resolution to use in sampling the lidar data, 'epsg' coordinate system, 'box_step' size of each rectangle data, 'fn_meta' path to the meta csv file, 'meta_entry_regex' regex how to match coordinates

        the data is stored in the subdirectory 'cache'

        :max_saved: maximum number of saved processed files

        """
        self._cache = NRWData_Cache\
            (path = os.path.abspath\
             (os.path.join(path,'cache')),
             regex = r'(.*)_(.*)_(.*)\.tif',
             fmt = '%d_%d_%s.tif',
             maxsize = max_saved)

        self.path = path
        self._las_whats = ['min','max','count']

        self._meta = self._read_meta()

        self._proj_from = pyproj\
            .Transformer.from_crs(self._meta['epsg'], 4326,
                                  always_xy=True)

        self._known_files = dict()
        self._fill_files()


    def _read_meta(self):
        with open(os.path.join\
                  (self.path,'las_meta.json'),'r') as f:
            return json.load(f)


    def _coord_to_index_data(self, lat, lon, what):
        res = {}
        step = self._meta['step']
        box_step = self._meta['box_step']
        resolution = self._meta['box_resolution']

        res['file'] = self._cache.coord2fn((lon,lat,what))

        p0 = self._proj_from.transform\
            (lon*step,lat*step)
        p1 = self._proj_from.transform\
            ((lon+box_step)*step,lat*step)
        p2 = self._proj_from.transform\
            ((lon+box_step)*step,(lat+box_step)*step)
        p3 = self._proj_from.transform\
            (lon*step,(lat+box_step)*step)
        r = self._proj_from.transform\
            (lon*step+resolution,lat*step+resolution)

        res['polygon'] = [p0,p1,p2,p3]
        res['resolution'] = (abs(r[0] - p0[0]),
                             abs(r[1] - p1[1]))

        return res


    def _download_index(self):
        url = self._meta['meta_url']
        res = requests.get(url, allow_redirects = True)

        if 200 != res.status_code:
            raise RuntimeError("""
            cannot download index data!
            status_code: %d
            url: %s
            """ % (res.status_code, url))

        return json.loads(res.text)


    def _search_meta(self):
        regex = re.compile(self._meta['meta_entry_regex'])
        index = self._download_index()['datasets'][0]['files']

        for item in index:
            fn = item['name']
            if not regex.match(fn):
                continue

            lon = int(regex.sub(r'\1', fn))
            lat = int(regex.sub(r'\2', fn))
            for what in self._las_whats:
                yield self._coord_to_index_data\
                    (lat=lat, lon=lon, what = what)


    def _search_cache(self):
        for fn in self._cache.list_paths():
            lon, lat = self._cache.fn2coord(fn)
            for what in self._las_whats:
                yield self._coord_to_index_data\
                    (lat = int(lat), lon = int(lon),
                     what = what)


    def _fill_files(self):
        for data in self._search_meta():
            self._known_files[data['file']] = data

        for data in self._search_cache():
            self._known_files[data['file']] = data


    def update_index(self, index):
        for _, data in tqdm(self._known_files.items(),
                            desc = "Building %s index" % self.path):
            index.insert(data = data)
        return index


    def _processing_path(self, path_fmt):
        NRW_TASKS = diskcache.Cache\
            (os.path.join(self.path,
                          "_NRW_LAZ_Processing_Tasks"))
        with diskcache.RLock(NRW_TASKS,
                             "lock: %s" % path_fmt):
            if "processing: %s" % path_fmt in NRW_TASKS:
                raise TASK_RUNNING()
            NRW_TASKS.set("processing: %s" % path_fmt, True,
                          expire=60*60)

        coord = self._cache.fn2coord(path_fmt)
        url = self._meta['root_url'] % coord

        task_las_processing.delay\
            (url = url,
             spath = self.path,
             dpath = path_fmt,
             resolution = self._meta['pdal_resolution'],
             whats = self._las_whats)


    def get_path(self, path):
        # if path is an unknown path, do nothing
        if path not in self._known_files:
            return path

        if path not in self._cache or not os.path.exists(path):
            path_fmt = self._cache.path2pathfmt(path)
            self._processing_path(path_fmt)
            for what in self._las_whats:
                self._cache.add(path_fmt % what)
            raise TASK_RUNNING()

        return path
