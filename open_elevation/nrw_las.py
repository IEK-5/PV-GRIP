import re
import os
import csv
import time
import json
import pyproj
import requests
import itertools

from tqdm import tqdm

import open_elevation.celery_tasks.app \
    as app

import open_elevation.utils as utils


class NRWData:
    """Get and process NRW data

    """

    def __init__(self, path, update_index = 1):
        """

        :path: where remote_meta.json file is located.

        remote_meta.json should contain fields: 'root_url' (format string), 'step' the integer is multiplied to get coordinate value, 'resolution' resolution to use in sampling the lidar data, 'epsg' coordinate system, 'box_step' size of each rectangle data, 'fn_meta' path to the meta csv file, 'meta_entry_regex' regex how to match coordinates

        the data is stored in the subdirectory 'cache'

        :update_index: number of days between index update

        """
        self.path = path
        self._update_index = update_index*24*60*60

        self._meta = self._read_meta()
        self._if_compute_las = \
            'yes' == self._meta['if_compute_las']

        if self._if_compute_las:
            self._las_whats = self._meta['las_stats']
            self._pdal_resolution = \
                self._meta['pdal_resolution']
        else:
            self._las_whats = ('',)
            self._pdal_resolution = 0

        self._proj_from = pyproj\
            .Transformer.from_crs(self._meta['epsg'], 4326,
                                  always_xy=True)

        self._files = dict()
        for data in tqdm(self._search_meta(),
                         desc = "Reading %s index" % \
                         self.path):
            self._files[data['file']] = data


    def _read_meta(self):
        with open(os.path.join\
                  (self.path,'remote_meta.json'),'r') as f:
            return json.load(f)


    def _coord_to_index_data(self, lat, lon, what):
        res = {}
        step = self._meta['step']
        box_step = self._meta['box_step']
        resolution = self._meta['box_resolution']

        key = ("nrw_las", self.path, lat, lon, what)
        res['file'] = app.RESULTS_CACHE.get(key, check = False)

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


    def _download_index(self, ofn):
        url = self._meta['meta_url']
        res = requests.get(url, allow_redirects = True)

        if 200 != res.status_code:
            raise RuntimeError("""
            cannot download index data!
            status_code: %d
            url: %s
            """ % (res.status_code, url))
        index = json.loads(res.text)

        with open(ofn, 'w') as f:
            json.dump(index, f)

        return index


    def _load_index(self):
        fn = os.path.join(self.path, 'meta_info.json')

        try:
            if time.time() - os.stat(fn).st_mtime \
               < self._update_index:
                with open(fn, 'r') as f:
                    return json.load(f)
            else:
                return self._download_index(fn)
        except:
            return self._download_index(fn)


    def _search_meta(self):
        regex = re.compile(self._meta['meta_entry_regex'])
        index = self._load_index()['datasets'][0]['files']

        for item in index:
            fn = item['name']
            if not regex.match(fn):
                continue

            lon = int(regex.sub(r'\1', fn))
            lat = int(regex.sub(r'\2', fn))
            for what in self._las_whats:
                data = self._coord_to_index_data\
                    (lat=lat, lon=lon, what = what)
                data.update({
                    'url': self._meta['root_url'] \
                    % (lon, lat),
                    'stat': what,
                    'remote_meta': self.path,
                    'if_compute_las': \
                    self._if_compute_las,
                    'pdal_resolution': \
                    self._pdal_resolution})
                yield data


    def update_index(self, index):
        for _, data in tqdm(self._files.items(),
                            desc = "Building %s index" % \
                            self.path):
            index.insert(data = data)
        return index
