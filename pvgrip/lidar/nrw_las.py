import re
import os
import time
import json
import pyproj
import requests

from pvgrip.utils.float_hash \
    import float_hash


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

        self._proj_from = pyproj\
            .Transformer.from_crs(self._meta['epsg'], 4326,
                                  always_xy=True)


    def _read_meta(self):
        with open(os.path.join\
                  (self.path,'remote_meta.json'),'r') as f:
            return json.load(f)


    def _coord_to_index_data(self, lat, lon):
        res = {}
        step = self._meta['step']
        box_step = self._meta['box_step']
        resolution = self._meta['box_resolution']

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


    def search_meta(self):
        regex = re.compile(self._meta['meta_entry_regex'])
        index = self._load_index()['datasets'][0]['files']

        for item in index:
            fn = item['name']
            if not regex.match(fn):
                continue

            lon = int(regex.sub(r'\1', fn))
            lat = int(regex.sub(r'\2', fn))
            url = self._meta['root_url'] % (lon, lat)

            data = self._coord_to_index_data(lat=lat, lon=lon)
            data.update({
                'file': os.path.join(self.path,'data',
                                     float_hash(("nrw_las", url, lon, lat))),
                'url': url,
                'remote_meta': self.path,
                'if_compute_las': self._if_compute_las})
            yield data
