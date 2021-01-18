import os
import json
import shutil

from .nrw_las import NRWData


def test_NRWData():
    meta = {
        "root_url": "https://www.opengeodata.nrw.de/produkte/geobasis/hm/3dm_l_las/3dm_l_las/3dm_32_%s_%s_1_nw.laz",
        "step": 1000,
        "box_resolution": 1,
        "epsg": 25832,
        "box_step": 1,
        "meta_entry_regex": "^3dm_32_(.*)_(.*)_1_nw.*$",
        "meta_url": "https://www.opengeodata.nrw.de/produkte/geobasis/hm/3dm_l_las/3dm_l_las/index.json",
        "pdal_resolution": 0.5,
    }
    path = 'test_NRWData'
    try:
        os.makedirs(path, exist_ok = True)
        with open(os.path.join(path, 'las_meta.json'),'w') as f:
            json.dump(meta, f)

        X = NRWData(path = path)
    finally:
        shutil.rmtree(path)
