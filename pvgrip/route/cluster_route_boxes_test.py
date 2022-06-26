import pickle

import numpy as np
import pandas as pd

from pvgrip.route.cluster_route_boxes \
    import get_list_rasters

from pvgrip.utils.files \
    import get_tempfile, remove_file


def _dummy_route(N, dummy):
    data = np.random.random((N,4))
    data = dummy[0] + data*(dummy[1] - dummy[0])
    return pd.DataFrame(data,
                        columns=['longitude','latitude',
                                 'other1','other2'])


def do_get_list_rasters(N=100, dummy = (1,60), box=30, box_delta=2):
    route = _dummy_route(N = N, dummy = dummy)
    box = (-box,)*2 + (box,)*2

    route_fn = get_tempfile()
    try:
        route.to_csv(route_fn, sep = '\t', index = False)
        rasters_fn = get_list_rasters(route_fn = route_fn,
                                      box = box,
                                      box_delta = box_delta)

        with open(rasters_fn, 'rb') as f:
            rasters = pickle.load(f)

        assert N == sum([len(x['route']) for x in rasters])
        return rasters
    finally:
        remove_file(route_fn)


def test_get_list_rasters():
    assert 1 == len(do_get_list_rasters(N=1, box_delta = 1))

    do_get_list_rasters(N=1000)
    do_get_list_rasters(N=1)
    do_get_list_rasters(N=2)
    do_get_list_rasters(N=3)
    do_get_list_rasters(box=1000)
    do_get_list_rasters(box=1)
    do_get_list_rasters(box=1e-5)
    do_get_list_rasters(box_delta = 1e-5)
    do_get_list_rasters(box_delta = 1)
    do_get_list_rasters(box_delta = 1e+2)
