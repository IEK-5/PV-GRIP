import numpy as np
import pandas as pd

from open_elevation.route import get_list_rasters


def _dummy_route(N, dummy):
    data = np.random.random((N,4))
    data = dummy[0] + data*(dummy[1] - dummy[0])
    return pd.DataFrame(data,
                        columns=['longitude','latitude',
                                 'other1','other2'])


def do_get_list_rasters(N=100, dummy = (0,100), box=3, box_delta=2):
    route = _dummy_route(N = N, dummy = dummy)
    box = (-box,)*2 + (box,)*2

    rasters = get_list_rasters(route = route, box = box,
                               box_delta = box_delta)

    assert N == sum([len(x['route']) for x in rasters])
    return rasters


def test_get_list_rasters():
    assert 1 == len(do_get_list_rasters(N=1, box_delta = 1))
    assert 100 == len(do_get_list_rasters(N=100, box_delta = 1))
    assert 100 == len(do_get_list_rasters(N=100, dummy=(0,1), box=1e-5, box_delta=1))

    do_get_list_rasters(N=1000)
    do_get_list_rasters(N=1)
    do_get_list_rasters(box=1, box_delta=2)
    do_get_list_rasters(box=1e-5, box_delta=2)
    do_get_list_rasters(dummy=(0,1e-6), box=1e-5, box_delta=2)
    do_get_list_rasters(dummy=(0,1e-6), box=1, box_delta=2)
    do_get_list_rasters(dummy=(0,1e-6), box=100, box_delta=2)
