from rtree_uniq import SpatialFileIndex


def test_SpatialFileIndex():
    x = SpatialFileIndex()

    x.insert(0, (0,0,.1,.1), obj={'file':'one'})
    assert 1 == x.get_size()

    x.insert(0, (0,0,.1,.1), obj={'file':'two'})
    assert 2 == x.get_size()

    x.insert(0, (.1,0,.2,.1), obj={'file':'three'})
    assert 3 == x.get_size()

    x.insert(0, (.1,.1,.2,.2), obj={'file':'three'})
    assert 3 == x.get_size()

    x.insert(0, (.1,.1,.2,.2), obj={'file':'three'})
    assert 3 == x.get_size()
