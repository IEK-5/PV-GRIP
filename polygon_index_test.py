import os

from polygon_index import Polygon_File_Index


def test_polygon_index():
    x = Polygon_File_Index()

    x.insert(data = {'file':'one',
                     'polygon': [(0,0),(0,1),(1,1),(1,0)]})
    assert 1 == len(list(x.nearest((0.5,0.5))))

    x.insert(data={'file':'two',
                   'polygon': [(0,0),(0,1),(1,1),(1,0)]})
    assert 2 == len(list(x.nearest((0.5,0.5))))

    x.insert(data={'file':'three',
                   'polygon': [(0.1,0.1),(0.1,1),(1,1),(1,0.1)]})
    assert 3 == len(list(x.nearest((0.5,0.5))))

    x.insert(data={'file':'three',
                   'polygon': [(0.1,0.1),(0.1,0.9),
                               (0.9,0.9),(0.9,0.1)]})
    assert 3 == len(list(x.nearest((0.5,0.5))))

    x.insert(data={'file':'three',
                   'polygon': [(0.2,0.2),(0.2,0.9),
                               (0.9,0.9),(0.9,0.2)]})
    assert 3 == len(list(x.nearest((0.5,0.5))))


def test_polygon_index_save_load():
    x = Polygon_File_Index()

    x.insert(data = {'file':'one',
                     'polygon': [(0,0),(0,1),(1,1),(1,0)]})
    x.save('test_polygon_index.json')

    y = Polygon_File_Index()
    y.load('test_polygon_index.json')
    assert 1 == len(list(y.nearest((0.5,0.5))))
    os.remove('test_polygon_index.json')
