import json

from rtree import index
from shapely import geometry


class Polygon_File_Index:

    def __init__(self):
        """A spatial file index for polygon areas boosted by an R-Tree

        The index contains unique 'file' fields.

        """
        self._polygons = dict()
        self._rtree = index.Index()


    def _check_data(self, data):
        if 'file' not in data:
            raise RuntimeError("'file' not in data")
        if 'polygon' not in data:
            raise RuntimeError("'polygon' not in data")


    def insert(self, data):
        """Insert a polygon area to the index

        :data: a dictionary containing at least 'file' and 'polygon'
        fields. polygon is a list of tuple coordinates. A polygon
        definition is given here:
        https://shapely.readthedocs.io/en/latest/manual.html#polygons

        """
        self._check_data(data)

        if data['file'] in self._polygons:
            return

        pl = geometry.Polygon(data['polygon'])
        self._polygons[data['file']] = pl
        self._rtree.insert(hash(data['file']), pl.bounds, obj = data)


    def nearest(self, point):
        """Yield index data that contain a given point

        :point: a tuple with the same coordinate convention as used in
        .insert method for polygon tuples.
        A point definition is given here:
        https://shapely.readthedocs.io/en/latest/manual.html#points

        """
        for x in self._rtree.nearest(point, 1, objects='raw'):
            if self._polygons[x['file']]\
                   .intersects(geometry.Point(point)):
                yield x


    def save(self, fn):
        """Save index to a json file

        :fn: path to a file

        """
        data = []
        for x in self._rtree.intersection(self._rtree.bounds,
                                          objects = True):
            data += [{'id': x.id,
                      'bbox': x.bbox,
                      'object': x.object}]

        with open(fn, 'w') as f:
            json.dump(data, f)


    def load(self, fn):
        """Load index from a json file

        :fn: path to a file

        """
        with open(fn, 'r') as f:
            for x in json.load(f):
                try:
                    self._check_data(x['object'])
                except:
                    continue

                self._rtree.insert(x['id'], x['bbox'],
                                   obj = x['object'])
                self._polygons[x['object']['file']] = \
                    geometry.Polygon(x['object']['polygon'])
