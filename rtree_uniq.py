import rtree
import json

def save_index_json(index, fn):
    data = []
    for x in index.intersection(index.bounds, objects = True):
        data += [{'id': x.id,
                  'bbox': x.bbox,
                  'object': x.object}]

    with open(fn, 'w') as f:
        json.dump(data, f)


class SpatialFileIndex(rtree.index.Index):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._files = set()


    def insert(self, *args, **kwargs):
        if 'obj' not in kwargs or 'file' not in kwargs['obj']:
            raise RuntimeError\
                ("insert should be called"\
                 " with obj, that contains 'file'")

        fn = kwargs['obj']['file']

        if fn in self._files:
            return

        self._files.add(fn)
        super().insert(*args, **kwargs)
