import unittest
import json

from pvgrip.osm.tasks import HistogrammCollector, order_dict_by_list
from pvgrip.utils.files import get_tempfile

class HistogrammCollectorTestCase(unittest.TestCase):
    def setUp(self) -> None:
        self.dicts = [
            {"building": {"house": 3, "yes": 2}},
            {"building": {"house": 3, "yes": 4}, "highway": {"yes": 2}},
        ]
        self.d = HistogrammCollector()

    def test_dict_list(self):
        self.assertDictEqual(self.d.to_dict(), dict())

        self.d.update(self.dicts[0])
        self.assertDictEqual(self.d.to_dict(), self.dicts[0])
        self.d.update(self.dicts[1])
        self.assertDictEqual(self.d.to_dict(), {"building": {"house": 6, "yes": 6}, "highway": {"yes": 2}})



class TestOrderDictByKeys(unittest.TestCase):

    def test_order_dict_by_list(self):
        d = {str(i): i for i in reversed(range(5))}
        d.update({str(i): i for i in range(5, 9)})

        keys = [str(i) for i in range(5)]
        ordered_d = dict(order_dict_by_list(d, keys))

        expected = {str(i): i for i in range(9)}
        self.assertListEqual(list(ordered_d), list(expected))
        self.assertDictEqual(ordered_d, expected)

if __name__ == "__main__":
    unittest.main()