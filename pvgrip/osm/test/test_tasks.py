from unittest import TestCase
from pvgrip.osm.tasks import HistogrammCollector


class HistogrammCollector(TestCase):
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
        self.assertDictEqual(self.d.to_dict(),{"building": {"house": 6, "yes": 6}, "highway": {"yes": 2}})
