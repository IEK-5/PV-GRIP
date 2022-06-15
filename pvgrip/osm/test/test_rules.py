import os
from typing import List, Set, Dict, Tuple
# from importlib import resources
import osmium
from pvgrip.utils import git
from pvgrip.osm.rules import _create_rules_from_tags, TagValueHandler
from unittest import TestCase, main


class Building_Handler(osmium.SimpleHandler):
    values: Set[str]

    def __init__(self):
        super().__init__()
        self.values = set([])

    def area(self, a: osmium.osm.Area):
        if "building" in dict(a.tags).keys():
            for tag, value in dict(a.tags).items():
                if "building" == tag:
                    self.values.add(value)


def xml_from_tags_hist(tags_hist: Dict[str, Dict[str, Tuple[int, str]]]):
    """
    create a string that is an smrender rules xml from
    Args:
        tags_hist:

    Returns:

    """
    rules = _create_rules_from_tags(tags_hist)
    xml_string = ""
    with open(rules, "r") as f:
        for line in f:
            xml_string += line
    os.remove(rules)
    return xml_string


class TestRules(TestCase):

    @staticmethod
    def get_testfile_path():
        """
        Return the path of the .osm file used for testing
        :return:
        :rtype:
        """
        root = git.git_root()
        testfile = os.path.join(root, "pvgrip/osm/test/test.osm")
        return testfile

    def test_TagValueHandler(self):
        handler = TagValueHandler(tags=["building"])
        testfile = self.get_testfile_path()
        handler.apply_file(testfile)

        test_handler = Building_Handler()
        test_handler.apply_file(testfile)
        self.assertSetEqual(test_handler.values, set(handler.values["building"].keys()))

        hist = handler.get_histogramm()
        expected = {"house": 43, "garages": 1, "garage": 1, "apartments": 2, "yes": 5}
        self.assertDictEqual(hist, {"building": expected})

    def test_create_rules_from_tags(self):
        #  [("building", "garage", "ff0000"), ("building", "yes", "00ff00")]
        tags_hist = {"building": {"garage": (12, "ff0000"), "yes": (44, "00ff00")}}
        xml_string = xml_from_tags_hist(tags_hist)
        expected = '<osm><way><tag k="building" v="garage" /><tag k="_action_" v="draw:color=ff0000;bcolor=black" /></way><way><tag k="building" v="yes" /><tag k="_action_" v="draw:color=00ff00;bcolor=black" /></way></osm>'

        assert (
                xml_string
                == expected
        )

        tags_hist = {}
        xml_string = xml_from_tags_hist(tags_hist)
        expected = '<osm />'
        assert (
                xml_string
                == expected
        )

        tags_hist = {"building":{"":(100, "ff0000")}}
        xml_string = xml_from_tags_hist(tags_hist)
        expected = '<osm><way><tag k="building" v="" /><tag k="_action_" v="draw:color=ff0000;bcolor=black" /></way></osm>'
        assert (
                xml_string
                == expected
        )



if __name__ == "__main__":
    main()
