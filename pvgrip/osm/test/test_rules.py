import os.path
from typing import List, Set
from importlib import resources
import osmium
from pvgrip.utils import git
from pvgrip.osm.rules import TagRuleContainer, _create_rules_from_tags, TagValueHandler


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

def get_testfile_path():
    """
    Return the path of the .osm file used for testing
    :return:
    :rtype:
    """
    root = git.git_root()
    testfile = os.path.join(root, "pvgrip/osm/test/test.osm")
    return testfile


def test_TagValueHandler():
    handler = TagValueHandler(tags=["building"])
    testfile = get_testfile_path()
    handler.apply_file(testfile)

    test_handler = Building_Handler()
    test_handler.apply_file(testfile)
    assert test_handler.values == handler.get_values()["building"]

    hist = handler.get_histogramm()
    expected = {"house": 43, "garages": 1, "garage": 1, "apartments": 2, "yes": 5}
    assert hist["building"] == expected


def test_TagRuleContainer():
    test_handler = Building_Handler()
    testfile = get_testfile_path()
    test_handler.apply_file(testfile)
    container = TagRuleContainer({"building": list(test_handler.values)})
    colors = (i for i in container.random_colors())
    mapping = container.get_mapping()

    for (tag, value), color in mapping.items():
        assert value in test_handler.values
        c = next(colors)
        assert color == c

    rev_mapping = container.get_reverse_mapping()
    colors = (i for i in container.random_colors())
    for color, (tag, value) in rev_mapping.items():
        assert value in test_handler.values
        c = next(colors)
        assert color == c


def test_create_rules_from_tags():
    rules = _create_rules_from_tags(
        [("building", "garage", "ff0000"), ("building", "yes", "00ff00")]
    )
    xml_string = ""
    with open(rules, "r") as f:
        for line in f:
            xml_string += line

    assert (
        xml_string
        == '<osm><way><tag k="building" v="garage" /><tag k="_action_" v="draw:color=ff0000;bcolor=black" /></way><way><tag k="building" v="yes" /><tag k="_action_" v="draw:color=00ff00;bcolor=black" /></way></osm>'
    )


def main():
    test_TagValueHandler()
    test_TagRuleContainer()
    test_create_rules_from_tags()


if __name__ == "__main__":
    main()
