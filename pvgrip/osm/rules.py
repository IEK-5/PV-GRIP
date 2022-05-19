import os
from collections import defaultdict
from xml.etree import ElementTree
import tempfile
from typing import List, Dict, Tuple
import numpy as np
import colorsys

import osmium.osm

from osmium import SimpleHandler

from pvgrip.utils.cache_fn_results import cache_fn_results
from pvgrip.utils.files import get_tempfile


class TagRuleContainer:
    _tags_values: Dict[str, List[str]]

    def random_colors(self, bright=True, seed: int = 17) -> List[str]:
        """
        Generate random colors one for each unique tag value pair.
        To get visually distinct colors, generate them in HSV space then
        convert to RGB.
        """
        N = 0
        for k, v in self._tags_values.items():
            N += len(v)
        brightness = 1.0 if bright else 0.7
        hsv = [(i / N, 1, brightness) for i in range(N)]
        rng = np.random.default_rng(seed=seed)
        offset = rng.uniform(0, 1)
        hsv = [(h + offset, s, v) for (h, s, v) in hsv]
        colors = list(map(lambda c: colorsys.hsv_to_rgb(*c), hsv))
        colors = ["".join("%02X" % round(i * 255) for i in rgb) for rgb in colors]
        colors = [f"#{c.lower()}" for c in colors]
        return colors

    def __iter__(self):
        N = 0
        for k, v in self._tags_values.items():
            N += len(v)
        colors = (i for i in self.random_colors())

        for tag, values in self._tags_values.items():
            for v in values:
                c = next(colors)
                yield tag, v, c

    def __init__(self, tags_values: Dict[str, List[str]] = None):
        if tags_values is None:
            self._tags_values = defaultdict(lambda x: [])
        else:
            self._tags_values = tags_values

    def add_pair(self, tag: str, value: str):
        self._tags_values[tag].append(value)

    def get_mapping(self) -> Dict[Tuple[str, str], str]:
        """
        Return a dict that maps each unique pair of tag and value to a unique colour
        :return:
        :rtype:
        """
        colors = (i for i in self.random_colors())
        out = dict()
        for tag, values in self._tags_values.items():
            for value in values:
                c = next(colors)
                out[(tag, value)] = c
        return out

    def get_reverse_mapping(self) -> Dict[str, Tuple[str, str]]:
        """
        Return a dict that maps a colour to each unique pair of tag and value
        :return:
        :rtype:
        """
        mapping = self.get_mapping()
        out = dict()
        for k, v in mapping.items():
            out[v] = k
        return out

    @classmethod
    def from_osm_by_tag(cls, osmfile: str, tags: List[str]) -> "TagRuleContainer":
        """
        Create a TagRuleContainer which contains a unique color for each unique value for each provided Tag
        Returns:
            TagRuleContainer
        """

        handler = TagValueHandler(tags)
        handler.apply_file(osmfile)
        out = TagRuleContainer(handler._values)

        return out

    def __repr__(self):
        return str(list(iter(self)))


class TagValueHandler(SimpleHandler):
    """
    This helper class collects all values appearing in an osm file
    given a list of tags and counts how often each tag:value pair appears
    """

    _values: defaultdict

    def __init__(self, tags: List[str]):
        """

        :param tags: list of osm tags like building, natural, highway
        :type tags: list[str]
        """
        super(TagValueHandler, self).__init__()
        self.tags = tags
        self._values = defaultdict(lambda: defaultdict(lambda: 0))

    def get_values(self):
        """
        Return a copy of the result dict
        :return:
        :rtype:
        """
        out = dict()
        for tag, values in self._values.items():
            out[tag] = set(values.keys())
        return out

    def get_histogramm(self):
        """
        Return a dict that maps tags to dicts that map keys to the number of their occurrence
        :return:
        :rtype:
        """
        out = dict()
        for tag, values in self._values.items():
            out[tag] = dict(values)
        return out

    def way(self, w: osmium.osm.Way):
        for k, v in dict(w.tags).items():
            if k in self.tags:
                self._values[k][v] += 1

    # areas are closed ways
    # so each area is also a way
    # this causes the handler to view objects twice
    # since we want highways and buildings we only look at ways

    # def area(self, a: osmium.osm.Area):
    #     for k, v in dict(a.tags).items():
    #         if k in self.tags:
    #             self._values[k][v] += 1



@cache_fn_results()
def create_rules_from_tags(tags: List[Tuple[str, str, str]]) -> str:
    """
        Create a rulefile to draw all ways of a certain tag in white
        Args:
            tags (list[tuple[str, str, str]]): list of tuples of tag, value, hexcode of color

        Returns:

    """
    return _create_rules_from_tags(tags)


def _create_rules_from_tags(tags: List[Tuple[str, str, str]]) -> str:
    """
    Create a rulefile to draw all ways of a certain tag in white
    Args:
        tags (list[tuple[str, str, str]]): list of tuples of tag, value, hexcode of color

    Returns:

    """
    root = ElementTree.Element("osm")

    tree = ElementTree.ElementTree(root)

    for tag, value, col in tags:
        type_ = ElementTree.Element("way")

        root.append(type_)

        tag_name = ElementTree.Element("tag")
        type_.append(tag_name)

        tag_name.set("k", tag)
        tag_name.set("v", value)

        tag_action = ElementTree.Element("tag")
        type_.append(tag_action)

        tag_action.set("k", "_action_")
        tag_action.set("v", f"draw:color={col};bcolor=black")

    ofn = get_tempfile()
    try:
        with open(ofn, "wb") as f:
            tree.write(f)
    except Exception as e:
        os.remove(ofn)
        raise e

    return ofn
