import os
from collections import defaultdict
from xml.etree import ElementTree
import tempfile
from typing import List, Dict, Tuple, Union
import numpy as np
import colorsys


import osmium.osm

from osmium import SimpleHandler

from pvgrip.utils.cache_fn_results import cache_fn_results
from pvgrip.utils.files import get_tempfile


def add_color_to_histogramm(hist: Dict[str, Dict[str, int]]) -> Dict[str, Dict[str, Tuple[int, str]]]:
    """
    This function adds a distinct random color to each unique tag:value pair
    Args:
        hist: Dict[str, Dict[str, int]], Osm Histogram so a map of tags to a map of values to occurrence

    Returns:
        Dict[str, Dict[str, Tuple[int, str]]] an Osm Histogram which in addition to the occurence also contains a hexcolor string
    """
    n = sum([len(v.keys()) for v in hist.values()])
    colors = (c for c in random_colors(n))
    out = {tag: {value: (occurrence, next(colors)) for value, occurrence in tag_dict.items()} for tag, tag_dict in
           hist.items()}
    return out


def random_colors(num_colors: int, bright=True, seed: int = 17) -> List[str]:
    """
    Generate num_colors distinct hex colors
    """

    brightness = 1.0 if bright else 0.7
    hsv = [(i / num_colors, 1, brightness) for i in range(num_colors)]
    rng = np.random.default_rng(seed=seed)
    offset = rng.uniform(0, 1)
    hsv = [(h + offset, s, v) for (h, s, v) in hsv]
    colors = list(map(lambda c: colorsys.hsv_to_rgb(*c), hsv))
    colors = ["".join("%02X" % round(i * 255) for i in rgb) for rgb in colors]
    colors = [f"#{c.lower()}" for c in colors]
    np.random.shuffle(colors)
    return colors


class TagValueHandler(SimpleHandler):
    """
    This helper class collects all values appearing in an osm file
    given a list of tags and counts how often each tag:value pair appears
    """

    _values: defaultdict


    @property
    def values(self):
        """
        Getter function to return _values as a python dict
        Returns:

        """
        return self.get_histogramm()


    def __init__(self, tags: List[str]):
        """

        :param tags: list of osm tags like building, natural, highway
        :type tags: list[str]
        """
        super(TagValueHandler, self).__init__()
        self.tags = tags
        self._values = defaultdict(lambda: defaultdict(lambda: 0))


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

    def add_values(self, osm_thing: Union[osmium.osm.Way, osmium.osm.Relation, osmium.osm.Node]):
        for k, v in dict(osm_thing.tags).items():
            if k in self.tags:
                self._values[k][v] += 1

    def way(self, w: osmium.osm.Way):
        self.add_values(w)

    def relation(self, r: osmium.osm.Way):
        self.add_values(r)




@cache_fn_results()
def create_rules_from_tags(tags_hist_with_colors: Dict[str, Dict[str, Tuple[int, str]]]) -> str:
    """
    Create a rulefile to draw pairs of tags and values in a certain color perhaps changed based on their occureence
    Args:
        tags (Dict[str, Dict[str, Tuple[int, str]]]): Dict of Dicts {tag:{value:(number of occurences, hex rgb color}}

    Returns:

    """
    return _create_rules_from_tags(tags_hist_with_colors)

def _create_rules_from_tags(tags_hist_with_colors: Dict[str, Dict[str, Tuple[int, str]]]) -> str:
    """
    Create a rulefile to draw pairs of tags and values in a certain color perhaps changed based on their occureence
    Args:
        tags (Dict[str, Dict[str, Tuple[int, str]]]): Dict of Dicts {tag:{value:(number of occurences, hex rgb color}}

    Returns:

    """
    root = ElementTree.Element("osm")

    tree = ElementTree.ElementTree(root)

    for i, (tag, value_dict) in enumerate(tags_hist_with_colors.items()):
        for value, (occurences, col) in value_dict.items():
            for osm_type in ["way", "relation"]:
                osm_type = ElementTree.Element(osm_type)
                # smrender documentation states ids are automatically given by the order of which the rule is in the file
                # to we omit the id and only specificy the version
                osm_type.set("version", f"{i}")
                root.append(osm_type)

                way_tag_name = ElementTree.Element("tag")
                osm_type.append(way_tag_name)

                way_tag_name.set("k", tag)
                way_tag_name.set("v", value)

                way_tag_action = ElementTree.Element("tag")
                osm_type.append(way_tag_action)

                way_tag_action.set("k", "_action_")
                way_tag_action.set("v", f"draw:color={col}")

    for i, (tag, value_dict) in enumerate(tags_hist_with_colors.items()):
        relation_type = ElementTree.Element("relation")
        relation_type.set("version", f"{i+len(tags_hist_with_colors)}")
        root.append(relation_type)

        relation_tag_name = ElementTree.Element("tag")
        relation_type.append(relation_tag_name)
        relation_tag_name.set("k", tag)
        relation_tag_name.set("v", "")

        relation_tag_type = ElementTree.Element("tag")
        relation_type.append(relation_tag_type)
        relation_tag_type.set("k", "type")
        relation_tag_type.set("v", "multipolygon")

        relation_tag_action = ElementTree.Element("tag")
        relation_type.append(relation_tag_action)
        relation_tag_action.set("k", "_action_")
        relation_tag_action.set("v", "cat_poly")

    ofn = get_tempfile()
    try:
        with open(ofn, "wb") as f:
            tree.write(f)
    except Exception as e:
        os.remove(ofn)
        raise e

    return ofn
