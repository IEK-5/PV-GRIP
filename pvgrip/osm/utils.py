import logging
import pickle
import re
import geohash
from osmium import SimpleHandler
import xml.etree.ElementTree as etree
import shutil
import os

from cassandra_io.utils \
    import bbox2hash

from pvgrip.globals \
    import PVGRIP_CONFIGS

from pvgrip.utils.cache_fn_results \
    import cache_fn_results
from pvgrip.utils.files \
    import get_tempfile, remove_file, get_tempdir


def get_box_list(box):
    hash_length = int(PVGRIP_CONFIGS['osm']['hash_length'])
    f = (geohash.bbox(i) \
         for i in bbox2hash(box, hash_length))
    return [(x['s'],x['w'],x['n'],x['e']) for x in f]


def form_query(bbox, tag, add_center: bool = True) -> str:
    """
    this function creates a query for overpassapi
    :param bbox: bounding box in lat long with form [lat_min, lon_min, lat_max, lon_max]
    :type bbox: Tuple[float, flaot, float, float]
    :param tag: osm tag
    :type tag: str
    :param add_center: flag if True centroids will be added for each way
    :type add_center: bool
    :return: query
    :rtype: str
    """
    bbox = tuple(bbox)
    query_tags = ""
    if tag:
        query_tags = (
            query_tags
            + f"""node{str(bbox)};
            way[{str(tag)}]{str(bbox)};
            relation[{str(tag)}]{str(bbox)};"""
        )
    else:
        query_tags = f"""node{str(bbox)};
                         way{str(bbox)};
                         relation{str(bbox)};"""
    out =  f"""
    [out:xml];
    (
    {query_tags}
    );
    out {'center' if add_center else ''};
    """
    return out


@cache_fn_results()
def create_rules(tag):
    root = etree.Element('osm')

    tree = etree.ElementTree(root)

    type_ = etree.Element('way')

    root.append(type_)

    tag_name = etree.Element('tag')
    type_.append(tag_name)

    match = re.match(r'(.*)=(.*)', tag)
    if match:
        key, value = match.groups()
    else:
        key = tag
        value = ''

    tag_name.set('k',key)
    tag_name.set('v',value)

    tag_action = etree.Element('tag')
    type_.append(tag_action)

    tag_action.set('k','_action_')
    tag_action.set('v','draw:color=white;bcolor=white')

    ofn = get_tempfile()
    try:
        with open(ofn, 'wb') as f:
            tree.write(f)
    except Exception as e:
        remove_file(ofn)
        raise e

    return ofn


def get_rules_from_pickle(
    rules_dict_pickle: str,
) -> str:
    """
    get the path of the rulesfrome from the pickled dict created by tag_dicts_to_rules
    :param rules_dict_pickle: path to pickled dict
    :type rules_dict_pickle: str
    :return: path of rulesfile
    :rtype: str
    """
    try:
        with open(rules_dict_pickle, "rb") as file:
            out = pickle.load(file)
            return out["rules"]
    except Exception as e:
        logging.error(e)
        raise e

def is_file_valid_osm(filepath: str) -> bool:
    """
    test if filepath is a valid osm file

    :param filepath: path to a file
    :type fielpath: str
    :return: boolean flag true means file is osm file false means it's not
    :rtype: bool
    """
    wdir = get_tempdir()
    mapfile = os.path.join(wdir, "map.osm")
    os.link(os.path.abspath(filepath), mapfile)

    # this is maybe a bit dirty but it get's the job done
    class DummyHandler(SimpleHandler):
        
        def __init__(self, *args, **kwargs):
            super().__init__(*args, **kwargs)
        
        def way(self, w):
            pass
    
    d = DummyHandler()
    out = True
    try:
        d.apply_file(mapfile)
    except RuntimeError as r:
        out = False
    finally:
        shutil.rmtree(wdir)
    return out
