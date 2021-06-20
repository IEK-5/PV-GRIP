import re
import geohash

import xml.etree.ElementTree as etree

from cassandra_io.utils \
    import bbox2hash

from pvgrip.globals \
    import PVGRIP_CONFIGS

from pvgrip.utils.cache_fn_results \
    import cache_fn_results
from pvgrip.utils.files \
    import get_tempfile, remove_file


def get_box_list(box):
    hash_length = int(PVGRIP_CONFIGS['osm']['hash_length'])
    f = (geohash.bbox(i) \
         for i in bbox2hash(box, hash_length))
    return [(x['s'],x['w'],x['n'],x['e']) for x in f]


def form_query(bbox, tag):
    bbox = tuple(bbox)
    query_tags = ""
    if tag:
        query_tags = query_tags + \
            f"""node{str(bbox)};
            way[{str(tag)}]{str(bbox)};
            relation[{str(tag)}]{str(bbox)};"""
    else:
        query_tags = f"""node{str(bbox)};
                         way{str(bbox)};
                         relation{str(bbox)};"""
    return f"""
    [out:xml];
    (
    {query_tags}

    );
    out center;
    """


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
