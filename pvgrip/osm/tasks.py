import os
import cv2
import json
import shutil
import pickle
import logging
import requests

import numpy as np

from collections import defaultdict, OrderedDict
from typing import List, Dict, Tuple, Any, TypeVar


from pvgrip.osm.utils \
    import form_query, is_file_valid_osm
from pvgrip.osm.rules \
    import TagValueHandler, add_color_to_histogramm, \
    create_rules_from_tags
from pvgrip.osm.overpass_error import OverpassAPIError

from pvgrip.raster.mesh \
    import mesh

from pvgrip \
    import CELERY_APP
from pvgrip.globals \
    import PVGRIP_CONFIGS
from pvgrip.utils.cache_fn_results \
    import cache_fn_results
from pvgrip.utils.celery_one_instance \
    import one_instance
from pvgrip.utils.basetask \
    import WithRetry

from pvgrip.utils.files \
    import get_tempfile, remove_file, get_tempdir
from pvgrip.utils.run_command \
    import run_command
from pvgrip.utils.format_dictionary \
    import format_dictionary


@CELERY_APP.task(bind=True, base=WithRetry)
@cache_fn_results(path_prefix = 'osm')
@one_instance(expire = 10)
def find_osm_data_online(self, bbox, tag, add_centers:bool = True):
    logging.debug("find_osm_data_online\n{}".format(format_dictionary(locals())))
    query = form_query(bbox, tag, add_centers)

    response = requests.get\
        (PVGRIP_CONFIGS['osm']['url'],
         params={'data':query},
         headers={'referer': PVGRIP_CONFIGS['osm']['referer']})

    ofn = get_tempfile()
    try:
        with open(ofn, 'w') as f:
            f.write(response.text)
    except Exception as e:
        remove_file(ofn)
        raise e
    if not is_file_valid_osm(ofn):
        with open(ofn, "r") as f:
            lines = []
            for i, l in enumerate(f):
                lines.append(l)
                if i>50:
                    break
        remove_file(ofn)
        raise OverpassAPIError("OverpassAPI response is not a valid osm file. Server might be busy", lines)
    return ofn


@CELERY_APP.task(bind=True, base=WithRetry)
@cache_fn_results(path_prefix = 'osm', minage = 1650884152)
@one_instance(expire = 10)
def readpng_asarray(self, png_fn, box, step, mesh_type):
    logging.debug("readpng_asarray\n{}"\
                  .format(format_dictionary(locals())))
    grid = mesh(box = box, step = step, mesh_type = mesh_type)

    ofn = get_tempfile()
    try:
        with open(ofn, "wb") as f:
            pickle.dump({"raster": \
                         cv2.imread(png_fn),
                         "mesh": grid}, f)
    except Exception as e:
        remove_file(ofn)
        raise e
    return ofn


@CELERY_APP.task(bind=True, base=WithRetry)
@cache_fn_results(path_prefix = 'osm')
@one_instance(expire = 10)
def merge_osm(self, osm_files):
    logging.debug("merge_osm\n{}"\
                  .format(format_dictionary(locals())))
    wdir = get_tempdir()
    ofn = get_tempfile()
    try:
        run_command\
            (what = ['osmconvert',
                     *osm_files,
                     '-o='+'output.osm'],
             cwd = wdir)
        os.rename(os.path.join(wdir,'output.osm'), ofn)
    finally:
        shutil.rmtree(wdir)

    return ofn


@CELERY_APP.task(bind=True, base=WithRetry)
@cache_fn_results(path_prefix = 'osm', minage = 1650884152)
@one_instance(expire = 10)
def render_osm_data(self, osm_fn, rules_fn, box, width, height):
    logging.debug("render_osm_data\n{}"\
                  .format(format_dictionary(locals())))

    # check if rules_fn == "NA" use default smrender rules:
    # /usr/local/share/smrender/rules.osm in docker
    if rules_fn is None or rules_fn == "NA":
        rules_fn = "/usr/local/share/smrender/rules_land.osm"
    if not os.path.isfile(rules_fn):
        raise RuntimeError(f"{rules_fn} is not a rulesfile in pvgrip")
    wdir = get_tempdir()
    ofn = get_tempfile()
    # -P specifies dimensions in mm
    # -d specifies density (points in inch)
    try:
        run_command\
            (what = \
             ['smrender',
              '-i', osm_fn,
              '-o', 'output.png',
              f"{str(box[0])}:{str(box[1])}:{str(box[2])}:{str(box[3])}",
              '-r', rules_fn,
              '-P','%.1fx%.1f' % (width/5, height/5),
              '-d','127',
              '-b','white'],
             cwd = wdir)
        os.rename(os.path.join(wdir,'output.png'), ofn)
    finally:
        shutil.rmtree(wdir)

    return ofn


@CELERY_APP.task(bind=True, base=WithRetry)
@cache_fn_results(path_prefix="osm")
@one_instance(expire=10)
def find_tags_in_osm(self, osmfile: str, tags: List[str]) -> str:
    """
    Extract all pairs of tag:value from a given osmfile if tag is in tags and count how often each tag value appears
    :param osmfile: path to osmfile
    :type osmfile: str
    :param tags: tags to extract, e.g. highway, building
    :type tags: list[str]
    :return: path to picklefile of a dict[str, dict[str, int]]
    :rtype: str
    """
    logging.debug("find_tags_in_osm\n{}".format(format_dictionary(locals())))

    wdir = get_tempdir()
    try:
        mapfile = f"{wdir}/map.osm"
        os.link(osmfile, f"{wdir}/map.osm")
        handler = TagValueHandler(tags)
        handler.apply_file(mapfile)
        res = handler.get_histogramm()
    finally:
        shutil.rmtree(wdir)

    ofn = get_tempfile()
    try:
        with open(ofn, "wb") as f:
            pickle.dump(res, f)
    except Exception as e:
        remove_file(ofn)
        raise e

    return ofn


class HistogrammCollector:
    """
    This class is a helper class for calculating a histogram of tags and values of an osm file
    """

    def __init__(self):
        self.default_dict = defaultdict(lambda : defaultdict(lambda: 0))


    def update(self, d:Dict[Any, Dict[str,int]]):
        """
        create new keys and append new values to existing lists
        :param d:
        :type d:
        :return:
        :rtype:
        """
        for tag, values in d.items():
            for value, occurrences in values.items():
                self.default_dict[tag][value] += occurrences

    def to_dict(self) -> Dict[str,Dict[str, int]]:
        """
        Return a Dict that maps tags to Dics that map values to occurences
        Returns: Dict[Dict[str, int]]

        """
        out = dict()
        for k, v in self.default_dict.items():
            out[k] = dict(v)
        return out


@CELERY_APP.task(bind=True, base=WithRetry)
@cache_fn_results(path_prefix="osm")
@one_instance(expire=10)
def collect_tags_from_osm(self, tag_dicts_paths: List[str]) -> str:
    """
    This function collects all tags and values from tag_dicts into one single dict and saves it on the disk
    :param tag_dicts: path to picked list[dict[str,dict[str,int]]]
    :type tag_dicts: str
    :return: path to json of Dict[str, Dict[str, int] mapping osm tag to mappings of osm values to occurrence
    :rtype: str
    """
    logging.debug(f"collect_tags_from_osm\n{format_dictionary(locals())}")

    res = HistogrammCollector()
    for fn in tag_dicts_paths:
        with open(fn, "rb") as f:
            d = pickle.load(f)
        res.update(d)

    res = res.to_dict()
    ofn = get_tempfile()
    try:
        with open(ofn, "w") as f:
            json.dump(res, f)
    except Exception as e:
        remove_file(ofn)
        raise e

    return ofn


@CELERY_APP.task(bind=True, base=WithRetry)
@cache_fn_results(path_prefix="osm", get_args_locally=False)
@one_instance(expire=10)
def map_raster_to_box(self, raster_fn:str, box:Tuple[float, float, float, float])->str:
    """
    This task maps the path of a raster rendered image to a box describing the position of the image
    this serves as an addapter-task for changing the output of osm_render
    :param self:
    :type self:
    :param raster_fn: path of rasterfile
    :type raster_fn: str
    :param box: lon_min, lat_min, lon_max, lat_max
    :type box:
    :return: path of jsonfile
    :rtype: str
    """

    ofn = get_tempfile()
    # write to json
    try:
        with open(ofn, "w") as file:
            json.dump({str(box): raster_fn},file)
        return ofn
    except Exception as e:
        logging.debug(e)
        remove_file(ofn)
        raise e


@CELERY_APP.task(bind=True, base=WithRetry)
@cache_fn_results(path_prefix="osm")
@one_instance(expire=10)
def collect_json_dicts(self, json_fns:List[str]) -> str:
    """
    collect multiple json dicts and merge them into one big json dict
    usually json_fns is a list of files produced by map_raster_to_box
    :param self:
    :type self:
    :param json_fns:
    :type json_fns:
    :return:
    :rtype:
    """
    out = dict()
    for json_fn in json_fns:
        with open(json_fn, "r") as f:
            out.update(json.load(f))

    ofn = get_tempfile()
    with open(ofn, "w") as f:
        json.dump(out, f)
    return ofn


@CELERY_APP.task(bind=True, base=WithRetry)
@cache_fn_results(path_prefix="osm")
@one_instance(expire=10)
def merge_and_order_osm_rules(self, json_dicts: List[str], order_by:List[str]) -> str:
    """
    This function merges dictionaries of dictionaries used for generating smrender rules.
    It collects dictionaries, adds random colors to them and then orders the entries by the osm tags in "order_by"
    This function is intented to be used as a merge task.
    Args:
        self (): used for celery only
        json_dicts (List[str]):
        list of paths to json files cointaining Dict[str, Dict[str, int]]
        that is a mapping osm tag to mappings of osm value to occurrence
        order_by (List[str]): list of osm tags to order the entried of the dict by

    Returns:
        path to jsonfile containing a Dict[str, Dict[str, Tuple[int, str]]] which is
        a mapping osm tag to mappings of osm value to tuple of occurrence and hexcolor string
    """
    collector = HistogrammCollector()
    for json_dict_path in json_dicts:
        with open(json_dict_path, "r") as f:
            json_dict = json.load(f)
        collector.update(json_dict)
    output_dict = collector.to_dict()
    output_dict = add_color_to_histogramm(output_dict)
    output_dict_ordered = order_dict_by_list(output_dict, order_by)
    ofn = get_tempfile()
    try:
        with open(ofn, "w") as f:
            json.dump(output_dict_ordered, f)
    except Exception as e:
        remove_file(ofn)
        raise e

    return ofn

def order_dict_by_list(to_order:Dict, order_by:List) -> List[Tuple[Any, Any]]:
    """
        Read the contents of "json_dict_path" to a dict and order the keys in the dict according to the keys in "keys".
        The rest is put back in the same order.

        Example:
            to_order = {1:"1", "3":3, "2":"2", "foo":"foo", "bar": "bar"}, keys = [1, foo, bar]
            result {1:"1", "foo":"foo", "bar": "bar", "3":3, "2":"2"}

        args:
            to_order (Dict[K, Any]): python dict
            keys (List[K]): list of keys used to order the dict

        return: path to a file with the new json dict in the form of a list of keys and values
        """
    old_keys = list(order_by)

    out = OrderedDict()
    for k in order_by:
        if k in old_keys and k in to_order.keys():
            out[k] = to_order.pop(k)

    out.update(to_order)
    out = [(k, v) for (k, v) in out.items()]
    return out

