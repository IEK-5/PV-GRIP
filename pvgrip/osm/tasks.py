import os
import cv2
import json
import shutil
import pickle
import logging
import requests

import numpy as np

from collections import defaultdict
from typing import List, Dict, Tuple, Any


from pvgrip.osm.utils \
    import form_query
from pvgrip.osm.rules \
    import TagValueHandler, TagRuleContainer, \
    create_rules_from_tags

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
def render_osm_data(self, osm_fn, rules_fn, box, width):
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
              '-P','%.1fx0' % (width/5),
              '-d','127',
              '-b','black'],
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


class _dict_list:
    """
    This class is a helper class for calculating a histogram of tags and values of an osm file
    """
    # dict_list should not contain strange stuff to pickle

    def __init__(self):
        self.default_dict = defaultdict(lambda : defaultdict(lambda: 0))

    # def _default_val(self):
    #     return defaultdict(self._inner_default_val)
    #
    # def _inner_default_val(self):
    #     # this class needs to be pickleable therefore defaultfunctions must be defined toplevel and can not be lambdas
    #     return 0
    #

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

    def to_dict(self):
        # return normal dictionary
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
    :return: path to pickled dict
    :rtype: str
    """
    # todo change input to list of pickled files
    logging.debug(f"collect_tags_from_osm\n{format_dictionary(locals())}")

    res = _dict_list()
    for fn in tag_dicts_paths:
        with open(fn, "rb") as f:
            d = pickle.load(f)
        res.update(d)

    res = res.to_dict()
    ofn = get_tempfile()
    try:
        with open(ofn, "wb") as f:
            pickle.dump(res, f)
    except Exception as e:
        remove_file(ofn)
        raise e

    return ofn

    # with open(tag_dicts_path, "rb") as file:
    #     try:
    #         tag_dicts = pickle.load(file)
    #     except Exception as e:
    #         logging.error(e)
    #         raise e

    # def default_val():
    #     return []

    # res_dir = defaultdict(lambda: [])
    # for tag_dict in tag_dicts:
    #     for k, v in tag_dict:
    #         for value in v:
    #             res_dir[k].append(value)  # bug?

    # ofn = get_tempfile()
    # try:
    #     with open(ofn, "wb") as f:
    #         pickle.dump(res_dir, f)
    # except Exception as e:
    #     remove_file(ofn)
    #     raise e

    # return ofn


@CELERY_APP.task(bind=True, base=WithRetry)
@cache_fn_results(path_prefix="osm")
@one_instance(expire=10)
def tag_dicts_to_rules(self, tag_dict_path: str) -> str:
    """
    This task accepts a dict that maps osm tags to dict that map osm values to their occurence does three things:
    1. creates an smrender rules file to make pictures of thoses tag:value pairs each in distinct colors
    2. create a dict that maps the colors used to the tag:value pair
    3. create a dict that shows how often each tag:value pair has occurred

    :param tag_dict: path to pickled dict that matches osm tags like building to osm values of that tag
    :type tag_dict: str
    :return: path to pickle file of dict of path of rulesfile, mapping and reverse mapping
    :rtype: str
    """
    # todo add unpickling of filt to dict of tag_dict
    try:
        with open(tag_dict_path, "rb") as file:
            hist = pickle.load(file)
    except Exception as e:
        logging.error(e)
        raise e
    tag_dict = {key:[val for val in values.keys()] for key, values in hist.items()}
    container = TagRuleContainer(tag_dict)
    tags = list(container)
    rules = create_rules_from_tags(tags)
    mapping = container.get_mapping()
    rev_mapping = container.get_reverse_mapping()
    ofn = get_tempfile()

    try:
        out_dict = {"rules": rules, "mapping": mapping, "rev_mapping": rev_mapping, "hist":hist}
        with open(ofn, "wb") as f:
            pickle.dump(out_dict, f)
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
            d = json.load(f)
        out.update(d)

    ofn = get_tempfile()
    with open(ofn, "w") as f:
        json.dump(out, f)
    return ofn
