import json
import pickle
import logging
import celery

from typing import Set, List, Dict, Tuple

from pvgrip.storage.remotestorage_path \
    import searchandget_locally

from pvgrip.route.calls \
    import route_rasters
from pvgrip.route.cluster_route_boxes \
    import get_list_rasters
from pvgrip.route.split_route \
    import split_route_calls

from pvgrip.utils.cache_fn_results \
    import call_cache_fn_results

from pvgrip.osm.utils \
    import get_box_list, get_rules_from_pickle

from pvgrip.raster.utils \
    import check_box_not_too_big
from pvgrip.raster.calls \
    import convert_from_to

from pvgrip.osm.tasks \
    import find_osm_data_online, \
    merge_osm, render_osm_data, readpng_asarray, \
    find_tags_in_osm, collect_tags_from_osm,\
    map_raster_to_box, collect_json_dicts, order_dict_by_list, merge_and_order_osm_rules
from pvgrip.osm.rules import create_rules_from_tags


@call_cache_fn_results(minage = 1650884152)
def osm_render(rules_fn: str, box:Tuple[float, float, float, float], step:float, mesh_type:str, output_type:str):
    """Render an osm map using smrender

    :rules_fn: path to a json file of a Dict[str, Dict[str, List[int, str]]
        it maps osm tags to a map of values to occurrence and hexcolor string

    :box, step, mesh_type, output_type: similar to
    ?pvgrip.raster.calls.sample_raster

    :returns: rendered map image

    """
    if rules_fn is None or rules_fn == "NA":
        smrender_rules = rules_fn
    else:
        with open(searchandget_locally(rules_fn), "r") as f:
            osm_hist = json.load(f)
            osm_hist = dict(osm_hist)
        smrender_rules = create_rules_from_tags(osm_hist)

    width, height = check_box_not_too_big\
        (box = box, step = step,
         mesh_type = mesh_type)

    box_list = get_box_list(box = box)
    # set add_centers to false to get the same result as with the other osm render functions
    # if this breaks smrender somehow then it needs to be set to True
    tasks = celery.group\
        (*[find_osm_data_online.signature\
        (kwargs={'tag': None, 'bbox':x, 'add_centers': False}) \
           for x in box_list])

    tasks |= merge_osm.signature()

    tasks |= render_osm_data.signature\
        (kwargs={'rules_fn': smrender_rules,
                 'box': box,
                 'width': width,
                 'height': height})

    tasks |= readpng_asarray.signature\
        (kwargs={'box': box, 'step': step,
                 'mesh_type': mesh_type})

    return convert_from_to(tasks,
                           from_type = 'pickle',
                           to_type = output_type)


@split_route_calls(fn_arg = 'tsvfn_uploaded',
                   merge_task = merge_and_order_osm_rules,
                   merge_task_args={"order_by":"tags"})
@call_cache_fn_results()
def osm_create_rules_from_route(tsvfn_uploaded, box, box_delta, tags):
    """Create a rules file from OSM along a route

    OSM data along a route is processed, and a distinct colour for
    each tag:value pair is created.

    :tsvfn_uploaded: see route_fn in ?get_list_rasters

    :box, box_delta: see ?get_list_rasters

    :tags: a list of tags to use

    :return: path to a json with a list of lists with 2 elements:
    tag and dict, dict maps values to list of 2 elements: occurrence and color
    """
    rasters_fn = get_list_rasters \
        (route_fn=searchandget_locally(tsvfn_uploaded),
         box=box, box_delta=box_delta)
    with open(searchandget_locally(rasters_fn), 'rb') as f:
        rasters = pickle.load(f)

    box_lists = [get_box_list(x['box']) for x in rasters]
    # ignore any route structure, just query individual boxes
    boxes = set([x for box_list in box_lists for x in box_list])

    tasks = celery.group \
        (*[find_osm_data_online.signature \
               (kwargs={'tag': None, 'bbox': x, 'add_centers':False}) | \
           find_tags_in_osm.signature(kwargs={"tags":tags}) \
           for x in boxes])
    return tasks | collect_tags_from_osm.signature()


@split_route_calls(fn_arg = 'tsvfn_uploaded',
                   merge_task = collect_json_dicts)
@call_cache_fn_results()
def osm_render_from_route(tsvfn_uploaded, rules_fn, box, box_delta, **kwargs):
    """Generate a series of OSM rasters along a route

    This call accepts a path of an uploaded tsv and an uploaded
    smrender rulesfile as well as args for the size of the boxes along
    the root to render the map according to the rules

    :tsvfn_uploaded: path to uploaded tsv route file

    :rules_fn: path to uploaded or generated json Dict[str, Dict[str,
    List[int, str]] its a map of osm tags to a map of osm values to
    tuples of occurrences and hexcolor string e.g.
    {"building":{"yes":[100, "#ff0000"], "garage":[3, "#00ff00"],
    "":[1000, "#0000ff"]} Empty strings are interpreted as wildcard
    characters

    :box, box_delta: see ?get_list_rasters

    :kwargs: passed to osm_raster

    """
    rasters_fn = get_list_rasters \
        (route_fn=searchandget_locally(tsvfn_uploaded),
         box=box, box_delta=box_delta)
    with open(searchandget_locally(rasters_fn), 'rb') as f:
        rasters = pickle.load(f)

    rules_fn = searchandget_locally(rules_fn)

    tasks = celery.group \
        (*[osm_render(rules_fn = rules_fn,
                      box = x['box'],
                      **kwargs) | \
           map_raster_to_box.signature(kwargs={'box':x['box']}) \
           for x in rasters])

    return tasks | collect_json_dicts.signature()
