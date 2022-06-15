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
    map_raster_to_box, collect_json_dicts
from pvgrip.osm.rules import create_rules_from_tags

@call_cache_fn_results(minage = 1650884152)
def osm_render(rules_fn: str, box:Tuple[float, float, float, float], step:float, mesh_type:str, output_type:str):
    """
    Render an osm map using smrender
    Args:
        rules_fn: path to a json file of a Dict[str, Dict[str, List[int, str]]
        it maps osm tags to a map of values to occurrence and hexcolor string
        box: (Tuple[float, float, float float] bounding box of desired locations
        format: [lat_min, lon_min, lat_max, lon_max]
        step: resolution of the sampling mesh in meters
        mesh_type: coordinate system to use either espg code or "utm"
        output_type: choices: "pickle","geotiff","pnghillshade","png","pngnormalize","pngnormalize_scale"

    Returns: rendered image of map

    """
    if rules_fn is None or rules_fn == "NA":
        smrender_rules = rules_fn
    else:
        with open(searchandget_locally(rules_fn), "r") as f:
            osm_hist = json.load(f)
        smrender_rules = create_rules_from_tags(osm_hist)
    width, _ = check_box_not_too_big\
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
                 'width': width})

    tasks |= readpng_asarray.signature\
        (kwargs={'box': box, 'step': step,
                 'mesh_type': mesh_type})

    return convert_from_to(tasks,
                           from_type = 'pickle',
                           to_type = output_type)


# todo: this should maybe live in route
@call_cache_fn_results()
def osm_create_rules_from_route(tsvfn_uploaded, box, box_delta, tags: Set[str], **kwargs):
    """
    Turn a specification of a route( a tsv of coordinates a box min box width and a max box width box_delta)
    and a list of tags of interest into a smrender rules file with a unique distinct colour for each tag:value pair
    and a pickled dict of the mapping of the tag:value pairs to the used colors and the reversed mapping
    :param tsvfn_uploaded: path to uploaded tsv file in pvgrip
    :type tsvfn_uploaded: str
    :param box: box that should sourround each point in the route inn the coordinates used for the mesh
    :type box: Tuple[float, float, float, float]
    :param box_delta: a constant that defines a maximum raster being sampled
    :type box_delta: int
    :param tags:
    :type tags:
    :return:
    :rtype:
    """
    # 1: turn the tsv, box and box_delta into a route
    rasters:List[Dict[str,Tuple[float, float, float, float]]]
    rasters_fn = get_list_rasters \
        (route_fn=searchandget_locally(tsvfn_uploaded),
         box=box, box_delta=box_delta)
    with open(searchandget_locally(rasters_fn), 'rb') as f:
        rasters = pickle.load(f)
    # list of list of boxes
    # each inner list makes up a box of the route
    box_lists = [get_box_list(x['box']) for x in rasters]
    boxes = set([x for box_list in box_lists for x in box_list])

    # 2: turn the route into osm files
    # create a task for each small box
    tasks = celery.group \
        (*[find_osm_data_online.signature \
               (kwargs={'tag': None, 'bbox': x, 'add_centers':False}) | find_tags_in_osm.signature(kwargs={"tags":tags})\
           for x in boxes])

    # 3: distribute osm files to workers and let them in parallel work on finding the tags
    # tasks |= find_tags_in_osm.signature(kwargs={"tags":tags})

    # 4: merge the result of each worker into a single result dict
    tasks |= collect_tags_from_osm.signature()

    return tasks



# todo: this should maybe live in route
@call_cache_fn_results()
def osm_render_from_route(tsvfn_uploaded:str, rulesfn_uploaded:str, box:Tuple[float, float, float, float], box_delta:int, **kwargs):
    """
    This call accepts a path of an uploaded tsv and an uploaded smrender rulesfile as well as args for
    the size of the boxes along the root to render the map according to the rules
    :param tsvfn_uploaded: path to uploaded tsv file in pvgrip
    :type tsvfn_uploaded: str
    :param rulesfn_uploaded: path to uploaded or generated json Dict[str, Dict[str, List[int, str]]
    its a map of osm tags to a map of osm values to tuples of occurrences and hexcolor string
    e.g.
    {"building":{"yes":[100, "ff0000"], "garage":[3, "00ff00"], "":[1000, "0000ff"]}
    Empty strings are interpreted as wildcard characters
    :type rulesfn_uploaded: str
    :param box: box that should sourround each point in the route inn the coordinates used for the mesh
    :type box: Tuple[float, float, float, float]
    :param box_delta: a constant that defines a maximum raster being sampled
    :type box_delta: int
    :param kwargs:
    :type kwargs:
    :return:
    :rtype:
    """
    rasters_fn = get_list_rasters \
        (route_fn=searchandget_locally(tsvfn_uploaded),
         box=box, box_delta=box_delta)
    with open(searchandget_locally(rasters_fn), 'rb') as f:
        rasters = pickle.load(f)
    # fetch the file locally to access it
    rulesfn_uploaded = searchandget_locally(rulesfn_uploaded)
    tasks = celery.group \
        (*[osm_render(rules_fn = rulesfn_uploaded,
                      box = x['box'],
                      **kwargs) | \
           map_raster_to_box.signature(kwargs={'box':x['box']}) \
           for x in rasters])

    return tasks | collect_json_dicts.signature()
