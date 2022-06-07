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
    find_tags_in_osm, collect_tags_from_osm, tag_dicts_to_rules,\
    map_raster_to_box, collect_json_dicts


@call_cache_fn_results(minage = 1650884152)
def osm_render(rules_fn, box, step, mesh_type, output_type):
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
        (kwargs={'rules_fn': rules_fn,
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
    :param tsvfn_uploaded:
    :type tsvfn_uploaded:
    :param box:
    :type box:
    :param box_delta:
    :type box_delta:
    :param tags:
    :type tags:
    :return:
    :rtype:
    """
    # 1: turn the tsv, box and box_delta into a route
    rasters:List[Dict[str,Tuple[float, float, float, float]]]
    # tasks, rasters = route_rasters \
    #     (tsvfn_uploaded=tsvfn_uploaded, box=box,
    #      box_delta=box_delta, **kwargs)
    print(box)
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

    # 5: turn the dict into rules
    # todo figure out to to return tuples or change behaviour to not use tuples
    tasks |= tag_dicts_to_rules.signature()

    return tasks



# todo: this should maybe live in route
@call_cache_fn_results()
def osm_render_from_route(tsvfn_uploaded, rulesfn_uploaded, box, box_delta, **kwargs):
    """
    This call accepts a path of an uploaded tsv and an uploaded smrender rulesfile as well as args for
    the size of the boxes along the root to render the map according to the rules
    :param tsvfn_uploaded: path to uploaded tsv file in pvgrip
    :type tsvfn_uploaded: str
    :param rulesfn_uploaded: path to uploaded or generated rules file in pvgrip or path to output from osm_create_rules_from_route
    :type rulesfn_uploaded:
    :param box: box that should sourround each point in the route inn the coordinates used for the mesh
    :type box: Tuple[float, float, float, float]
    :param box_delta:
    :type box_delta:
    :param kwargs:
    :type kwargs:
    :return:
    :rtype:
    """
    logging.debug("osm_render_from__route", kwargs)
    kwargs["output_type"]="".join([i for i in kwargs["output_type"]]) # this is just to test
    # it seems the passing of args is broken because it turns a string into a list of chars
    # or I am using curl wrong
    rasters_fn = get_list_rasters \
        (route_fn=searchandget_locally(tsvfn_uploaded),
         box=box, box_delta=box_delta)
    with open(searchandget_locally(rasters_fn), 'rb') as f:
        rasters = pickle.load(f)
    # fetch the file locally to access it
    rulesfn_uploaded = searchandget_locally(rulesfn_uploaded)
    # it its not an osm file assume its a pickled dict with the key "rules" in it
    # which points to the rules file
    if not rulesfn_uploaded.endswith(".osm"):
        rulesfn_uploaded = get_rules_from_pickle(rulesfn_uploaded)
    tasks = celery.group \
        (*[osm_render(rules_fn = rulesfn_uploaded,
                      box = x['box'],
                      **kwargs) | \
           map_raster_to_box.signature(kwargs={'box':x['box']}) \
           for x in rasters])

    return tasks | collect_json_dicts.signature()
