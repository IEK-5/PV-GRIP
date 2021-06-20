import celery

from pvgrip.osm.utils \
    import get_box_list, create_rules

from pvgrip.raster.utils \
    import check_box_not_too_big
from pvgrip.raster.calls \
    import convert2output_type

from pvgrip.osm.tasks \
    import find_osm_data_online, \
    merge_osm, render_osm_data, readpng_asarray


def osm_render(box, step, mesh_type, tag, output_type):
    width, _ = check_box_not_too_big\
        (box = box, step = step,
         mesh_type = mesh_type)
    rules_fn = create_rules(tag)

    box_list = get_box_list(box = box)

    tasks = celery.group\
        (*[find_osm_data_online.signature\
           (kwargs={'tag': tag, 'bbox':x}) \
           for x in box_list])

    tasks |= merge_osm.signature()

    tasks |= render_osm_data.signature\
        (kwargs={'rules_fn': rules_fn,
                 'box': box,
                 'width': width})

    tasks |= readpng_asarray.signature\
        (kwargs={'box': box, 'step': step,
                 'mesh_type': mesh_type})

    return convert2output_type(tasks,
                               output_type = output_type)
