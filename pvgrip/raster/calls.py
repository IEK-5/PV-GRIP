import celery

from pvgrip.storage.remotestorage_path \
    import searchif_instorage

from pvgrip.globals \
    import get_SPATIAL_DATA

from pvgrip.lidar.calls \
    import process_laz

from pvgrip.raster.tasks \
    import save_png, save_geotiff, \
    save_pnghillshade, save_pickle, \
    sample_from_box
from pvgrip.raster.utils \
    import check_box_not_too_big


def check_all_data_available(*args, **kwargs):
    SPATIAL_DATA = get_SPATIAL_DATA()
    index = SPATIAL_DATA.subset(*args, **kwargs)

    tasks = []
    for x in index.iterate():
        if searchif_instorage(x['file']):
            continue

        if 'remote_meta' in x:
            tasks += [process_laz\
                      (url = x['url'],
                       ofn = x['file'],
                       resolution = x['pdal_resolution'],
                       what = x['stat'],
                       if_compute_las = x['if_compute_las'])]
            continue

        raise RuntimeError('%s file is not available'\
                           % x['file'])
    return celery.group(tasks)


def _convert_from_pickle(tasks, output_type):
    if output_type not in ('geotiff', 'pickle',
                           'pnghillshade','png',
                           'pngnormalize'):
        raise RuntimeError("Invalid 'output_type' argument!")

    if output_type == 'png':
        tasks |= save_png.signature()
        return tasks

    if output_type == 'pngnormalize':
        tasks |= save_png.signature(kwargs = {'normalize': True})
        return tasks

    if output_type in ('geotiff', 'pnghillshade'):
        tasks |= save_geotiff.signature()

    if output_type == 'pnghillshade':
        tasks |= save_pnghillshade.signature()

    return tasks


def convert_from_to(tasks, from_type, to_type):
    if 'pickle' == from_type:
        if 'pickle' == to_type:
            return tasks

        return _convert_from_pickle(tasks, to_type)

    if 'geotiff' == from_type:
        if 'geotiff' == to_type:
            return tasks

        if 'pnghillshade' == to_type:
            return tasks | save_pnghillshade.signature()

        tasks = tasks | save_pickle.signature()


    return _convert_from_pickle(tasks, to_type)


def sample_raster(box, data_re, stat,
                  mesh_type, step, output_type):
    check_box_not_too_big(box = box, step = step,
                          mesh_type = mesh_type)

    tasks = celery.chain\
        (check_all_data_available(box = box,
                                  data_re = data_re,
                                  stat = stat),\
         sample_from_box.signature\
         (kwargs = {'box': box,
                    'data_re': data_re,
                    'stat': stat,
                    'mesh_type': mesh_type,
                    'step': step},
          immutable = True))

    return convert_from_to(tasks,
                           from_type = 'pickle',
                           to_type = output_type)
