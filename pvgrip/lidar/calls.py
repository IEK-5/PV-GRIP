import os

from pvgrip.lidar.tasks \
    import download_laz, run_pdal


def process_laz(url, ofn, resolution, what, if_compute_las):
    tasks = download_laz\
        .signature(kwargs = {'url': url,
                             'ofn': os.path.join(ofn,'src')})

    if if_compute_las:
        tasks |= run_pdal\
            .signature(kwargs = {
                'resolution': resolution,
                'what': what,
                'ofn': os.path.join\
                (ofn, '{}_{:.8f}'.format(what,resolution))})

    return tasks
