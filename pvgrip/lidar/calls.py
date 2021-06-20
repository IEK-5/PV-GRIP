from pvgrip.lidar.tasks \
    import download_laz, run_pdal, link_ofn


def process_laz(url, ofn, resolution, what, if_compute_las):
    tasks = download_laz\
        .signature(kwargs = {'url': url})

    if if_compute_las:
        tasks |= run_pdal\
            .signature(kwargs = {'ofn': ofn,
                                 'resolution': resolution,
                                 'what': what})
    else:
        tasks |= link_ofn\
            .signature(kwargs = {'ofn': ofn})

    return tasks
