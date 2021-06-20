from scipy import ndimage as nd

from pvgrip.raster.mesh \
    import mesh


def fill_missing(data, missing_value = -9999):
    """Replace the value of missing 'data' cells (indicated by
    'missing_value') by the value of the nearest cell

    Taken from: https://stackoverflow.com/a/27745627

    """
    missing = data == missing_value
    ind = nd.distance_transform_edt(missing,
                                    return_distances=False,
                                    return_indices=True)
    return data[tuple(ind)]


def check_box_not_too_big(box, step, mesh_type,
                          limit = 0.1, max_points = 2e+7):
    if abs(box[2] - box[0]) > limit \
       or abs(box[3] - box[1]) > limit:
        raise RuntimeError\
            ("step in box should not be larger than %.2f" % limit)

    grid = mesh(box = box, step = step, which = mesh_type)
    if len(grid['mesh'][0])*len(grid['mesh'][1]) \
       > max_points:
        raise RuntimeError\
            ("either box or resolution is too high!")

    return len(grid['mesh'][0]), len(grid['mesh'][1])
