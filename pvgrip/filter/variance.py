import numpy as np

from pvgrip.filter.filters \
    import sum_convolve


def variance(stdev, mean, count, how = sum_convolve, **kwargs):
    """Compute variance from lidar data

    :stdev,mean,count: arrays of the same shape

    :how, kwargs: how to average

    :return: array of the same shape
    """
    if stdev.shape != mean.shape or stdev.shape != count.shape:
        raise RuntimeError\
            ("stdev, mean, count are not the same shape!")

    xi = count*mean
    xisq = count * (stdev*stdev + mean*mean)

    xisq = how(xisq, **kwargs)
    xi = how(xi, **kwargs)
    count = how(count, **kwargs)

    return xisq / count - np.power(xi/count,2)
