import numpy as np

from scipy.signal import fftconvolve


def convolve(raster, weights):
    res = []
    for i in range(raster.shape[2]):
        res += [fftconvolve(raster[:,:,i],weights,mode='same')]
    return np.transpose(np.array(res),axes=(1,2,0))


def const_weights(filter_size, step):
    if 2 != len(filter_size):
        raise RuntimeError\
            ("filter_size = {} must have length 2!"\
             .format(filter_size))

    shape = tuple([int(x//step) for x in filter_size])
    return np.ones(shape)


def sum_convolve(raster, filter_size, step):
    return convolve(raster,
                    const_weights(filter_size, step))


def average_per_sqm(filter_size, step):
    return const_weights(filter_size, step)/np.prod(filter_size)


def average_in_filter(filter_size, step):
    """
    Create a convolution kernel for an average over the area defined by filter_size and step.
    Each value in this kernel is the same and they add up to 1.

    e.g. if filter_size = (2,3) and step=0.5
    then a 4 x 6 array where each value is 1/24 is created.
    """
    kernel = const_weights(filter_size, step)
    n = np.prod(kernel.shape)
    return kernel / n
