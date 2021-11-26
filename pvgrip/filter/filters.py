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
