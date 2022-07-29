import numpy as np
from typing import Tuple
from scipy.signal import convolve as scipy_convolve


def pad_raster(raster: np.ndarray, pad_shape: Tuple[int, int]) -> np.ndarray:
    """pad a 3d numpy array using the reflect method

    each dimension is padded individually.  e.g. if the raster is a 3
    channel rgb image each color is only padded using the values of
    that color
    """
    raster_padded = np.empty((raster.shape[0] + 2*pad_shape[0],
                              raster.shape[1] + 2*pad_shape[1],
                              raster.shape[2]))

    for i in range(raster.shape[2]):
        raster_padded[:, :, i] = np.pad\
            (raster[:, :, i], pad_shape, "reflect")

    return raster_padded


def unpad_raster(padded_raster: np.ndarray, pad_shape: Tuple[int, int]) -> np.ndarray:
    """Undo the padding done with pad_raster
    """
    width, height = padded_raster.shape[:2]

    return padded_raster[pad_shape[0]:width-pad_shape[0],
                         pad_shape[1]:height-pad_shape[1], :]


def convolve(raster, weights):
    res = []
    kernel_width, kernel_height = weights.shape
    raster_padded = pad_raster(raster, (kernel_width//2, kernel_height//2))
    for i in range(raster.shape[2]):
        res += [scipy_convolve(raster_padded[:, :, i], weights, mode='same')]
    res = np.array(res)
    res = unpad_raster(res, (kerne_width//2, kernel_height//2))
    return np.transpose(res, axes=(1, 2, 0))


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

# todo current filters are average per square meters we need average in relation to filter size
# make new branch of lupin for testing
# e.g. convolution with weight = 1 /n*m with filter size n x m
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
