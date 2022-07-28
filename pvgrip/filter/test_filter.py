import numpy as np
from numpy.testing import assert_allclose
from filters import pad_raster, unpad_raster


def test_pad_raster():
    arr = np.random.randint(0, 10, (5, 5, 3))
    pad_shape = (3, 3)
    arr_padded = pad_raster(arr, pad_shape)
    for i in range(arr.shape[2]):
        actual = arr_padded[pad_shape[0]:-pad_shape[0],pad_shape[1]:-pad_shape[1], i]
        expected = arr[:,:,i]
        assert_allclose(actual, expected)

def test_unpad_raster():
    arr = np.random.randint(0, 10, (5, 5, 3))
    pad_shape = (3, 3)
    arr_padded = pad_raster(arr, pad_shape)
    arr_unpad = unpad_raster(arr_padded, pad_shape)
    assert_allclose(arr_unpad, arr)

if __name__ == '__main__':
    test_pad_raster()
    test_unpad_raster()
