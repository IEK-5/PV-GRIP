import os
import cv2

import numpy as np
import matplotlib.pyplot as plt

from si_prefix \
    import si_format, si_prefix_scale

from pvgrip.utils.files \
    import remove_file


def draw_scale(height, map_axis, wdir, title, nticks = 5, dpi = 150, fmt = '{:.2f}'):
    fig, ax = plt.subplots(1,1)
    fig.set_dpi(dpi)
    fig.set_size_inches(height/dpi/10, height/dpi)

    ticks = np.linspace(0, 255, nticks)
    ticks_labels = [fmt.format(map_axis(x)).replace("e-0","e-") \
                    for x in ticks]

    ax.imshow(np.transpose(np.tile(np.linspace(255,0,256), (1,1))),
              cmap='gray', aspect='auto')

    ax.set_xticks([])
    ax.set_xticklabels([])
    ax.yaxis.tick_right()
    ax.set_yticks(ticks)
    ax.set_yticklabels(ticks_labels[::-1])
    ax.set_title(title, loc='left')

    fn = os.path.join(wdir, 'draw_scale.png')
    try:
        fig.savefig(fn, bbox_inches = 'tight')
        img = cv2.imread(fn, 0)
    except Exception as e:
        remove_file(fn)
        raise e

    img = cv2.resize(img, (int(height*img.shape[1] / img.shape[0]), int(height)))
    return img.reshape(img.shape + (1,))


def minmaxprefix(img):
    prefix = si_format(img.mean(),0).split(' ')[1]
    if prefix != '':
        delta = si_prefix_scale(prefix)
    else:
        delta = 1
    return img.min()/delta, img.max()/delta, prefix


def addscale(img, title, constant, wdir, nticks = 6):
    m, M, prefix = minmaxprefix(img/constant)
    print("m = {}, M = {}, prefix = {}".format(m,M,prefix))

    mapping = lambda x: x*(M-m)/255 + m

    scale = draw_scale(height = img.shape[0],
                       map_axis = mapping,
                       title = prefix + title,
                       nticks = nticks,
                       wdir = wdir)
    return scale
