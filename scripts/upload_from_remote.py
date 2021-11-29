#!/usr/bin/env python3

import os
import sys

from pvgrip.globals \
    import get_SPATIAL_DATA

def print_usage(cmd):
    print("""
    {} <path>

    path  a path to a csv file that contain remote storage rasters
    """.format(cmd))


if __name__ == '__main__':
    if 2 != len(sys.argv):
        print_usage(sys.argv[0])
        sys.exit()

    csvfn = os.path.realpath(sys.argv[1])
    data = get_SPATIAL_DATA()
    data.upload_from_csv(csvfn)
