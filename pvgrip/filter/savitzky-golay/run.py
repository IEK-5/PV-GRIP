#!/usr/bin/env python

import os
import struct
import itertools

import numpy as np

from multiprocessing import Pool



def readfilter(fn):
    """Read filter from binary file

    In case file is incorrect, an exception is raised

    :fn: filename

    :return: np.array with the filter

    """
    f = open(fn,'rb')

    sizeof_int = len(struct.pack("i",int(0)))
    sizeof_double = len(struct.pack("d",float(0)))

    def r(frmt,sz):
        byte = f.read(sz)
        if len(byte) != sz:
            close(f)
            raise RuntimeError("prematurely ended file")
        return struct.unpack(frmt, byte)[0]
    ri = lambda: r("i",sizeof_int)
    rd = lambda: r("d",sizeof_double)

    nw,ne,nn,ns = [ri(),ri(),ri(),ri()]
    fltr = [rd() for i in range(0,(nn+ns+1)*(nw+ne+1))]

    fltr = np.array(fltr)
    fltr = np.reshape(fltr, (nn+ns+1,-1))
    return fltr


def run_filterimage(commands, filterimage_path):
    dpath = tempfile.mkdtemp(dir='.')
    path = os.path.join(dpath,'fifo')
    os.mkfifo(path)

    # just do FilterImage -f

    rc = t.join()
    os.remove(path)
    os.rmdir(dpath)

    if rc:
        raise RuntimeError("FilterImage has non-zero exit")


def filterimage_filter(shape, degree = 2, df = [(1,1,0),(1,0,1)]):
    """Get an array of filter weights of the given shape

    :shape: shape of the output array

    :degree: degree of the polynomial fit

    :df: list of tuples of 3, where (a,n,m) correspond to the axnym
    string and tuples are join as sums

    :return: np.array of the shape = shape

    """
    dpath = tempfile.mkdtemp(dir=".")
    path = os.path.join(dpath,'filter')

    # determine nn,ns,nw,ne parameters
    nn = (shape[0]-1)//2 + (shape[0]-1)%2
    ns = (shape[0]-1)//2
    ne = (shape[1]-1)//2 + (shape[1]-1)%2
    nw = (shape[1]-1)//2

    # get filter parameter string
    df = [str(x[0])+"x"+str(x[1])+"y"+str(x[2]) for x in df]
    df = '+'.join(df)

    commands=[]
    commands+=["makefilter F=filter" +
               " nn=" + str(nn) +
               " ns=" + str(ns) +
               " ne=" + str(ne) +
               " nw=" + str(nw) +
               " m="  + str(degree) +
               " dm=" + df]
    commands+=["fsave F=filter file=" + path]

    run_filterimage(commands, "./FilterImage")
    res = readfilter(path)

    os.remove(path)
    os.rmdir(dpath)

    return res


def degree_df_ordering(n):
    """Generate degree and df parameters following some ordering

    :n: number of parameter sets to generate

    :return: list of dictionaries with keys 'degree' and 'df'

    """
    res = []
    degree = 1

    while len(res) < n:
        for pair in filter(lambda x: x[0]+x[1] <= degree,
                        itertools.product(range(0,degree+1),range(0,degree+1))):
            a = {"degree":degree,"d":pair[0]+pair[1],"df":[(1,) + pair]}
            x = list(filter(lambda x: x["d"] == a["d"] and x["degree"] == a["degree"], res))

            res += [a]
            res += [{"degree":a["degree"],"d":a["d"],"df":x["df"] + a["df"]} for x in x]

            if len(res) >= n:
                break
        degree += 1

    return [{"degree":x["degree"],"df":x["df"]} for x in res[0:n]]


def _filterimage_layer(p):
    w = filterimage_filter(p["shape"][0:2],degree=p["degree"],df=p["df"])
    return np.repeat(w[:,:,np.newaxis],p["shape"][2],axis=2)


def filterimage_layer(shape):
    params = degree_df_ordering(shape[3])

    for p in params:
        p["shape"] = shape

    P = Pool()
    results = P.map(_filterimage_layer, params)
    P.close()
    P.join()

    return np.transpose(np.array(results), axes=(1,2,3,0))
