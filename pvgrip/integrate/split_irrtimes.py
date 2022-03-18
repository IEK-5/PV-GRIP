import celery
import logging

import pandas as pd
import numpy as np

from functools import wraps

from pvgrip.storage.remotestorage_path \
    import searchandget_locally
from pvgrip.integrate.tasks \
    import sum_pickle
from pvgrip.storage.upload \
    import upload, Saveas_Requestdata
from pvgrip.raster.calls \
    import convert_from_to


def split_irrtimes(irrtimes_fn, maxnrows):
    irrtimes_fn = searchandget_locally(irrtimes_fn)

    data = pd.read_csv(irrtimes_fn, sep=None, engine='python')

    if 'timestr' not in data:
        raise RuntimeError\
            ('timestr column is missing!')

    data = data.sort_values(by=['timestr'])

    chunks = np.array_split(data, data.shape[0]//maxnrows + 1)
    return [upload(Saveas_Requestdata(x))['storage_fn']
            for x in chunks]


def split_irrtimes_calls\
    (fn_arg,
     output_type_arg = 'output_type',
     maxnrows = 10000,
     **convert_from_to_kwargs):
    """A decorator that splits irradiance times onto chunks

    The passed tasks should contain 'output_type_arg' that accepts
    'pickle' which mean that the result of a task should be a pickle
    rsater file, as defined in the sample_from_box function

    :output_type_arg: in kwargs defining the output_type argument

    :fn_arg: key in kwargs defining the filename argument

    :maxnrows: maximum number of rows per chunk

    """
    def wrapper(fun):
        @wraps(fun)
        def wrap(*args, **kwargs):
            if fn_arg not in kwargs:
                raise RuntimeError\
                    ("{} was not passed as kwargs"\
                     .format(fn_arg))
            if output_type_arg not in kwargs:
                logging.warning("{} was not passed in kwargs"\
                                .format(output_type_arg))
                kwargs[output_type_arg] = 'pickle'
            output_type = kwargs[output_type_arg]

            chunks = split_irrtimes\
                (irrtimes_fn = kwargs[fn_arg],
                 maxnrows = maxnrows)
            tasks = []

            for x in chunks:
                chunk_kwargs = kwargs
                chunk_kwargs.update({fn_arg: x,
                                     output_type_arg: 'pickle'})
                tasks += [fun(*args, **chunk_kwargs)]
            tasks = celery.group(tasks)
            tasks |= sum_pickle.signature()

            return convert_from_to\
                (tasks,
                 from_type = 'pickle',
                 to_type = output_type,
                 **convert_from_to_kwargs)
        return wrap
    return wrapper
