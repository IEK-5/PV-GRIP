import pandas as pd


def iterate_csv(fn, chunksize=10000, **kwargs):
    for chunk in pd.read_csv(fn, chunksize=chunksize,
                             escapechar='\\', **kwargs):
        for _, row in chunk.iterrows():
            yield row
