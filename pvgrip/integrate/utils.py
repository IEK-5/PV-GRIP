import pandas as pd

from pvgrip.ssdp.utils \
    import timestr2utc_time


def write_irrtimes(ifn, ofn):
    data = pd.read_csv(ifn, sep=None, engine='python')
    data.columns = [x.lower() for x in data.columns]

    if 'timestr' not in data \
       or 'ghi' not in data \
       or 'dhi' not in data:
        raise RuntimeError\
            ('timestr, ghi or dhi columns are missing!')

    data['utctime'] = data['timestr'].apply(timestr2utc_time)

    data[['utctime','ghi','dhi']].to_csv(ofn, sep='\t',
                                         index = False,
                                         header = False)
