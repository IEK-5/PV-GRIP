import pyproj

import pandas as pd

from pvgrip.ssdp.utils \
    import timestr2utc_time


_T2MT = pyproj.Transformer.from_crs(4326, 3857,
                                    always_xy=True)


def _convert_to_metric(lon, lat):
    return _T2MT.transform(lon, lat)


def write_locations(route,
                    ghi_default, dhi_default,
                    time_default, locations_fn):
    with open(locations_fn, 'w') as f:
        for x in route:
            if 'longitude' not in x or 'latitude' not in x:
                raise RuntimeError\
                    ("longitude or latitude is missing!")

            lon_met, lat_met = \
                _convert_to_metric(x['longitude'], x['latitude'])

            if 'dhi' not in x:
                x['dhi'] = dhi_default

            if 'ghi' not in x:
                x['ghi'] = ghi_default

            if 'timestr' not in x:
                x['utc_time'] = timestr2utc_time(time_default)
            else:
                x['utc_time'] = timestr2utc_time(x['timestr'])

            fmt = '\t'.join(('%.12f',)*4 + ('%d\n',))

            f.write(fmt %(lat_met, lon_met, x['ghi'],x['dhi'],x['utc_time']))

    return route


def write_result(route, ssdp_ofn, ofn):
    df_a = pd.DataFrame(route)
    df_b = pd.read_csv(ssdp_ofn, header=None)
    df_b.columns = ['POA']
    df_c = pd.concat([df_a.reset_index(drop=True), df_b], axis=1)
    return df_c.to_csv(ofn, sep='\t', index=False)
