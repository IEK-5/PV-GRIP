import pyproj

import pandas as pd

from pvgrip.ssdp.utils \
    import timestr2utc_time

from pvgrip.utils.epsg \
    import ll2epsg


def write_locations(route, ghi_default, dhi_default, time_default,
                    azimuth_default, zenith_default, offset_default,
                    locations_fn, epsg):
    with open(locations_fn, 'w') as f:
        for x in route:
            if 'longitude' not in x or 'latitude' not in x:
                raise RuntimeError\
                    ("longitude or latitude is missing!")

            lat_met, lon_met = \
                ll2epsg(lat = x['latitude'], lon = x['longitude'], epsg = epsg)

            if 'dhi' not in x:
                x['dhi'] = dhi_default

            if 'ghi' not in x:
                x['ghi'] = ghi_default

            if 'azimuth' not in x:
                x['azimuth'] = azimuth_default

            if 'zenith' not in x:
                x['zenith'] = zenith_default

            if 'offset' not in x:
                x['offset'] = offset_default

            if 'timestr' not in x:
                x['utc_time'] = timestr2utc_time(time_default)
            else:
                x['utc_time'] = timestr2utc_time(x['timestr'])

            fmt = '\t'.join(('%.12f',)*7 + ('%d\n',))

            f.write(fmt %(lat_met, lon_met,
                          x['ghi'],x['dhi'],
                          x['azimuth'],x['zenith'],x['offset'],
                          x['utc_time']))

    return route


def write_result(route, ssdp_ofn, ofn):
    df_a = pd.DataFrame(route)
    df_b = pd.read_csv(ssdp_ofn, header=None)
    df_b.columns = ['POA']
    df_c = pd.concat([df_a.reset_index(drop=True), df_b], axis=1)
    return df_c.to_csv(ofn, sep='\t', index=False)
