import os
import re
import cdsapi
import itertools

import pandas as pd

from datetime import datetime

from pvgrip.globals \
    import RESULTS_PATH

from pvgrip.utils.times \
    import time_step2seconds


timeperiod_re = \
    r'([0-9]{4}-[0-9]{2}-[0-9]{2}T' + \
    r'[0-9]{1,2}:[0-9]{1,2}:[0-9]{1,2})\..*'
timeperiod_re = '{x}/{x}'\
    .format(x = timeperiod_re)
timeperiod_re = re.compile('^' + timeperiod_re + '$')


reanalysis_era5_variables = [
    '10m_u_component_of_wind','10m_v_component_of_wind',
    '2m_dewpoint_temperature','2m_temperature',
    'evaporation_from_bare_soil',
    'evaporation_from_open_water_surfaces_excluding_oceans',
    'evaporation_from_the_top_of_canopy',
    'evaporation_from_vegetation_transpiration',
    'forecast_albedo',
    'lake_bottom_temperature','lake_ice_depth',
    'lake_ice_temperature','lake_mix_layer_depth',
    'lake_mix_layer_temperature','lake_shape_factor',
    'lake_total_layer_temperature',
    'leaf_area_index_high_vegetation',
    'leaf_area_index_low_vegetation',
    'potential_evaporation','runoff',
    'skin_reservoir_content','skin_temperature',
    'snow_albedo','snow_cover',
    'snow_density','snow_depth',
    'snow_depth_water_equivalent',
    'snow_evaporation', 'snowfall', 'snowmelt',
    'soil_temperature_level_1', 'soil_temperature_level_2',
    'soil_temperature_level_3', 'soil_temperature_level_4',
    'sub_surface_runoff', 'surface_latent_heat_flux',
    'surface_net_solar_radiation',
    'surface_net_thermal_radiation', 'surface_pressure',
    'surface_runoff', 'surface_sensible_heat_flux',
    'surface_solar_radiation_downwards',
    'surface_thermal_radiation_downwards',
    'temperature_of_snow_layer',
    'total_evaporation', 'total_precipitation',
    'volumetric_soil_water_layer_1',
    'volumetric_soil_water_layer_2',
    'volumetric_soil_water_layer_3']


def retrieve(credentials, what, args, ofn):
    """Download CDS/ADS data

    :credentials: a dictionary with 'url' and 'key'

    :what, args: name and args of the cds request

    :ofn: output file name
    """
    os.makedirs(os.path.dirname(ofn), exist_ok = True)
    c = cdsapi.Client(
        url=credentials['url'],
        key=credentials['key'])
    c.retrieve(what, args, ofn)


def cams_solar_radiation_timeseries\
    (date, location,
     sky_type = 'observed_cloud', time_step = '1minute',
     altitude = '-999.', fnformat = 'csv'):
    if not isinstance(date, datetime):
        raise RuntimeError('"date" is not of type datetime')

    if 'region_latitude' not in location or \
       'region_longitude' not in location or \
       'region_hash' not in location:
        raise RuntimeError\
            ("'region_latitude', 'region_longitude' "+\
             "or 'region_hash' are missing")

    if sky_type not in ('observed_cloud','clear'):
        raise RuntimeError('invalid sky_type = {}'\
                           .format(sky_type))

    if time_step not in ('1minute','15minute','1hour',
                         '1day','1month'):
        raise RuntimeError('invalid time_step = {}'\
                           .format(time_step))

    if fnformat not in ('csv','netcdf'):
        raise RuntimeError('invalid fnformat = {}'\
                           .format(fnformat))

    args = {'sky_type': sky_type,
            'location': \
            {'latitude': location['region_latitude'],
             'longitude': location['region_longitude']},
            'altitude': altitude,
            'date': datetime.strftime(date,'%Y-%m-%d/%Y-%m-%d'),
            'time_step': time_step,
            'time_reference': 'universal_time',
            'format': fnformat}

    res = {'credentials_type': 'ads',
           'what': 'cams-solar-radiation-timeseries',
           'args': args}
    res['ofn'] = os.path.join\
        (RESULTS_PATH,'weather',
         res['credentials_type'],
         res['what'],
         location['region_hash'],
         datetime.strftime(date,'%Y-%m-%d'))

    return res


def reanalysis_era5_land\
    (date, location,
     variable, fnformat = 'grib'):
    if not isinstance(date, datetime):
        raise RuntimeError('"date" is not of type datetime')

    if 'bbox' not in location:
        raise RuntimeError\
            ('missing bbox in location = {}'.\
             format(location))

    if not (set(variable) <= set(reanalysis_era5_variables)):
        raise RuntimeError\
            ("""
            invalid variable = {}
            available options = {}
            """\
            .format(variable, reanalysis_era5_variables))

    if fnformat not in ('grib','netcdf'):
        raise RuntimeError('invalid fnformat = {}'\
                           .format(fnformat))

    args = {'product_type': 'reanalysis',
            'time': [
                '00:00', '01:00', '02:00',
                '03:00', '04:00', '05:00',
                '06:00', '07:00', '08:00',
                '09:00', '10:00', '11:00',
                '12:00', '13:00', '14:00',
                '15:00', '16:00', '17:00',
                '18:00', '19:00', '20:00',
                '21:00', '22:00', '23:00',
            ],
            'box': location['region_bbox'],
            'year': date.year,
            'month': date.month,
            'day': date.day,
            'variable': variable,
            'format': fnformat}

    res = {'credentials_type': 'ads',
           'what': 'reanalysis-era5-land',
           'args': args}
    res['ofn'] = os.path.join\
        (RESULTS_PATH,'weather',
         res['credentials_type'],
         res['what'],
         location['region_hash'],
         datetime.strftime(date,'%Y-%m-%d'))

    return res


def read_irradiance_csv(ifn):
    """Read irradiance csv

    :ifn: path to a download ads csv file

    :return: pandas dataframe
    """
    with open(ifn, 'r') as f:
        *_comments, header = itertools.takewhile\
            (lambda line: line.startswith('#'), f)
    data = pd.read_csv(ifn, sep = None,
                       engine = 'python', comment='#',
                       header = None)
    # here '[2:]' because I assume that
    # header starts with '# Observation period;...'
    data.columns = header[2:].rstrip().split(';')

    data['time_from'], data['time_to'] = \
        zip(*data['Observation period']\
            .map(lambda x:\
                 timeperiod_re.findall(x)[0]))
    data['time_from'] = data['time_from'].apply\
        (lambda x: datetime.strptime(x,'%Y-%m-%dT%H:%M:%S'))
    data['time_to'] = data['time_to'].apply\
        (lambda x: datetime.strptime(x,'%Y-%m-%dT%H:%M:%S'))
    return data


def sample_irradiance(time_location, source_fn,
                      what = ('GHI','DHI'),
                      cams_time_step = '1minute'):
    irr = read_irradiance_csv(source_fn)

    if not set(what) <= set(irr.columns):
        raise RuntimeError\
            ("""
            columns missing from the irradiance source!

            required columns = {}
            source columns = {}
            """\
             .format(list(what),list(irr.columns)))

    res = []
    for row in time_location.to_dict(orient = 'records'):
        x = irr.loc[(row['datetime'] >= irr['time_from']) & \
                    (row['datetime'] < irr['time_to']),
                    list(what)]

        if x.shape[0] != 1:
            raise RuntimeError\
                ("""
                irradiance source for matching date
                has nrows != 1. something is fishy...

                row['datetime'] = {}
                matching irr.shape = {}
                source_fn = {}
                """\
                 .format(row['datetime'],x.shape,source_fn))

        value = x.to_numpy()[0]
        # "convert" Wh to W
        value *= 3600/time_step2seconds(cams_time_step)
        res += [value]

    return res
