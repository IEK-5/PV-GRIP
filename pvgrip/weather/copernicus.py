import os
import re
import cdsapi
import netCDF4
import itertools

import pandas as pd
import numpy as np

from datetime \
    import datetime, timedelta

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


# see: https://cds.climate.copernicus.eu/cdsapp#!/dataset/reanalysis-era5-land?tab=overview
reanalysis_era5_variables = {
    '10m_u_component_of_wind': 'u10',
    '10m_v_component_of_wind': 'v10',
    '2m_dewpoint_temperature': '2dm',
    '2m_temperature': 't2m',
    'evaporation_from_bare_soil': 'evabs',
    'evaporation_from_open_water_surfaces_excluding_oceans': 'evaow',
    'evaporation_from_the_top_of_canopy': 'evatc',
    'evaporation_from_vegetation_transpiration': 'evavt',
    'forecast_albedo': 'fal',
    'lake_bottom_temperature': 'lblt',
    'lake_ice_depth': 'licd',
    'lake_ice_temperature': 'lict',
    'lake_mix_layer_depth': 'lmld',
    'lake_mix_layer_temperature': 'lmlt',
    'lake_shape_factor': 'lshf',
    'lake_total_layer_temperature': 'ltlt',
    'leaf_area_index_high_vegetation': 'lai_hv',
    'leaf_area_index_low_vegetation': 'lai_lv',
    'potential_evaporation': 'pev',
    'runoff': 'ro',
    'skin_reservoir_content': 'src',
    'skin_temperature': 'skt',
    'snow_albedo': 'asn',
    'snow_cover': 'snowc',
    'snow_density': 'rsn',
    'snow_depth': 'sde',
    'snow_depth_water_equivalent': 'sd',
    'snow_evaporation': 'es',
    'snowfall': 'sf',
    'snowmelt': 'smlt',
    'soil_temperature_level_1': 'stl1',
    'soil_temperature_level_2': 'stl2',
    'soil_temperature_level_3': 'stl3',
    'soil_temperature_level_4': 'stl4',
    'sub_surface_runoff': 'ssro',
    'surface_latent_heat_flux': 'slhf',
    'surface_net_solar_radiation': 'ssr',
    'surface_net_thermal_radiation': 'str',
    'surface_pressure': 'sp',
    'surface_runoff': 'sro',
    'surface_sensible_heat_flux': 'sshf',
    'surface_solar_radiation_downwards': 'ssrd',
    'surface_thermal_radiation_downwards': 'strd',
    'temperature_of_snow_layer': 'tsn',
    'total_evaporation': 'e',
    'total_precipitation': 'tp',
    'volumetric_soil_water_layer_1': 'swvl1',
    'volumetric_soil_water_layer_2': 'swvl2',
    'volumetric_soil_water_layer_3': 'swvl3',
    'volumetric_soil_water_layer_4': 'swvl4',
}


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
    (location,
     sky_type = 'observed_cloud', time_step = '1minute',
     altitude = '-999.', fnformat = 'csv'):
    for x in ('region_latitude','region_longitude',
              'region_hash','year','week'):
        if x not in location:
            raise RuntimeError('{} missing from location!'\
                               .format(x))

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

    year,week = location['year'], location['week']
    monday = datetime.strptime('{}-W{}-1'.format(year,week),
                               '%G-W%V-%u')
    sunday = datetime.strptime('{}-W{}-7'.format(year,week),
                               '%G-W%V-%u')

    args = {'sky_type': sky_type,
            'location': \
            {'latitude': location['region_latitude'],
             'longitude': location['region_longitude']},
            'altitude': altitude,
            'date': '{}/{}'\
            .format\
            (datetime.strftime(monday,'%Y-%m-%d'),
             datetime.strftime(sunday,'%Y-%m-%d')),
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
         '{}-W{}'.format(year,week))

    return res


def reanalysis_era5_land\
    (location,
     variables = list(reanalysis_era5_variables.keys()),
     fnformat = 'netcdf'):
    for x in ('region_bbox','region_hash',
              'datetime','date'):
        if x not in location:
            raise RuntimeError('{} missing from location!'\
                               .format(x))


    if not (set(variables) <= set(reanalysis_era5_variables.keys())):
        raise RuntimeError\
            ("""
            invalid variables = {}
            available options = {}
            """\
            .format(variables, reanalysis_era5_variables))

    if fnformat not in ('grib','netcdf'):
        raise RuntimeError('invalid fnformat = {}'\
                           .format(fnformat))

    args = {'time': [
                '00:00', '01:00', '02:00',
                '03:00', '04:00', '05:00',
                '06:00', '07:00', '08:00',
                '09:00', '10:00', '11:00',
                '12:00', '13:00', '14:00',
                '15:00', '16:00', '17:00',
                '18:00', '19:00', '20:00',
                '21:00', '22:00', '23:00',
            ],
            'area': location['region_bbox'],
            'year': location['datetime'].year,
            'month': location['datetime'].month,
            'day': location['datetime'].day,
            'variable': variables,
            'format': fnformat}

    res = {'credentials_type': 'cds',
           'what': 'reanalysis-era5-land',
           'args': args}
    res['ofn'] = os.path.join\
        (RESULTS_PATH,'weather',
         res['credentials_type'],
         res['what'],
         location['region_hash'],
         location['date'])

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


def _era5time2datetime(times, origin = datetime(1900,1,1,0,0,0)):
    return np.array([(origin + timedelta(hours=int(x)))\
                     .timestamp() \
                     for x in times])


def time_location2idx(tl, times, lats, lons):
    tl['dtimes'] = tl['datetime'].apply\
        (lambda x: np.abs(times - x.timestamp()).argmin())
    tl['dlats'] = tl['latitude'].apply\
        (lambda x: np.abs(lats - x).argmin())
    tl['dlons'] = tl['longitude'].apply\
        (lambda x: np.abs(lons - x).argmin())

    return tl


def sample_reanalysis(time_location, source_fn, what):
    src = netCDF4.Dataset(source_fn)

    if not set(what) <= set(reanalysis_era5_variables.keys()):
        raise RuntimeError\
            ("""
            invalid 'what' argument!

            what = {}
            allowed values = {}
            """\
             .format(list(what),
                     list(reanalysis_era5_variables.keys())))

    time_location = time_location2idx\
        (time_location,
         times=_era5time2datetime\
         (src['time'][:].data),
         lats=src['latitude'][:].data,
         lons=src['longitude'][:].data)

    for item in what:
        src_item = reanalysis_era5_variables[item]
        if src_item not in list(src.variables):
            raise RuntimeError("{}={} not in src!"\
                               .format(item,src_item))

        time_location[item] = time_location\
            .apply(lambda x: src[src_item][x['dtimes'],
                                           x['dlats'],
                                           x['dlons']],
                   axis = 1)

    return time_location[list(what)]
