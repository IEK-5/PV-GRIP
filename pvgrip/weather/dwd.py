import requests
import pandas as pd
import numpy as np

from bs4 import BeautifulSoup as bs


class DWD_Data:
    DWD_DATA_URL = 'https://opendata.dwd.de/climate_environment/CDC/' +\
        'observations_germany/climate/10_minutes/solar/historical/'
    DWD_LOCATIONS = 'https://www.dwd.de/DE/leistungen/' +\
        'klimadatendeutschland/statliste/statlex_html.html;jsessionid=' +\
        '32DE96EEB8E1877EA218A42FED2234F5.live11044?view=' +\
        'nasPublication&nn=16102'
    WEATHER_DATA_TYPE = 'KL'

    def __init__():
        pass


    def _locations():
        # should not be queries too often
        data = pd.read_html(self.DWD_LOCATIONS, header=0, skiprows=1)[0]
        data = data[data.Kennung == self.WEATHER_DATA_TYPE]

        data.rename(columns={'Breite': 'latitude', 'Länge': 'longitude',
                             'Beginn': 'time_start', 'Ende': 'time_end'}, inplace=True)
        return data


    def _filenames():
        # should not be queried too often
        data = requests.get(self.DWD_DATA_URL).content

        data = [link.get('href') \
                for link in bs(data, features="lxml").findAll('a')]
        data.pop(0)

        # better filter non-matching ".*\.zip"
        data.pop(-1)
        data.pop(-1)
        data.pop(-1)

        return data


    def _download(url):
        df = pd.read_csv(DWD_DATA_URL + x, sep=';')

        # make a DatetimeIndex using values from the df
        index = pd.to_datetime(df.MESS_DATUM, format='%Y%m%d%H%M')
        df.index = index

        # rename columns
        df.rename(columns={'DS_10': 'dhi', 'GS_10': 'ghi'}, inplace=True)

        # replace -999 values with nans
        df.ghi.replace(-999, np.nan, inplace=True)
        df.dhi.replace(-999, np.nan, inplace=True)

        # unit conversion J/cm2 -> w/m2
        conversion_factor = (1 / 600) * 10000
        df.ghi = df.ghi * conversion_factor
        df.dhi = df.dhi * conversion_factor

        pass


#%% functions to retrieve dwd irradiance data using the station id


def retrieve_dwd_irradiance_data(station_id):
    """
    Retrieve weather data for a weather station using its station_id.
    station_id information can be retrieved using the get_dwd_locations()
    function.

    Parameters
    ----------
    station_id : int
        Unique ID given to each weather station.

    Returns
    -------
    dwd_df : df
        Dataframe with weather data for the given station. Data available
        for all years for that station is returned.
    """
    # make station_id 5 digits long
    station_id = "%05d" % station_id

    # get all available dwd file names
    dwd_files = get_dwd_irradiance_datafile_names()

    # for the given station_id get all the matching files
    files_to_retrieve = [x for x in dwd_files
                         if x.split('_')[2] == station_id]

    dwd_df = pd.DataFrame()
    for x in files_to_retrieve:
        df = pd.read_csv(DWD_DATA_URL + x, sep=';')

        # make a DatetimeIndex using values from the df
        index = pd.to_datetime(df.MESS_DATUM, format='%Y%m%d%H%M')
        df.index = index

        # rename columns
        df.rename(columns={'DS_10': 'dhi', 'GS_10': 'ghi'}, inplace=True)

        # replace -999 values with nans
        df.ghi.replace(-999, np.nan, inplace=True)
        df.dhi.replace(-999, np.nan, inplace=True)

        # unit conversion J/cm2 -> w/m2
        conversion_factor = (1 / 600) * 10000
        df.ghi = df.ghi * conversion_factor
        df.dhi = df.dhi * conversion_factor

        dwd_df = dwd_df.append(df)

    return dwd_df
