import time
from time import sleep
import pandas as pd
from geopy import GoogleV3
from tqdm import tqdm
import numpy as np
from geopy.extra.rate_limiter import RateLimiter
import censusgeocode
from pandarallel import pandarallel

#  Suppress Pandas SettingWithCopyWarning
pd.options.mode.chained_assignment = None

#####
#  Read-in main data, dropping unnecessary columns
#####

print('\nReading-in data...')

#  Read in Water Utility Authority data, omitting unnecessary columns (fast)
fname = 'DataFiles/ABCWUA-AMI-Interval-Reads-March-to-June-2021.csv'
df_full = pd.read_csv(fname, skiprows=0, usecols=[0, 4, 8] + [i for i in range(9, 33)], low_memory=False)


def geolocate_latlon(df, batch=50):
    """
    Geolocate the addresses in df, using geopy's GoogleV3.

    1) create a dataframe for addresses; 2) split this dataframe into 'batch'
    number of batches; 3) per batch, geocodes addresses and save them to CSV file.
    :return: N/A
    """

    print('\nGeolocating addresses...\n')
    start = time.time()

    df_coord = pd.DataFrame(df['ADDRESS'].drop_duplicates())
    df_coord_chunked = np.array_split(df_coord, batch)

    tqdm.pandas()  # Enable progress bar
    locator = GoogleV3(api_key='MY_KEY')

    geocode = RateLimiter(locator.geocode, min_delay_seconds=0.4)

    #  Geocode and save batche CSVs
    for i, frame in enumerate(df_coord_chunked):
        frame_start = time.time()

        sleep(0.05)  # Keeps double progress bar from appearing (most of the time)
        frame['location'] = frame['ADDRESS'].progress_apply(lambda row: geocode(row, timeout=None))

        frame['point'] = frame['location'].apply(lambda loc: tuple(loc.point) if loc else None)
        frame[['latitude', 'longitude', 'altitude']] = pd.DataFrame(frame['point'].tolist(), index=frame.index)
        frame = frame.drop(['point', 'altitude', 'ADDRESS', 'location'], axis=1)

        fname_save_frame = f'DataFiles/Lat_Long/WUA_geo_{i}.csv'

        frame.to_csv(fname_save_frame, index=False)

        runtime = (time.time() - start)/60
        print(f'\nBatch {i+1}/{batch} complete. Time elapsed: {(time.time() - frame_start) / 60:.2f} minutes.\n')

    print(f'All batches complete. Time elapsed: {(time.time() - start)/60:.2f} minutes.\n')


def geolocate_census(df, batch=50, year=2019):
    """
    :param df: dataframe missing lat./long. columns
    :param batch: the number of batches into which to separate the coordinates in 'df' (default=50)
    :param year: year from which to use census tracts (default=2019)
    :return: df (dataframe with tract_geoid column), df_geo (dataframe combining all batches from 'geolocate_latlon()'
    """
    print('Combining dataframes and removing NaN entries...\n')

    #  Combine all chunked CSV files, containing lat./long.
    df_coord_combined = pd.concat(
        (pd.read_csv(f) for f in [f'DataFiles/Lat_Long/WUA_geo_{i}.csv' for i in range(batch)]))

    #  Add lat./long. to df
    df_address = pd.DataFrame(df['ADDRESS'].drop_duplicates())  # Dataframe of unique addresses
    df_address = df_address.reset_index(drop=True)  # Reset index, to prepare for merging
    df_coord_combined = df_coord_combined.reset_index(drop=True)  # Reset index, to prepare for merging
    df_coord_address = pd.concat([df_address, df_coord_combined], axis=1)  # Link address with lat./long. in df_coord_address
    df = df_coord_address.merge(df, on='ADDRESS', how='outer')  # Merge df with df_geo_address, matching on ADDRESS

    #  Drop NaN location values from both df_geo_final and df
    df_coord_combined = df_coord_combined.dropna(subset=['latitude', 'longitude'])
    df = df.dropna(subset=['latitude', 'longitude'])

    #####
    #  Geolocate to census tract
    #####

    print('Gelocating to census tract...\n')

    pandarallel.initialize(progress_bar=True, verbose=0)

    if year == 2019:
        #  Use ACS 2019 census tracts (default is to 2020 census determinations)
        cg = censusgeocode.CensusGeocode(benchmark='Public_AR_Current', vintage='ACS2019_Current')
    else:
        cg = censusgeocode.CensusGeocode()

    #  Add census tract identifier to each lat./long.
    df_coord_combined['tract_geoid'] = df_coord_combined.parallel_apply(lambda row:
                                                                        cg.coordinates(
                                                                            x=row[1],
                                                                            y=row[0])['Census Tracts'][0]['GEOID'],
                                                                        axis=1)

    #  Merge df_geo_combined into df, to add census tract identifier to df entries
    df = df_coord_combined.merge(df, on=['latitude', 'longitude'], how='outer')

    #  Drop 'tract_geoid' from df_geo_combined
    df_coord_combined.drop(columns=['tract_geoid'])

    return df, df_coord_combined


# geolocate_latlon(df)  # Very time intensive and needs to be run ONLY ONCE per address CSV
df_full, df_coord_combined = geolocate_census(df_full)

#  Save dataframes
fname_save_coord = 'DataFiles/Lat_Long/WUA_geo_combined.csv'
df_coord_combined.to_csv(fname_save_coord, index=False)

fname_save_df = 'DataFiles/WUA_full_2019_.csv'
df_full.to_csv(fname_save_df, index=False)

#####
#  Other manipulations, not necessarily to be performed sequentially following above
#####

#  Combine 2019 and 2020 tract information in one dataframe
fname19 = f'DataFiles/WUA_full_2019.csv'
df19 = pd.read_csv(fname19, skiprows=0, low_memory=False)
df19.rename(columns={'tract_geoid': 'tract_geoid_19'}, inplace=True)

fname20 = f'DataFiles/WUA_full_2020.csv'
df20 = pd.read_csv(fname20, skiprows=0, low_memory=False)
df20.rename(columns={'tract_geoid': 'tract_geoid_20'}, inplace=True)

df_full = df20.merge(df19, left_index=True, right_index=True,
                     how='outer', suffixes=('', '_DROP')).filter(regex='^(?!.*_DROP)')

cols_to_order = ['latitude', 'longitude', 'tract_geoid_19', 'tract_geoid_20']
new_columns = cols_to_order + (df_full.columns.drop(cols_to_order).tolist())
df_full = df_full[new_columns]

df_full.to_csv('DataFiles/WUA_full.csv', index=False)

#  Create and save downsampled version of df
random_state = 1
for frac in [0.05, 0.075, 0.1, 0.15, 0.3]:
    df_sampled = df_full.sample(frac=frac, random_state=random_state)
    fname_save_df_downsampled = f'DataFiles/WUA_full_2019_downsamp_f{frac}_r{random_state}.csv'
    df_sampled.to_csv(fname_save_df_downsampled, index=False)

print(f'Success! Cleaned dataframes saved to {fname_save_coord} and {fname_save_df}, '
      f'and WUA_full_2019_downsamp_fX_rY.csv.')
