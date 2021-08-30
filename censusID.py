import censusgeocode
import pandas as pd
from pandarallel import pandarallel
import numpy as np
import time

start = time.time()

print('\nReading in data...\n')
fname = 'DataFiles/ABCWUA-AMI-Interval-Reads-March-to-June-2021.csv'
df = pd.read_csv(fname, skiprows=0, usecols=[0, 4, 8] + [i for i in range(9, 33)], low_memory=False)

print(f'INFO: Shape of df: {df.shape}.\n')

print('Concatenating batched lat./long. files...\n')
#  Dataframe of all lat./long. pairs, combining the 50 batched lat./long. files
df_coord_combined = pd.concat(
    (pd.read_csv(f) for f in [f'DataFiles/Lat_Long/WUA_geo_{i}.csv' for i in range(50)]))

print('Merging df with lat./long....\n')
#  Add lat./long. to df
df_address = pd.DataFrame(df['ADDRESS'].drop_duplicates())  # Dataframe of unique addresses
df_address = df_address.reset_index(drop=True)  # Reset index, to prepare for merging
df_coord_combined = df_coord_combined.reset_index(drop=True)  # Reset index, to prepare for merging
df_coord_address = pd.concat([df_address, df_coord_combined], axis=1)  # Link address w/ lat./long. in df_coord_address
df = df_coord_address.merge(df, on='ADDRESS', how='inner')  # Merge df with df_coord_address, matching on ADDRESS

print(f'INFO: Shape of df post-merge: {df.shape}.\n')

print('Screening for duplicate lat./long....\n')
#  Screen df_full for duplicate lat./long. (location) and drop duplicates (TODO: sum instead of dropping)
ncoord = df.drop_duplicates(subset='ADDRESS').shape[0]
temp = df.drop_duplicates(subset='ADDRESS')
address_duplicates = pd.DataFrame(temp[temp.duplicated(subset=['latitude', 'longitude'])]['ADDRESS'].reset_index(drop=True))
address_duplicates_index = df[df.ADDRESS.isin(address_duplicates.values.flatten())].index
df.drop(address_duplicates_index, inplace=True)

print(f'{ncoord - df.drop_duplicates(subset="ADDRESS").shape[0]} addresses '
      f'({100 * (ncoord - df.drop_duplicates(subset="ADDRESS").shape[0]) / ncoord:.2f}% '
      f'of the {ncoord} total addresses) resolved to the same location as another address. '
      f'\nOnly the first address within each set of duplicate addresses has been retained.\n')

print(f'INFO: Shape of df post-merge, absent lat./long. duplicates: {df.shape}.\n')

#  Drop NaN location values from df
df.dropna(subset=['ADDRESS', 'latitude', 'longitude'], inplace=True)

print(f'INFO: Shape of df post-merge, absent lat./long. duplicates, absent NaN address, lat., long.: {df.shape}.\n')

#  Get lat./long. from duplicate- and NaN-screened df_full
df_coord_combined = df.drop_duplicates(subset='ADDRESS')[['latitude', 'longitude']]

print(f'INFO: Shape of df_coord_combined: {df_coord_combined.shape}.\n')

print(f'Saving combined coordinate dataframe to single file...\n')
#  Save combined, non-duplicate coordinates to single CSV
fname_save_coord = 'DataFiles/Lat_Long/WUA_geo_combined.csv'
# df_coord_combined.to_csv(fname_save_coord, index=False)

#####
#  Geolocate to census tract
#####

year = 2020

print(f'Gelocating to {year} census tracts...\n')

pandarallel.initialize(progress_bar=False, verbose=0)

if year == 2019:
    #  Use ACS 2019 census tracts (default is to 2020 census determinations)
    cg = censusgeocode.CensusGeocode(benchmark='Public_AR_Current', vintage='ACS2019_Current')
else:
    cg = censusgeocode.CensusGeocode()

batch = 50
df_coord_combined_chunked = np.array_split(df_coord_combined, batch)

print(f'Shape (approx.) of each of the {batch} batched coordinate frames: {df_coord_combined_chunked[0].shape[0]}.\n')

#  Add census tract identifier to each lat./long.
for i, frame in enumerate(df_coord_combined_chunked):
    print(f'{i + 1}/{batch}:')
    print(f'  Begun...')

    frame['tract_geoid'] = frame.parallel_apply(lambda row:
                                                cg.coordinates(
                                                    x=row[1],
                                                    y=row[0])['Census Tracts'][0]['GEOID'],
                                                axis=1)

    fname_save_frame = f'DataFiles/Census/{year}/WUA_tracts_{year}_{i}.csv'
    frame.to_csv(fname_save_frame, index=False)

    print(f'  Complete.\n')

print(f'Concatenating batched census tract files...\n')
census_full = pd.concat(
    (pd.read_csv(f) for f in [f'DataFiles/Census/{year}/WUA_tracts_{year}_{i}.csv' for i in range(batch)]))

print(f'Merging df with census tracts...\n')
df_full = census_full.merge(df, on=['latitude', 'longitude'], how='inner')

print(f'Saving merged dataframe to file...\n')
fname_save_df = f'DataFiles/WUA_full_{year}.csv'
df_full.to_csv(fname_save_df, index=False)

print(f'INFO: Shape of post-processed df: {df.shape}. Shape of post-processed and censused df_full: {df_full.shape}.\n')

print(f'Time to complete: {(time.time() - start)/60:.2f} minutes.')
