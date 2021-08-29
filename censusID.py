import censusgeocode
import pandas as pd
from pandarallel import pandarallel
import numpy as np

print('\nReading in data...\n')
fname = 'DataFiles/ABCWUA-AMI-Interval-Reads-March-to-June-2021.csv'
df = pd.read_csv(fname, skiprows=0, usecols=[0, 4, 8] + [i for i in range(9, 33)], nrows=119*2340*4, low_memory=False)

print('Concatenating batched lat./long. files...\n')
#  Dataframe of all lat./long. pairs, combining the 50 batched lat./long. files
df_coord_combined = pd.concat(
    (pd.read_csv(f) for f in [f'DataFiles/Lat_Long/WUA_geo_{i}.csv' for i in range(50)]))

print('Merging addresses with lat./long....\n')
#  Add lat./long. to df
df_address = pd.DataFrame(df['ADDRESS'].drop_duplicates())  # Dataframe of unique addresses
df_address = df_address.reset_index(drop=True)  # Reset index, to prepare for merging
df_coord_combined = df_coord_combined.reset_index(drop=True)  # Reset index, to prepare for merging
df_coord_address = pd.concat([df_address, df_coord_combined], axis=1)  # Link address w/ lat./long. in df_coord_address
df = df_coord_address.merge(df, on='ADDRESS', how='inner')  # Merge df with df_coord_address, matching on ADDRESS

#  Drop NaN location values from both df_coord_combined and df
df_coord_combined = df_coord_combined.dropna(subset=['latitude', 'longitude'])
df = df.dropna(subset=['latitude', 'longitude'])

#  Save combined coordinates to single CSV
fname_save_coord = 'DataFiles/Lat_Long/WUA_geo_combined.csv'
# df_coord_combined.to_csv(fname_save_coord, index=False)

#####
#  Geolocate to census tract
#####

year = 2019

print('Gelocating to census tract...\n')

pandarallel.initialize(progress_bar=True, verbose=0)

if year == 2019:
    #  Use ACS 2019 census tracts (default is to 2020 census determinations)
    cg = censusgeocode.CensusGeocode(benchmark='Public_AR_Current', vintage='ACS2019_Current')
else:
    cg = censusgeocode.CensusGeocode()

batch = 50
df_coord_combined_chunked = np.array_split(df_coord_combined, batch)

#  Add census tract identifier to each lat./long.
for i, frame in enumerate(df_coord_combined_chunked):
    print(f'Beginning batch {i+1} out of {batch}...\n')

    frame['tract_geoid'] = frame.parallel_apply(lambda row:
                                                cg.coordinates(
                                                    x=row[1],
                                                    y=row[0])['Census Tracts'][0]['GEOID'],
                                                axis=1)

    print(f'Merging df with census tracts and saving file...\n')
    temp = frame.merge(df, on=['latitude', 'longitude'], how='inner')

    fname_save_frame = f'DataFiles/Census/WUA_tracts_{year}_{i}.csv'
    temp.to_csv(fname_save_frame, index=False)

    print(f'Batch {i + 1} complete.\n')

print(f'Concatenating batched census tract files...\n')
df_full = pd.concat(
    (pd.read_csv(f).dropna(subset=['ADDRESS']) for f in [f'DataFiles/Census/WUA_tracts_{year}_{i}.csv' for i in range(batch)]))

fname_save_df = f'DataFiles/WUA_full_{year}.csv'
df_full.to_csv(fname_save_df, index=False)
