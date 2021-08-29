import time
from time import sleep
import pandas as pd
from geopy import GoogleV3
from tqdm import tqdm
import numpy as np
from geopy.extra.rate_limiter import RateLimiter

#  Suppress Pandas SettingWithCopyWarning
pd.options.mode.chained_assignment = None

#####
#  Read-in main data, dropping unnecessary columns
#####

print('\nReading-in data...')

#  Read in Water Utility Authority data, omitting unnecessary columns (fast)
fname = 'DataFiles/ABCWUA-AMI-Interval-Reads-March-to-June-2021.csv'
df_full = pd.read_csv(fname, skiprows=0, usecols=[0, 4, 8] + [i for i in range(9, 33)], low_memory=False)

print('\nGeolocating addresses...\n')
start = time.time()

#  Geolocate the addresses in df, using geopy's GoogleV3.
#   1) create a dataframe for addresses;
#   2) split this dataframe into 'batch' number of batches;
#   3) per batch, geocode addresses and save them to CSV file.

batch = 50

df_coord = pd.DataFrame(df_full['ADDRESS'].drop_duplicates())
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

    runtime = (time.time() - start) / 60
    print(f'\nBatch {i + 1}/{batch} complete. Time elapsed: {(time.time() - frame_start) / 60:.2f} minutes.\n')

print(f'All batches complete. Time elapsed: {(time.time() - start) / 60:.2f} minutes.\n')
