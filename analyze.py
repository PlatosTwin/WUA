# -*- coding: utf-8 -*-
import pandas as pd
import folium
from folium import plugins
import plotly
import plotly.express as px
import plotly.io as pio
import numpy as np
import time
import threading

#####
# TODO: Screen out addresses with greater than ~100 cubic feet/hour of use, to limit to residential
# TODO: Map largest individual users
# TODO: Add popups to maps
# TODO: Check whether df_geo num. rows == df.groupby(['latitude', 'longitude']) num. rows
# TODO: Style bar plot
# TODO: Style map(s)
# TODO: Time study
# TODO: Sprinkler use study (add M, W, F and subtract T, R, S?)
#####

#  Suppress Pandas SettingWithCopyWarning
pd.options.mode.chained_assignment = None



# fname19 = f'DataFiles/WUA_full_2019.csv'
# df19 = pd.read_csv(fname19, skiprows=0, low_memory=False)
# df19.rename(columns={'tract_geoid': 'tract_geoid_19'}, inplace=True)
#
# fname20 = f'DataFiles/WUA_full_2020.csv'
# df20 = pd.read_csv(fname20, skiprows=0, low_memory=False)
# df20.rename(columns={'tract_geoid': 'tract_geoid_20'}, inplace=True)
#
# df_full = df20.merge(df19, left_index=True, right_index=True,
#                      how='outer', suffixes=('', '_DROP')).filter(regex='^(?!.*_DROP)')
#
# cols_to_order = ['latitude', 'longitude', 'tract_geoid_19', 'tract_geoid_20']
# new_columns = cols_to_order + (df_full.columns.drop(cols_to_order).tolist())
# df_full = df_full[new_columns]
#
# df_full.to_csv('DataFiles/WUA_full.csv', index=False)




#  Read in cleaned Water Utility Authority data
print('\nReading-in data...')
downsamp = ''#'_downsamp_f0.075_r1'
fname = f'DataFiles/WUA_full{downsamp}.csv'
df_full = pd.read_csv(fname, skiprows=0, low_memory=False, nrows=10000)

#  Get lat./long. from df
df_coord = df_full.drop_duplicates(subset='ADDRESS')[['latitude', 'longitude']].copy()

#  Screen both df and df_geo for duplicate/out-of-bounds addresses
#   Combine identical locations in df_geo
ncoord = df_coord.shape[0]
df_coord = df_coord.groupby(['latitude', 'longitude']).sum().reset_index()
print(f'\n{ncoord - df_coord.shape[0]} addresses ({100 * (ncoord - df_coord.shape[0]) / ncoord:.2f}%) resolved to the same location '
      f'as an existing address; these \nduplicates have been removed and their quantities added to the originals.')

#   Screen out mis-located addresses in df, i.e., those misidentified to be outside of Albuquerque
ndf = df_full.shape[0]
df_full = df_full[(df_full['tract_geoid_19'] < 35002000000) &
                  (df_full['tract_geoid_19'] >= 35001000000)]  # Relevant tracts: 35001xxxxxx
print(f'\n{ndf - df_full.shape[0]} entries ({100 * (ndf - df_full.shape[0]) / ndf:.2f}%) in df resolved to locations outside '
      f'of Albuquerque; these \nhave been removed.')

#   Screen out mis-located addresses in df_geo, i.e., those misidentified to be outside of Albuquerque
ncoord = df_coord.shape[0]
df_coord = df_coord[(df_coord['latitude'] <= 35.4) &
                    (df_coord['latitude'] >= 34.7) &
                    (df_coord['longitude'] <= -106.2) &
                    (df_coord['longitude'] >= -107.)]
print(f'\n{ncoord - df_coord.shape[0]} locations ({100 * (ncoord - df_coord.shape[0]) / ncoord:.2f}%) in df_geo resolved to '
      f'locations outside of Albuquerque; these \nhave been removed.')

#  Filter df_full to include only low-use (residential) addresses, putting high-use (commerical) into df_high
# want mean hourly usage and mean hourly usage std. dev.

#  Set up for threading, to perform file save operations in the background
class BackgroundSave(threading.Thread):

    def __init__(self, save_obj, file_name):
        # calling superclass init
        threading.Thread.__init__(self)
        self.save_obj = save_obj
        self.fname = file_name

    def run(self):
        if isinstance(self.save_obj, folium.folium.Map):
            self.save_obj.save(self.fname)

        if isinstance(self.save_obj, plotly.graph_objs.Figure):
            pio.write_html(self.save_obj,
                           file=self.fname,
                           auto_open=False,
                           config=dict(modeBarButtonsToRemove=['autoScale2d']))

        #  Wait 0.5 seconds
        time.sleep(0.5)


#####
# Meter location mapping
#####

print('\nMapping locations...')

#  If not downsampled already, take a random sample of df_geo (at 'frac=' fraction), to avoid slowing down mapping
if downsamp == '':
    frac = 0.075
    random_state = 1
    df_geo_sampled = df_coord.sample(frac=frac, random_state=random_state)
    downsamp_loc = f'_f{frac}_r{random_state}'
else:
    df_geo_sampled = df_coord
    downsamp_loc = ''

#  Initiate map
m = folium.Map(
    location=np.array([df_coord[['latitude', 'longitude']].max().to_list(),
                       df_coord[['latitude', 'longitude']].min().to_list()]).mean(axis=0),
    tiles='cartodbpositron',
    zoom_start=12,
)

#  Map the locations using the down-sampled dataframe
df_geo_sampled.apply(lambda row:
                     folium.CircleMarker(
                         location=[row['latitude'], row['longitude']],
                         radius=1,
                         weight=1,
                         fill=True,
                         color='#3186cc',
                         fill_color='#3186cc',
                         fill_opacity=1).add_to(m),
                     axis=1)

#  Add heatmap layer
df_geo_sampled_arr = df_geo_sampled[['latitude', 'longitude']].to_numpy()
m.add_child(plugins.HeatMap(df_geo_sampled_arr, radius=15, min_opacity=0.20))

#  Save file in background
background_locmap = BackgroundSave(m, f'LocationMap_n{df_coord.shape[0]}{downsamp_loc}{downsamp}.html')
background_locmap.start()

#####
# Water usage calculations/plotting
#####

print('\nCalcuating water usage metrics...')

total_mean_usage_hrly = df_full.groupby(['latitude', 'longitude']).mean().drop(
    columns=['tract_geoid_19', 'tract_geoid_20']).mean().to_numpy()

print('foo')
fig = px.bar(total_mean_usage_hrly)

# fig.show()

#  Save file in background
background_fig = BackgroundSave(fig, f'HourlyBar_n{df_coord.shape[0]}{downsamp}.html')
background_fig.start()

#####
# Water usage mapping
# Census tract shape files: https://www.census.gov/geographies/mapping-files/time-series/geo/cartographic-boundary.html
# Shape file to JSON converter: https://mapshaper.org/
# ACS 2019 by-tract population: https://data.census.gov/cedsci/table?g=0500000US35001.140000&tid=ACSDT5Y2019.B01003
#####

print('\nMapping water usage...')

fname_acs = 'DataFiles/ACS 2019 Population/ACSDT5Y2019.B01003_data_with_overlays_2021-07-16T185004.csv'
df_acs = pd.read_csv(fname_acs, skiprows=[1], usecols=[0, 2, 3], low_memory=False)
df_acs['GEO_ID'] = df_acs['GEO_ID'].apply(lambda val: val[9:])
df_acs['B01003_001E'] = df_acs['B01003_001E'].apply(lambda val: int(val))

usage_start = np.where(df_full.columns.values == 'USAGE1')[0][0]  # First column of hourly water usage data

#  Group by tract to get mean daily usage
df_tract = df_full.groupby(['tract_geoid_19']).mean()[df_full.columns.values[usage_start:]].sum(axis=1).reset_index()
df_tract['tract_geoid_19'] = df_tract.tract_geoid.astype(str)
df_tract.rename(columns={0: 'mean_usage_daily'}, inplace=True)

#  Add mean daily usage and population to df_tract
df_tract['mean_usage_daily'] = \
    np.log(df_tract['mean_usage_daily'].replace(0.0, np.nan)).replace(np.nan, 0.0)
df_tract = df_tract.merge(df_acs, left_on='tract_geoid_19', right_on='GEO_ID', how='outer')

#  Initialize mapping
tracts = folium.Map(
    location=df_coord[['latitude', 'longitude']].mean().to_list(),
    tiles='cartodbpositron',
    zoom_start=12,
)

folium.Choropleth(
    geo_data='DataFiles/NM_census_tracts_2019.json',
    name='choropleth',
    data=df_tract,
    columns=['tract_geoid_19', 'mean_usage_daily'],
    key_on='feature.properties.GEOID',
    fill_color='YlGnBu',
    fill_opacity=0.3,
    line_opacity=0.5,
    legend_name='Mean Daily Water Usage (log)',
).add_to(tracts)

tracts.save(f'TractsMap2019_n{df_coord.shape[0]}{downsamp}.html')

background_locmap.join()
background_fig.join()
