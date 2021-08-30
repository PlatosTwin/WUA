# -*- coding: utf-8 -*-
import pandas as pd
import folium
from folium import plugins
import plotly
import plotly.express as px
import plotly.graph_objects as go
import plotly.io as pio
import numpy as np
import time
import threading

#####
# TODO: Map largest individual users
# TODO: Add popups to maps
# TODO: Check whether df_geo num. rows == df.groupby(['latitude', 'longitude']) num. rows
# TODO: Style bar plot
# TODO: Style map(s)
# TODO: Time study
# TODO: Sprinkler use study (add M, W, F and subtract T, R, S?)
#####

#####
#  Front matter
#####

#  Suppress Pandas SettingWithCopyWarning
pd.options.mode.chained_assignment = None

#  Read in processed Water Utility Authority data
print('\nReading-in data...')
downsamp = ''#'_downsamp_f0.075_r1'
fname = f'DataFiles/WUA_full{downsamp}.csv'
df_full = pd.read_csv(fname, skiprows=0, low_memory=False, nrows=10000)

print(f'Shape of {fname}: {df_full.shape}')

#  Get lat./long. from location delimited df_full
df_coord = df_full.drop_duplicates(subset='ADDRESS')[['latitude', 'longitude']]

#  Drop ADDRESS column
df_full.drop(['ADDRESS'], axis=1)

#  Convert READDATE to datetime type
df_full.loc[:, 'READDATE'] = pd.to_datetime(df_full['READDATE'])

#  Filter df_full to include only low-use (residential) addresses, putting high-use (commerical) into df_high
mean_daily = df_full.groupby(['latitude', 'longitude']).mean()[[f'USAGE{i}' for i in range(1, 25)]].mean(axis=1)
daily_mu = mean_daily.mean()
daily_sigma = mean_daily.std()
high = mean_daily[mean_daily > daily_mu + 2*daily_sigma].index

drop_high = []
for pair in high:
    drop_high.append(df_full[(df_full['latitude'] == pair[0]) & (df_full['longitude'] == pair[1])].index)

df_high = df_full.iloc[list(np.concatenate(drop_high).flat)]
df_full.drop(list(np.concatenate(drop_high).flat), inplace=True)

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
# Water usage timeline calculations
#####

print('\nCalcuating water usage timeline...')

timeline = df_full.groupby(['READDATE']).mean().drop(
    columns=['latitude', 'longitude', 'tract_geoid_19', 'tract_geoid_20']).reset_index()

num_days = timeline.shape[0]
days_from_start = np.arange(num_days)

#  Segment timeline to days of week
day_names = ['mon', 'tues', 'wed', 'thurs', 'fri', 'sat', 'sun']
for i, d in enumerate(day_names):
    exec(d + ' = timeline[(days_from_start % 7 == i)]')

#  Segment timeline to weeks
weeks = [timeline.iloc[7*i : 7*(i+1)] for i in range(0, 17)]

mean_weekly = [weeks[k][[f'USAGE{i}' for i in range(1, 25)]].mean(axis=1).mean() for k in range(0, 17)]

days_of_week = []
temp = None
for d in day_names:
    exec('temp = ' + d)
    days_of_week.append([temp.mean(axis=1).mean(), temp.mean(axis=1).std()])

#  Create figure
fig = go.Figure(layout_yaxis_range=[0, 4])

dataset = sun
for k in range(dataset.shape[0]):
    fig.add_trace(
        go.Bar(
            visible=False,
            x=[i for i in range(0, 25)],
            y=dataset.iloc[k],
            name='Primary Product',
            marker_color='indianred'
        )
    )

fig.data[0].visible = True

steps = []
for i in range(len(fig.data)):
    step = dict(
        method="update",
        args=[{"visible": [False] * len(fig.data)},
              {"title": "Slider switched to step: " + str(i)}],  # layout attribute
    )
    step["args"][0]["visible"][i] = True  # Toggle i'th trace to "visible"
    steps.append(step)

sliders = [dict(
    active=0,
    currentvalue={"prefix": "Frequency: "},
    pad={"t": 50},
    steps=steps
)]

fig.update_layout(
    sliders=sliders,
    title_text='Mean Hourly Usage'
)

# fig.show()

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


fig = px.bar(total_mean_usage_hrly)

# fig.show()

#  Save file in background
background_fig = BackgroundSave(fig, f'HourlyBar_n{df_coord.shape[0]}{downsamp}.html')
background_fig.start()

#####
# Water usage mapping (choropleth)
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
df_tract['tract_geoid_19'] = df_tract.tract_geoid_19.astype(str)
df_tract.rename(columns={0: 'mean_usage_daily'}, inplace=True)

#  Add mean daily usage and population to df_tract
df_tract['mean_usage_daily'] = \
    np.log(df_tract['mean_usage_daily'].replace(0.0, np.nan)).replace(np.nan, 0.0)
df_tract = df_tract.merge(df_acs, left_on='tract_geoid_19', right_on='GEO_ID', how='inner')

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
