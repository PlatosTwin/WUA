import pandas as pd

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

print(f'Screening out mislocated addresses\n')
#  Screen out mis-located addresses in df_full, i.e., those misidentified to be outside of Albuquerque
ndf_full = df_full.shape[0]
df_full = df_full[(df_full['tract_geoid_19'] < 35002000000) &
                  (df_full['tract_geoid_19'] >= 35001000000)]  # Relevant tracts: 35001xxxxxx

print(f'{ndf_full - df_full.shape[0]} entries ({100 * (ndf_full - df_full.shape[0]) / ndf_full:.2f}% of census-tract '
      f'tracked entries) in df resolved to locations outside of Albuquerque and have been removed.\n')

fname_full = 'DataFiles/WUA_full.csv'
df_full.to_csv(fname_full, index=False)

#  Create and save downsampled version of df_full
random_state = 1
for frac in [0.05, 0.075, 0.1, 0.15, 0.3]:
    df_sampled = df_full.sample(frac=frac, random_state=random_state)
    fname_save_df_downsampled = f'DataFiles/WUA_full_downsamp_f{frac}_r{random_state}.csv'
    df_sampled.to_csv(fname_save_df_downsampled, index=False)

print(f'2019 and 2020 census tract identifiers combined into single file, {fname_full}, and \n'
      f'5 downsampled versions save to WUA_full_downsamp_fX_rY.csv.')