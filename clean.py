import pandas as pd

print(f'Reading in data from 2019 and 2020 files...\n')
#  Combine 2019 and 2020 tract information in one dataframe
fname19 = f'DataFiles/WUA_full_2019.csv'
df19 = pd.read_csv(fname19, skiprows=0, low_memory=False)
df19.rename(columns={'tract_geoid': 'tract_geoid_19'}, inplace=True)

fname20 = f'DataFiles/WUA_full_2020.csv'
df20 = pd.read_csv(fname20, skiprows=0, low_memory=False)
df20.rename(columns={'tract_geoid': 'tract_geoid_20'}, inplace=True)

print(f'Merging files...\n')
df_full = df20.merge(df19, left_index=True, right_index=True,
                     how='inner', suffixes=('', '_DROP')).filter(regex='^(?!.*_DROP)')

print(f'Re-ordering columns...\n')
cols_to_order = ['latitude', 'longitude', 'tract_geoid_19', 'tract_geoid_20']
new_columns = cols_to_order + (df_full.columns.drop(cols_to_order).tolist())
df_full = df_full[new_columns]

print(f'Screening out mislocated addresses...\n')
#  Screen out mis-located addresses in df_full, i.e., those misidentified to be outside of Albuquerque
ndf_full = df_full.shape[0]
df_full = df_full[(df_full['tract_geoid_20'] < 35002000000) &
                  (df_full['tract_geoid_20'] >= 35001000000)]  # Relevant tracts: 35001xxxxxx

print(f'{ndf_full - df_full.shape[0]} entries ({100 * (ndf_full - df_full.shape[0]) / ndf_full:.2f}% of post-processed '
      f'and census-tract tracked entries) resolved to locations outside of Albuquerque, by 2020 census '
      f'tracts, and have been removed.\n')

print(f'Saving merged dataframe to single file...\n')
#  Save to file
fname_full = 'DataFiles/WUA_full.csv'
df_full.to_csv(fname_full, index=False)

print(f'Saving downsampled versions...\n')
#  Create and save downsampled version of df_full (downsampling addresses/locations rather than raw rows)
random_state = 1
for frac in [0.05, 0.075, 0.1, 0.15, 0.3]:
    address_sampled = df_full.drop_duplicates(subset=['latitude', 'longitude']).sample(frac=frac, random_state=random_state)
    df_sampled = address_sampled.merge(df_full, on=['latitude', 'longitude'],
                                       how='inner', suffixes=('', '_DROP')).filter(regex='^(?!.*_DROP)')
    fname_save_df_downsampled = f'DataFiles/WUA_full_downsamp_f{frac}_r{random_state}.csv'
    df_sampled.to_csv(fname_save_df_downsampled, index=False)

print(f'2019 and 2020 census tract identifiers combined into single file, {fname_full}, and \n'
      f'5 downsampled versions saved to WUA_full_downsamp_fX_rY.csv.')
