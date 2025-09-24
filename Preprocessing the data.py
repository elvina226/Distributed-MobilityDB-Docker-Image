import pandas as pd
import glob2
from sklearn.preprocessing import StandardScaler
from sklearn.neighbors import LocalOutlierFactor
import matplotlib.pyplot as plt
#------------------------------------------------------------- download data ----------------------------------------------------------------------------------
ais_static_saronic = pd.read_csv('D:\\New folder (2)\\6323416\\ais_static\\ais_static\\unipi_ais_static.csv', sep=',', header=0)
print(ais_static_saronic .columns)

noaa_weather = pd.read_csv('D:\\New folder (2)\\6323416\\noaa_weather\\noaa_weather_data.csv', sep=',', header=0)
print(noaa_weather.columns)

# bulk loading for kinematic table
files17 = glob2.glob('D:\\New folder (2)\\6323416\\unipi_ais_dynamic_2017\\*.csv')
files18 = glob2.glob('D:\\New folder (2)\\6323416\\unipi_ais_dynamic_2018\\*.csv')
files19 = glob2.glob('D:\\New folder (2)\\6323416\\unipi_ais_dynamic_2019\\*.csv')
files = files17 + files18 + files19

# read the first file to capture column names
first_df = pd.read_csv('D:\\New folder (2)\\6323416\\unipi_ais_dynamic_2017\\unipi_ais_dynamic_aug2017.csv')
columns = first_df.columns.tolist()

# read remaining files without their headers and assign the same columns
dfs = [first_df]
for f in files[1:]:
    df = pd.read_csv(f, skiprows=1, header=None, names=columns)
    dfs.append(df)

# concatenate into one dataframe
ais_kinematic_saronic = pd.concat(dfs, ignore_index=True)

# convert unix to datetime
ais_kinematic_saronic["datetime"] = pd.to_datetime(ais_kinematic_saronic['t'], unit='ms')
print(ais_kinematic_saronic.columns)

# bulk loading for trajectory_synopses
files17 = glob2.glob('D:\\New folder (2)\\6323416\\unipi_ais_dynamic_synopses\\ais_synopses\\2017\\*.csv')
files18 = glob2.glob('D:\\New folder (2)\\6323416\\unipi_ais_dynamic_synopses\\ais_synopses\\2018\\*.csv')
files19 = glob2.glob('D:\\New folder (2)\\6323416\\unipi_ais_dynamic_synopses\\ais_synopses\\2019\\*.csv')
files = files17 + files18 + files19

# read the first file to capture column names
first_df = pd.read_csv('D:\\New folder (2)\\6323416\\unipi_ais_dynamic_synopses\\ais_synopses\\2017\\unipi_ais_synopses_aug_2017.csv')
columns = first_df.columns.tolist()

# read remaining files without their headers and assign the same columns
dfs = [first_df]
for f in files[1:]:
    df = pd.read_csv(f, skiprows=1, header=None, names=columns)
    dfs.append(df)

# concatenate into one dataframe
trajectory_synopses = pd.concat(dfs, ignore_index=True)

# convert unix to datetime
trajectory_synopses["datetime"] = pd.to_datetime(trajectory_synopses['t'], unit='ms')
print(trajectory_synopses.columns)

#-----------------------------------------------------------------cleaning and preprocessing data-----------------------------------------------------

# for ais_static_saronic
a_before = len(ais_static_saronic)  # length before cleaning

ais_static_saronic = ais_static_saronic[ais_static_saronic['shiptype'].isna() | (ais_static_saronic['shiptype']>=0)&(ais_static_saronic['shiptype']<=99)]    #exclude range of shiptypes
ais_static_saronic = ais_static_saronic.dropna(subset=['vessel_id'])   #drop null vessel_id

#drop duplicates
ais_static_saronic['null_count'] = ais_static_saronic.isna().sum(axis=1)      #Count number of nulls in each row
ais_static_saronic = ais_static_saronic.sort_values(by=['vessel_id', 'null_count'])   #Sort so the row with the fewest nulls comes first within each vessel_id group
ais_static_saronic = ais_static_saronic.drop_duplicates(subset='vessel_id', keep='first')  # Drop duplicates, keeping the first (fewest nulls per vessel_id)
ais_static_saronic = ais_static_saronic.drop(columns='null_count')    #drop the helper column
a_after = len(ais_static_saronic)                    # length after cleaning
print(a_before-a_after)                              # rows deleted

# for ais_kinematic_saronic
b_before = len(ais_kinematic_saronic)  # length before cleaning

ais_kinematic_saronic = ais_kinematic_saronic[ais_kinematic_saronic['vessel_id'].isin(ais_static_saronic['vessel_id'])]    #keep based on ais_static_saronic table
ais_kinematic_saronic = ais_kinematic_saronic.drop_duplicates(subset=['vessel_id', 't'])                                         #logic constraint
ais_kinematic_saronic = ais_kinematic_saronic[(ais_kinematic_saronic['lon']>=-180) & (ais_kinematic_saronic['lon']<=180)]
ais_kinematic_saronic = ais_kinematic_saronic[(ais_kinematic_saronic['lat']>=-90) & (ais_kinematic_saronic['lat']<=90)]
ais_kinematic_saronic = ais_kinematic_saronic[ais_kinematic_saronic['heading'].isna() |(ais_kinematic_saronic['heading']>=0) & (ais_kinematic_saronic['heading']<=359.9)]
ais_kinematic_saronic = ais_kinematic_saronic[ais_kinematic_saronic['course'].isna() |(ais_kinematic_saronic['course']>=0) & (ais_kinematic_saronic['course']<=359.9)]
ais_kinematic_saronic = ais_kinematic_saronic[ais_kinematic_saronic['course'].isna() |(ais_kinematic_saronic['speed']>=0)]

b_after = len(ais_kinematic_saronic)  # length after cleaning
print(b_before-b_after)                     # rows deleted

# for trajectory_synopses
c_before = len(trajectory_synopses)  # length before cleaning

trajectory_synopses = trajectory_synopses.merge(ais_kinematic_saronic[['vessel_id', 't']], on=['vessel_id', 't'], how='inner')  #FK constrain
trajectory_synopses= trajectory_synopses.drop(['lon', 'lat', 'heading', 'speed'], axis=1)      #repeated columns

c_after = len(trajectory_synopses)  # length after cleaning
print(c_before-c_after)                     # rows deleted

#----------------------------------------------------------------- outlier detection -------------------------------------------------------------------

# feature selection
features = ais_kinematic_saronic[['lat', 'lon', 'course', 'speed']].copy()
features = features.dropna()       #for LOF
indexes = features.index

# scaling
scaler = StandardScaler()
features_scaled = scaler.fit_transform(features)

# LOF
lof = LocalOutlierFactor(n_neighbors=20, contamination=0.01)
lof_result = lof.fit_predict(features_scaled)

# create outlier column
ais_kinematic_saronic['outlier'] = 1

# assign LOF results only to rows used
ais_kinematic_saronic.loc[indexes, 'outlier'] = lof_result

# mark outliers
outliers = ais_kinematic_saronic[ais_kinematic_saronic['outlier'] == -1]
inliers = ais_kinematic_saronic[ais_kinematic_saronic['outlier'] == 1]

# Print stats
print(f"Total points: {len(ais_kinematic_saronic)}")
print(f"Outliers detected: {len(outliers)}")

# remove outliers (keep only inliers)
ais_kinematic_saronic = ais_kinematic_saronic[ais_kinematic_saronic['outlier']  != -1].copy()

# drop the outlier column
ais_kinematic_saronic.drop(columns=['outlier'], inplace=True)

# visualization
plt.figure(figsize=(10, 8))
plt.scatter(inliers['lon'], inliers['lat'], s=5, alpha=0.5, label='Inliers')
plt.scatter(outliers['lon'], outliers['lat'], s=10, color='red', alpha=0.7, label='Outliers')
plt.xlabel("Longitude")
plt.ylabel("Latitude")
plt.title("AIS Outlier Detection using LOF")
plt.legend()
plt.grid(True)
plt.tight_layout()
plt.legend(loc='upper right')
plt.savefig("D:\\New folder (2)\\6323416\\outlier_plot.png", dpi=300)

#----------------------------------------------------------------- Descripteves ------------------------------------------------------------------------

ais_kinematic_saronic.select_dtypes(include=['number']).mean()

desc = noaa_weather.describe()
print(desc)
noaa_weather.select_dtypes(include=['number']).mean()

#----------------------------------------------------------------- export data to csv ------------------------------------------------------------------------

# for ais_static_saronic
ais_static_saronic.to_csv('D:\\New folder (2)\\6323416\\ais_static_new\\unipi_ais_static_new.csv', index=False)

# for ais_kinematic_saronic
ais_kinematic_saronic.to_csv('D:\\New folder (2)\\6323416\\unipi_ais_dynamic_new\\unipi_ais_dynamic_new.csv', index=False)

# for trajectory_synopses
trajectory_synopses.to_csv('D:\\New folder (2)\\6323416\\unipi_ais_dynamic_synopses_new\\unipi_ais_synopses_new.csv', index=False)

#--------------------------------------------------------------------------------------------------------------------------------------------------------------




#------------------------------------------------------------- download data for 1 month-----------------------------------------------------------------------
ais_kinematic_saronic_month = pd.read_csv('D:\\New folder (2)\\6323416\\unipi_ais_dynamic_2018\\unipi_ais_dynamic_may2018.csv', sep=',', header=0)
ais_kinematic_saronic_month = ais_kinematic_saronic_month.rename(columns={'timestamp':'t'})  #exei apo timestamp aftos o minas  oxi t
ais_kinematic_saronic_month["datetime"] = pd.to_datetime(ais_kinematic_saronic_month['t'], unit='ms') # convert unix to datetime
print(ais_kinematic_saronic_month.columns)

trajectory_synopses_month = pd.read_csv('D:\\New folder (2)\\6323416\\unipi_ais_dynamic_synopses\\ais_synopses\\2018\\unipi_ais_synopses_may_2018.csv', sep=',', header=0)
trajectory_synopses_month["datetime"] = pd.to_datetime(trajectory_synopses_month['t'], unit='ms') # convert unix to datetime
print(trajectory_synopses_month.columns)
#-----------------------------------------------------------------cleaning and preprocessing data for 1 month-------------------------------------------------

# for ais_kinematic_saronic
b_before = len(ais_kinematic_saronic_month)  # length before cleaning

ais_kinematic_saronic_month = ais_kinematic_saronic_month[ais_kinematic_saronic_month['vessel_id'].isin(ais_static_saronic['vessel_id'])]    #keep based on ais_static_saronic table
ais_kinematic_saronic_month = ais_kinematic_saronic_month.drop_duplicates(subset=['vessel_id', 't'])              #logic constraint
ais_kinematic_saronic_month = ais_kinematic_saronic_month[(ais_kinematic_saronic_month['lon']>=-180) & (ais_kinematic_saronic_month['lon']<=180)]
ais_kinematic_saronic_month = ais_kinematic_saronic_month[(ais_kinematic_saronic_month['lat']>=-90) & (ais_kinematic_saronic_month['lat']<=90)]
ais_kinematic_saronic_month = ais_kinematic_saronic_month[ais_kinematic_saronic_month['heading'].isna() |(ais_kinematic_saronic_month['heading']>=0) & (ais_kinematic_saronic_month['heading']<=359.9)]
ais_kinematic_saronic_month = ais_kinematic_saronic_month[ais_kinematic_saronic_month['course'].isna() |(ais_kinematic_saronic_month['course']>=0) & (ais_kinematic_saronic_month['course']<=359.9)]
ais_kinematic_saronic_month = ais_kinematic_saronic_month[ais_kinematic_saronic_month['course'].isna() |(ais_kinematic_saronic_month['speed']>=0)]

b_after = len(ais_kinematic_saronic_month)  # length after cleaning
print(b_before-b_after)                     # rows deleted

# for trajectory_synopses
c_before = len(trajectory_synopses_month)  # length before cleaning

trajectory_synopses_month = trajectory_synopses_month.merge(ais_kinematic_saronic_month[['vessel_id', 't']], on=['vessel_id', 't'], how='inner')  #FK constrain
trajectory_synopses_month = trajectory_synopses_month.drop(['lon', 'lat', 'heading', 'speed'], axis=1)

c_after = len(trajectory_synopses_month)  # length after cleaning
print(c_before-c_after)                     # rows deleted
#----------------------------------------------------------------- export data to csv for 1 month--------------------------------------------------------------
ais_kinematic_saronic_month.to_csv('D:\\New folder (2)\\6323416\\unipi_ais_dynamic_new\\unipi_ais_dynamic_month.csv', index=False)
trajectory_synopses_month.to_csv('D:\\New folder (2)\\6323416\\unipi_ais_dynamic_synopses_new\\unipi_ais_synopses_month.csv', index=False)






