import pandas as pd
import numpy as np
from sklearn.linear_model import LinearRegression

#load the data 
df = pd.read_csv('data/exoplanet_data.csv')

# simuation by randomly setting values to NaN
np.random.seed(42)  # For reproducibility
missing_surface_temp_indices = np.random.choice(df.index, size=3, replace=False)
missing_mass_indices = np.random.choice(df.index, size=2, replace=False)

df.loc[missing_surface_temp_indices, 'host_star_temperature_K'] = np.nan
df.loc[missing_mass_indices, 'planet_mass_jupiter_units'] = np.nan


# imputation 
df['host_star_temperature_K_imputed'] = df.groupby('detection_method')['host_star_temperature_K'].transform(
    lambda x: x.fillna(x.mean())
)


# pepare data for linear regression (getting rid off rows where both columns are NaN)
df_no_missing = df.dropna(subset=['planet_mass_jupiter_units'])
X = df_no_missing[['orbital_period_days', 'planet_radius_earth_units', 'host_star_temperature_K']]
y = df_no_missing['planet_mass_jupiter_units']

# try a simple linear regression model
model = LinearRegression()
model.fit(X, y)

# predict missing values for planet_mass_jupiter_units
missing_mass = df[df['planet_mass_jupiter_units'].isna()]
X_missing = missing_mass[['orbital_period_days', 'planet_radius_earth_units', 'host_star_temperature_K']]
predictions = model.predict(X_missing)

df.loc[df['planet_mass_jupiter_units'].isna(), 'planet_mass_jupiter_units'] = predictions

#print
print(df.isna().sum())
print(df[['host_star_temperature_K', 'planet_mass_jupiter_units']].isna().sum())
print(df[['host_star_temperature_K', 'planet_mass_jupiter_units']].describe())
print(df[['host_star_temperature_K_imputed', 'planet_mass_jupiter_units']].describe())
