### **Task 4: Create a Summary Report for Each Host Star Temperature Range**  
#### **Goal:** Generate a **grouped summary report** for different host star temperature ranges.  

#### **Steps:**  
# 1. **Bin `host_star_temperature_K`** into the following categories:  
#    - **Cool Stars**: < 4000K  
#    - **Sun-like Stars**: 4000K–6000K  
#    - **Hot Stars**: > 6000K  
# 2. Group by these categories and calculate:  
#    - **Average planet radius**  
#    - **Median orbital period**  
#    - **Max water presence probability**  
# 3. Format the result as a **clean Pandas DataFrame summary**.  


import pandas as pd

df1 = pd.read_csv('../data/exoplanet_data.csv')
df2 = pd.read_json('../data/exoplanet_data_b.json')

#adding the two dataframes togehther
df = pd.concat([df1, df2], axis=0).drop_duplicates()

#removing incorrect values
incorrect_star_temperature_rows = df['host_star_temperature_K'].isna() | (df['host_star_temperature_K'] < 500.0)

incorrect_planet_radius_rows = df['planet_radius_earth_units'].isna() | df['planet_radius_earth_units'] <= 0.0

df = df[~(incorrect_star_temperature_rows | incorrect_planet_radius_rows)]


# print(mean_temp)
# 1. **Bin `host_star_temperature_K`** into the following categories:  
#    - **Cool Stars**: < 4000K  
#    - **Sun-like Stars**: 4000K–6000K  
#    - **Hot Stars**: > 6000K  

bins =  [0, 4000, 6000, float('inf')]

labels = ['Cool Stars', 'Sun-like Stars', 'Hot Stars' ]

df['temp_bin'] = pd.cut(df['host_star_temperature_K'], bins=bins, labels=labels)


# print(df.head(5))
# 2. Group by these categories and calculate:  
#    - **Average planet radius**  
#    - **Median orbital period**  
#    - **Max water presence probability**  

# planet_radius_earth_units
summary = df.groupby('temp_bin',observed=False).agg(
    average_planet_radius=('planet_radius_earth_units', 'mean'),
    median_orbiatal_period=('orbital_period_days', 'median'),
    max_water_presence_probability=('water_presence_probability', 'max')
).reset_index().round(2)




print(summary)