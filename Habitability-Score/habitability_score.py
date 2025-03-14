import pandas as pd
import numpy as np
#extac data
df_stg = pd.read_csv('../data/exoplanet_data.csv')
    
#result
df_int = pd.DataFrame()
df_mart = pd.DataFrame()
# ['temperature_score', 'atmospheric_composition', 'water_presence', 'gravitational_score']
df_int['exoplanet'] = df_stg['exoplanet_name']
# Goal: Engineer a new feature, habitability_score, using multiple columns and normalize it. 


# Surface temperature
# Optimal Range (Earth-like conditions): 273 K – 323 K (~0°C – 50°C)

#steps:
#get the absolute value of the measured surface temp minus the optimal Earth like temp (15C), this makes sure both too hot and too cold considered correctly
#then multiply it with the chose steepness of the curve. this is the granuality settings. now we have our exponent
# np.exp rises 2.718 to this power

# using logistic curve
df_int['temperature_score'] = 1 / (1 + np.exp(0.05 * (abs(df_stg['surface_temperature_K'] - 288))))

# we are interested in everything over 1e-3 


# Water presence probability (higher is better) 
df_int['water_presence']  = df_stg['water_presence_probability']


# Atmospheric composition (give weights to O2-N2 > CO2 > H2-He > Unknown) 

score_map = {
    'O2-N2': 1.0,
    'CO2': 0.6,
    'H2-He': 0.2,
    'Unknown': 0.1
}


df_int['atmospheric_composition'] = df_stg['atmospheric_composition'].map(score_map).fillna(0.01)

# Gravitational force (closer to Earth's 1G is better)

df_int['gravitational_score'] = 1 / (1+ np.exp(0.5 * (1 -  df_stg['gravitational_force_EarthG'])    ) )



# filtering by score totals 

print(
df_int[(df_int['temperature_score'] > 1e-3 ) &
(df_int['gravitational_score'] > 0.7) &
(df_int['water_presence'] > 0.5 ) &
(df_int['atmospheric_composition'] > 0.6)])
  



# Combine these factors into a formula to compute a habitability_score.   Normalize the score between 0 and 1 using Min-Max scaling. 

# weights
weights = {
    "temperature_score": 0.4,
    "water_presence": 0.4,
    "atmospheric_composition": 0.3,
    "gravitational_score": 0.1
}

df_mart = df_int


df_mart["habitability_score"] = (df_int["temperature_score"] * weights["temperature_score"] +
                            df_int["water_presence"] * weights["water_presence"] +
                            df_int["atmospheric_composition"] * weights["atmospheric_composition"] +
                            df_int["gravitational_score"] * weights["gravitational_score"])


print(df_mart[df_mart['habitability_score'] > 0.75])

