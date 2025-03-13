import pandas as pd

#  Load the CSV data into a  DataFrame. 

df = pd.read_csv('../data/ocean_temperature_data.csv')

#     Group the data by the "location" column and calculate the average temperature per region. 

temperature_mean  = df['temperature'].groupby(df['location']).mean()
depth_mean = df['depth'].groupby(df['location']).mean()

print(temperature_mean)

#     Filter the results to show only regions where the average temperature is above a certain threshold (e.g., 15°C). 

temperature_treshold_passed = temperature_mean[temperature_mean > 15] 


#     Display the result. 

print(temperature_treshold_passed)

