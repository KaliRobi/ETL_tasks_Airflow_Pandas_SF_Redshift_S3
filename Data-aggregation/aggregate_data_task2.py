import pandas as pd

#     Load the CSV data into a DataFrame.
df  = pd.read_csv('../data/ocean_temperature_data.csv')

# Group the data by the location column and calculate the standard deviation of temperature for each region. 
# This will measure the variability in temperature.

temperature_variability = df.groupby('location')['temperature'].std() 

print(temperature_variability)


#     Filter the results to show only regions where the temperature variability is below a certain threshold (e.g., 8°C).

below_treshold  = temperature_variability[temperature_variability < 8]

# print(below_treshold)
#     Display the result.

# Expected Result:

#     A DataFrame showing regions with their corresponding temperature variability (standard deviation).

#     A filtered DataFrame where the temperature variability is less than 8°C.