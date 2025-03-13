import pandas as pd


#  Load the CSV data into a DataFrame.
df = pd.read_csv('../data/ocean_temperature_data.csv')

# Group the data by both the location and depth columns.
grouped_temp = df.groupby(['location', 'depth'])['temperature']

#  Calculate the average temperature for each combination of region and depth.
average_temp_per_region_depth = grouped_temp.mean()


# Filter the results to show only depth levels where the average temperature is below a certain threshold (e.g., 13°C).
below_critical = average_temp_per_region_depth[average_temp_per_region_depth < 13]

# Display the result.

print(below_critical)