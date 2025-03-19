import pandas as pd

# Load the CSV data into a Pandas DataFrame
climate_data = pd.read_csv("climate_data.csv")

# Convert 'time' to datetime format
climate_data['time'] = pd.to_datetime(climate_data['time'])

# Extract month and region
climate_data['month'] = climate_data['time'].dt.month

# Pivot table with average temperature per month for each region
pivot_table = climate_data.pivot_table(values='temperature', index='location', columns='month', aggfunc='mean')

# Display the result
print(pivot_table)