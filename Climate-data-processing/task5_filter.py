import pandas as pd

# Load the JSON data into a Pandas DataFrame
ocean_data = pd.read_json("ocean_temperature_data.json")

# Filter the data to show only records from the region "North Atlantic"
north_atlantic_data = ocean_data[ocean_data['location'] == "North Atlantic"]

# Sort the records by temperature in descending order
sorted_data = north_atlantic_data.sort_values(by='temperature', ascending=False)

# Display the first few rows
print(sorted_data.head())
