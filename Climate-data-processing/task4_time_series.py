import pandas as pd
import matplotlib.pyplot as plt

# Load the dataset into Pandas
ocean_data = pd.read_csv("ocean_temperature_data.csv")

# Convert 'time' to datetime format
ocean_data['time'] = pd.to_datetime(ocean_data['time'])

# Set 'time' as index
ocean_data.set_index('time', inplace=True)

# Resample data to show weekly average temperature
weekly_avg_temp = ocean_data['temperature'].resample('W').mean()

# Plot the time series of weekly average temperatures
plt.figure(figsize=(10,5))
plt.plot(weekly_avg_temp, marker='o', linestyle='-')
plt.title("Weekly Average Ocean Temperature")
plt.xlabel("Date")
plt.ylabel("Temperature")
plt.grid()
plt.show()
