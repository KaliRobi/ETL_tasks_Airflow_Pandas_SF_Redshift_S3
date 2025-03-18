import pandas as pd
from datetime import timedelta
# Task: 

# Load the CSV file into a Pandas DataFrame. 

df = pd.read_csv('data/climate_data.csv')

# Convert the "timestamp" column into a proper datetime format. 

df['time'] = pd.to_datetime(df['time'])


# Create a new column showing the "day" extracted from the timestamp. 

df['day']  = df['time'].dt.day

print(df[df['time'] > pd.Timestamp.today() - timedelta(days=3)])


# Filter the data to show records only from the last three days 

