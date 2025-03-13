import pandas as pd
    # Load the JSON dataset into a Pandas DataFrame. 

df = pd.read_csv('../data/climate_data.csv')

    # Convert the "timestamp" column to a datetime type.

df['time'] = pd.to_datetime(df['time'])

 
    # Rename the column to reflect the change
df = df.rename(columns={'time': 'datetime'})

    # Set the "timestamp" as the index for time-series analysis. 

df.set_index('datetime', inplace=True)

    #  A DataFrame with individual columns for "temperature," "humidity," and "wind_speed," indexed by the "timestamp." 

new_df = df[['temperature', 'humidity', 'wind_speed' ]]


    # Display the transformed DataFrame with a focus on the first few rows to verify. 


print(new_df.head(5))