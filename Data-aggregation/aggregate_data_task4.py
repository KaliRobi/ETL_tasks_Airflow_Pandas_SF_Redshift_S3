import pandas as pd

#  Load the JSON dataset into a DataFrame. 

df = pd.read_json('data/climate_data.json', lines=True)

# Group the data by "region" and calculate the average humidity per region. 

average_humidity = df.groupby('location')['humidity'].mean().round(3).to_frame().rename(columns={'humidity': 'average_humidity' })


# print(average_humidity)

# Filter regions with an average humidity of more than  54.5%. 

regions = average_humidity[average_humidity['average_humidity'] > 54.5 ]


# Show the results in ascending order of average humidity. 
print(regions.sort_values('average_humidity', ascending=True))