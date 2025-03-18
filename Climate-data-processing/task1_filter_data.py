import pandas as pd
# Task 1: Filtering Data (Ocean Temperature Data) 

# Tool: Pandas 

# Task: 

#     Load the JSON data into a Pandas DataFrame. 

df = pd.read_json('data/ocean_temperature_data.json', lines=True)

#     Filter the data to show only the rows where the temperature exceeds 10°C. 

result = df[df['temperature'] > 10]

#     Convert the "date" column to a proper datetime format and sort the DataFrame by date. 

result['time'] = pd.to_datetime(result['time'], unit='ms')

#     Display the first 5 rows of the filtered DataFrame. 

print(result.head(5))