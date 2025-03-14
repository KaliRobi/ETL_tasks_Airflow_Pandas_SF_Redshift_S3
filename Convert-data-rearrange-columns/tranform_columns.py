import pandas as pd

def load_data(file_path):
    # Load CSV data into a DataFrame
    return pd.read_csv(file_path)

def process_data(df):
    # Convert 'time' column to datetime format
    df['time'] = pd.to_datetime(df['time'])

    # Rename 'time' column to 'datetime'
    df = df.rename(columns={'time': 'datetime'})

    # Set 'datetime' as the index for time-series analysis
    df.set_index('datetime', inplace=True)

    # Select specific columns for analysis
    return df[['temperature', 'humidity', 'wind_speed']]

def main():
    # Define file path and load data
    input_file = '../data/climate_data.csv'
    df = load_data(input_file)

    # Process the data and extract relevant columns
    new_df = process_data(df)

    # Display the first few rows of the cleaned data
    print(new_df.head(5))

    # Return the processed DataFrame
    return new_df

if __name__ == "__main__":
    main()
