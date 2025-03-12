import pandas as pd

#    Load the CSV dataset into a Pandas DataFrame. 
df = pd.read_csv('../data/climate_data.csv ')

# Check for any missing values in the columns. 

print(df.isna().sum() )

# For numerical columns like "temperature" or "humidity," fill missing values with the mean or median. 

num_cols = df.select_dtypes(include='number').columns

df[num_cols]  = df[num_cols].fillna( df[num_cols].mean())

for col  in df.select_dtypes(include='object').columns:
    df[col] = df[col].fillna(df[col].value_counts().idxmax())
    
# Drop rows with missing date values 

df = df.drop(df[df['time'].isna()].index)

#Print the count of missing values in each column after cleaning.
print(df.head(5))



