import pandas as pd

 # Load the CSV dataset into a Pandas DataFrame.
df = pd.read_csv('../data/climate_data.csv ')

# Check for missing values in all columns.
# print(df.isna().sum())

mcdf =  df.isna().sum()
mcdf = mcdf.get(mcdf > 0)

# For numerical columns like temperature and wind_speed, fill missing values with the mean.

num_cols = df.select_dtypes(include='number').columns

df[num_cols] = df[num_cols].fillna( df[num_cols].mean() )

#     For categorical columns like location, replace missing values with the mode (lease frequent value).

for col in df.select_dtypes(include='object').columns:
    df[col] = df[col].fillna(df[col].value_counts().idxmin())

#     Drop rows where the time column has missing values (as it's critical data).

df = df.drop(df[df['time'].isnull()].index)

# Print the first 5 rows of the cleaned DataFrame.
print(df.head(5))

print(df.isna().sum())