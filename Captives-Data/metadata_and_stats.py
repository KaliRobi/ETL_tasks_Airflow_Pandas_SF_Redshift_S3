import pandas as pd
from geopy.geocoders import Nominatim
from elasticsearch import Elasticsearch

## **1. Extract the captives dataset**  
# Load the dataset from the CSV file into a DataFrame.

df = pd.read_csv('data/captives.csv', encoding='utf-8')


def location_coordinates(location):

    loc = Nominatim(user_agent="GetLoc")
    coordinates = []
    
    try:

        getLoc = loc.geocode(location)
        coordinates.append(getLoc.longitude)
        coordinates.append(getLoc.latitude)
        print(coordinates)
        return coordinates
        
    except:
        print([0,0])
        return [0,0]
        
coords = pd.DataFrame()

coords['locations'] = df['date_of_birth'].unique()

coords[['place_of_birth_lon', 'place_of_birth_lat']] = coords['locations'].apply(location_coordinates)


# print(df.columns)
# Ensure all columns have appropriate data types (e.g., dates as datetime, numbers as int/float).

# volume                     int64
# id                        object
# name                      object
# sex                       object
# height                   float64
df['height'] = df['height'].replace('n.a', '')
df['height'] = pd.to_numeric(df['height'])
# build                     object
# dentition                 object
# special_peculiarities     object

# adding age, in some cases it was not possible to figure out the month and day of the month of the birthdate
# date_of_birth, "1933-801"
df['age_at_hearing'] = (
    pd.to_numeric(df['id'].str[:4], errors='coerce') -
    pd.to_numeric(df['date_of_birth'].str[:4], errors='coerce')).round(0).fillna(0).astype(int)

# date_of_birth             object
df['date_of_birth'] = pd.to_datetime(df['date_of_birth'].str.strip().str.replace(r'.', '-'), errors='coerce')

print(df['age_at_hearing'])
# place_of_birth            object


# coordinates
df[['place_of_birth_lon', 'place_of_birth_lat']] = df['place_of_birth'].apply(location_coordinates).apply(pd.Series)


# place_of_residence        object
# residence                 object
# religion                  object
# childhood_status          object
# marital_status            object
# number_of_children        object
df['number_of_children'] = pd.to_numeric(df['number_of_children'].str.replace('n.a', ''))

# occupation                object
# occupation_2              object
# occupation_3              object
# military_service          object
# literacy                  object
# education                 object
# criminal_history          object
# crime                     object
# sentence_begins           object
df['sentence_begins'] = pd.to_datetime(df['sentence_begins'].str.strip().str.replace('n.a', '').str.replace('-', '.'), errors='coerce')

# sentence_expires          object
df['sentence_expires'] = pd.to_datetime(df['sentence_expires'].str.strip().str.replace('n.a', '' ).str.replace('-', '.'), errors='coerce'  )
print(df['sentence_expires'] )
# prison_term_day           object
df['prison_term_day'] = pd.to_numeric(df['prison_term_day'], errors='coerce')

# ransom                   object
df['ransome'] = pd.to_numeric(df['ransome'], errors='coerce').fillna(0)
df.rename(columns={"ransome": "ransom"}, inplace=True)

print(df['ransom'])
# associates                object
# degree_of_crime           object
# degree_of_punishment      object
# notes                     object
# arrest_site               object



# username                 float64
# record_session           float64
# created_at               float64
df.drop(['record_session', 'username', 'created_at' ], axis=1)





# Identify and handle missing values, duplicates, and potential inconsistencies in the data.
# Display basic statistics and metadata, such as column types, unique values, and descriptive statistics.  