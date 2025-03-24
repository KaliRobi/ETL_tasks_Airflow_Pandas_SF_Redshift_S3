import pandas as pd
from geopy.geocoders import Nominatim
from elasticsearch import Elasticsearch, helpers

# setup
df = pd.read_csv('data/captives.csv', encoding='utf-8')
es = Elasticsearch('http://localhost:9200')
coords = pd.DataFrame()
dfp = pd.DataFrame()

# retrun coordinates
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

#create bulk format        
def df_to_elastic(df, index_name):
    for _, row in df.iterrows():
        yield {
            "_index": index_name,
            "_source": {
                "location_name": row["location_name"],
                "longitude": row["longitude"],
                "latitude": row["latitude"],
                "location": {  
                    "lat": row["latitude"],
                    "lon": row["longitude"]
                }
            }
        }


dfp['location_name'] = df['place_of_residence'].unique()

coords['location_name'] = df['place_of_birth'].unique()

# concat all values where location data is present
coords['location_name'] = pd.concat([dfp['location_name'].str.strip(),coords['location_name'].str.strip()] ).drop_duplicates().reset_index(drop=True)

#apply the function
coords[['longitude', 'latitude']] = coords['location_name'].apply(location_coordinates).apply(pd.Series)

coords['location']= coords.apply(lambda row: (row['longitude'], row['latitude']), axis=1)
#create bulk format        
def df_to_elastic(df, index_name):
    for _, row in df.iterrows():
        yield {
            "_index": index_name,
            "_source": row.to_dict()
        }

mappings = {
    "mappings": {
        "properties": {
            "location_name": {"type": "keyword"},
            "longitude": {"type": "float"},
            "latitude": {"type": "float"},
            "location": {"type": "geo_point" }
        }
    }
}

#create a new index
es.indices.create(index="location_coordinates", body=mappings, ignore=400)

helpers.bulk(es, df_to_elastic(coords, "location_coordinates" ))
