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
            "_source": row.to_dict()
        }


dfp['locations'] = df['place_of_residence'].unique()

coords['locations'] = df['place_of_birth'].unique()

# concat all values where location data is present
coords['locations'] = pd.concat([dfp['locations'].str.strip(),coords['locations'].str.strip()] ).drop_duplicates().reset_index(drop=True)

#apply the function
coords[['place_of_birth_lon', 'place_of_birth_lat']] = coords['locations'].apply(location_coordinates).apply(pd.Series)

#create bulk format        
def df_to_elastic(df, index_name):
    for _, row in df.iterrows():
        yield {
            "_index": index_name,
            "_source": row.to_dict()
        }

mappings = {
    "mappings": {
        "settings": {"number_of_shards": 1, "number_of_replicas": 0},
        "properties": {
            "location": {"type": "keyword"},
            "longitude": {"type": "float"},
            "latitude": {"type": "float"}
        }
    }
}

#create a new index
es.indices.create(index="location_coordinates", body=mappings, ignore=400)

helpers.bulk(es, df_to_elastic(coords, "location_coordinates" ))
