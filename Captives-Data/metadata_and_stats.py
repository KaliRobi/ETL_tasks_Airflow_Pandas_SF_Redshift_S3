import pandas as pd
from geopy.geocoders import Nominatim
from elasticsearch import Elasticsearch


## sink to ELK

df = pd.read_csv('data/captives.csv', encoding='utf-8')


es = Elasticsearch('http://localhost:9200')
        
coords = pd.DataFrame()

query= {
    "size": 1000,
    "query":{
        "match_all": {}
   }
}

es_index = es.search(index="location_coordinates", body=query )

coords = pd.DataFrame( doc["_source"] for doc in es_index['hits']['hits'] )


# need the mapig

mapping = {
    "mappings":{
            "properties":{
                "volume" : {"type": "integer" },
                "id": {"type": "text" },
                "name" : {"type": "text" },
                "sex" : {"type": "keyword" },
                "height" : {"type": "integer" },
                "build" : {"type": "keyword" },
                "dentition": {"type": "keyword"},
                "special_peculiarities" :{"type": "text" },
                "age" : {"type": "integer" },
                "date_of_birth" : {"type": "text" },
                "place_of_birth" : {"type": "text" },
                "place_of_birth_coo" : {"type": "geo_point" },
                "place_of_residence" : {"type": "text" },
                "place_of_residence_coo" : {"type": "geo_point" },
                "residence" : {"type": "text" },
                "religion": {"type": "keyword"},
                "childhood_status" : {"type": "keyword"},
                "marital_status" : {"type": "keyword"},
                "number_of_children": {"type": "integer" },
                "occupation" : {"type": "keyword"},
                "occupation_2" : {"type": "keyword"},
                "occupation_3" : {"type": "keyword"},
                "military_service" :{"type": "text" },
                "literacy" : {"type": "keyword"},
                "education" : {"type": "keyword"},
                "criminal_history": {"type": "text" },
                "crime": {"type": "keyword"},
                "sentence_begins": {"type": "text" },
                "sentence_expires": {"type": "text" },
                "prison_term_day" : {"type": "integer" },
                "ransom" : {"type": "float" },
                "associates" : {"type": "text" },
                "degree_of_crime" : {"type": "text" },
                "degree_of_punishment" : {"type": "text" },
                "notes" : {"type": "text" },
                "arrest_site" : {"type": "text" }
           }
    }
}








#LIST OF COLUMNS
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
# age
df['age_at_hearing'] = (
    pd.to_numeric(df['id'].str[:4], errors='coerce') -
    pd.to_numeric(df['date_of_birth'].str[:4], errors='coerce')).round(0).fillna(0).astype(int)

# date_of_birth             object
df['date_of_birth'] = pd.to_datetime(df['date_of_birth'].str.strip().str.replace(r'.', '-'), errors='coerce')


# place_of_birth            object
df = df.merge(coords, left_on="place_of_birth", right_on="location_name", how="left")
df = df.rename(columns={"location": "place_of_birth_coo"}).drop(columns=["location_name",   "longitude",   "latitude"])

# coordinates



# place_of_residence        object

df = df.merge(coords, left_on='place_of_residence', right_on='location_name', how='left')
df = df.rename(columns={"location": "place_of_residence_coo"}).drop(columns=["location_name",   "longitude",   "latitude"])


# residence                 object

print(df['residence'])
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

# prison_term_day           object
df['prison_term_day'] = pd.to_numeric(df['prison_term_day'], errors='coerce')

# ransom                   object
df['ransome'] = pd.to_numeric(df['ransome'], errors='coerce').fillna(0)
df.rename(columns={"ransome": "ransom"}, inplace=True)

# print(df['ransom'])
# associates                object
# degree_of_crime           object
# degree_of_punishment      object
# notes                     object
# arrest_site               object

# THEY ARE NOT NEEDED FOR TIS PROJECT
# username                 float64
# record_session           float64
# created_at               float64
df.drop(['record_session', 'username', 'created_at' ], axis=1)



