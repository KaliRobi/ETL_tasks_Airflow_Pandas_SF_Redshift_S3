import pandas as pd
from elasticsearch import Elasticsearch, helpers
import numpy as np

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


# need the mapping

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
                "place_of_birth_geo_point" : {"type": "geo_point" },
                "place_of_residence" : {"type": "text" },
                "place_of_residence_geo_point" : {"type": "geo_point" },
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


es.indices.create(index="captives_index", body=mapping, ignore=400)



def process_dataframe(df, index_name):
    df = df.replace({np.nan: None, pd.NaT: None})
    for _, row in df.iterrows():
        yield{
            "_index": index_name,
            "_source": {
                "volume" : row["volume"] ,
                "id": row["id"] ,
                "name" : row["name"] ,
                "sex" : row["sex"] ,
                "height" : row["height"] ,
                "build" : row["build"] ,
                "dentition": row["dentition"] ,
                "special_peculiarities" :row["special_peculiarities"] ,
                "age" : row["age"] ,
                "date_of_birth" : row["date_of_birth"] ,
                "place_of_birth" : row["place_of_birth"] ,
                "place_of_birth_geo_point" : {
                    "lat": row["place_of_birth_latitude"], 
                    "lon": row["place_of_birth_longitude"]
                }, 
                "place_of_residence" : row["place_of_residence"],
                "place_of_residence_geo_point" : {
                    "lat": row["place_of_residence_latitude"],
                    "lon": row["place_of_residence_longitude"]
                }, 
                "residence" : row["residence"] ,
                "religion": row["religion"] ,
                "childhood_status" : row["childhood_status"] ,
                "marital_status" : row["marital_status"] ,
                "number_of_children": row["number_of_children"] ,
                "occupation" : row["occupation"] ,
                "occupation_2" : row["occupation_2"] ,
                "occupation_3" : row["occupation_3"] ,
                "military_service" : row["military_service"] ,
                "literacy" : row["literacy"] ,
                "education" : row["education"] ,
                "criminal_history": row["criminal_history"] ,
                "crime": row["crime"] ,
                "sentence_begins": row["sentence_begins"] ,
                "sentence_expires": row["sentence_expires"] ,
                "prison_term_day" : row["prison_term_day"] ,
                "ransom" : row["ransom"] ,
                "associates" : row["associates"] ,
                "degree_of_crime" : row["degree_of_crime"] ,
                "degree_of_punishment" : row["degree_of_punishment"] ,
                "notes" : row["notes"] ,
                "arrest_site" : row["arrest_site"] 
            }
        }



#LIST OF COLUMNS NEED TRANSFORMATION

# height               
df['height'] = df['height'].replace('n.a', '')
df['height'] = pd.to_numeric(df['height'])

# adding age, in some cases it was not possible to figure out the month and day of the month of the birthdate
# age
df['age'] = (
    pd.to_numeric(df['id'].str[:4], errors='coerce') -
    pd.to_numeric(df['date_of_birth'].str[:4], errors='coerce')).round(0).fillna(0).astype(int)

# date_of_birth             
df['date_of_birth'] = pd.to_datetime(df['date_of_birth'].str.strip().str.replace(r'.', '-'), errors='coerce')

# place_of_birth
# 
df["place_of_birth"]  =     df["place_of_birth"].str.slice()
df = df.merge(coords, left_on="place_of_birth", right_on="location_name", how="left")
df = df.rename(columns={"longitude": "place_of_birth_longitude", "latitude":  "place_of_birth_latitude" }
               ).drop(columns=["location_name", "location"])

# place_of_residence   
df["place_of_residence"]  =  df["place_of_residence"].str.slice()     
df = df.merge(coords, left_on='place_of_residence', right_on='location_name', how='left')
df = df.rename(columns={"longitude": "place_of_residence_longitude", "latitude": "place_of_residence_latitude" }
               ).drop(columns=["location_name", "location"])

#temporary as 0,0 is a valid coordinata
df["place_of_birth_longitude"].fillna(0, inplace=True)
df["place_of_birth_latitude"].fillna(0, inplace=True)
df["place_of_residence_longitude"].fillna(0, inplace=True) 
df["place_of_residence_latitude"].fillna(0, inplace=True) 



# number_of_children
df['number_of_children'] = pd.to_numeric(df['number_of_children'].str.replace('n.a', ''))

# sentence_begins           
df['sentence_begins'] = pd.to_datetime(df['sentence_begins'].str.strip().str.replace('n.a', '').str.replace('-', '.'), errors='coerce')

# sentence_expires          
df['sentence_expires'] = pd.to_datetime(df['sentence_expires'].str.strip().str.replace('n.a', '' ).str.replace('-', '.'), errors='coerce'  )

# prison_term_day           
df['prison_term_day'] = pd.to_numeric(df['prison_term_day'], errors='coerce')

# ransom                   
df['ransome'] = pd.to_numeric(df['ransome'], errors='coerce').fillna(0)
df.rename(columns={"ransome": "ransom"}, inplace=True)


# THEY ARE NOT NEEDED FOR THIS PROJECT
# username, record_session, created_at               
df.drop(['record_session', 'username', 'created_at' ], axis=1)


helpers.bulk(es, process_dataframe(df, "captives_index"),
             ignore_status=[400, 404],
              raise_on_error=False)