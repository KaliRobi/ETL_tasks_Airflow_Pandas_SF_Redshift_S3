import pandas as pd
from datetime import datetime

# Load the dataset
df = pd.read_csv("../data/captives.csv")  

# date columns to datetime
date_cols = ["date_of_birth", "sentence_begins", "sentence_expires"]
for col in date_cols:
    df[col] = pd.to_datetime(df[col], errors='coerce')

#  age
df["age"] = (datetime.today() - df["date_of_birth"]).dt.days // 365

# --- anomaly checks ---
anomalies = {}

# age validation
anomalies["negative_age"] = df[df["age"] < 0]
anomalies["unrealistic_age"] = df[df["age"] > 120]

# crime and sentence 
anomalies["missing_crime"] = df[(df["crime"].isna()) & (df["sentence_begins"].notna())]
anomalies["sentence_order_issue"] = df[df["sentence_begins"] > df["sentence_expires"]]
anomalies["prison_term_mismatch"] = df[df["prison_term_day"] != (df["sentence_expires"] - df["sentence_begins"]).dt.days]

# date 
anomalies["future_birthdates"] = df[df["date_of_birth"] > datetime.today()]
anomalies["future_sentences"] = df[df["sentence_begins"] > datetime.today()]

# locations
anomalies["missing_places"] = df[df["place_of_birth"].isna() | df["place_of_residence"].isna()]

# occupation and status consistency
anomalies["missing_occupation"] = df[df[["occupation", "occupation_2", "occupation_3"]].isna().all(axis=1)]
anomalies["child_number_invalid"] = df[df["number_of_children"].astype(str).str.isnumeric() == False]

#military service and education checks
anomalies["military_service_female"] = df[(df["sex"] == "n") & df["military_service"].notna()]
anomalies["missing_education"] = df[df["education"].isna()]

# financial and punishment validations
df["ransome"] = (
    df["ransome"]
    .astype(str)
    .str.replace(".", "", regex=False) 
    .str.replace(",", ".", regex=False) 
)
df["ransome"] = pd.to_numeric(df["ransome"], errors="coerce")

anomalies["negative_ransome"] = df[(df["ransome"].astype(float) < 0)]

#record integrity
anomalies["duplicate_ids"] = df[df.duplicated("id", keep=False)]
anomalies["duplicate_records"] = df[df.duplicated(["name", "date_of_birth", "crime"], keep=False)]

#logical anomalies
anomalies["underage_criminals"] = df[(df["age"] < 18) & df["crime"].notna()]
anomalies["early_release"] = df[df["sentence_expires"] < df["sentence_begins"]]

#combine all into a dataframe
quarantine_df = pd.concat(anomalies.values()).drop_duplicates()
quarantine_df.to_csv("quarantine.csv", index=False)

#log the anomalies
for check, records in anomalies.items():
    print(f"{check}: {len(records)} anomalies found")
