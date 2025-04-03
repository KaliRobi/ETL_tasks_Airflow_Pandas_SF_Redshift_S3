import pandas as pd

## **8. Compare demographics across different crimes**  
#- Group by `crime_severity` and analyze distributions for:  
#  - **Age** (Are younger or older people more likely to commit certain crimes?)  
#  - **Gender** (Are certain crimes gender-skewed?)  
#  - **Education level** (Are literacy and crime related?)  


raw_data = pd.read_csv('data/captives.csv')


crime_per_age = pd.DataFrame()


crime_per_age['age'] = (pd.to_numeric(raw_data['id'].str[:4], errors='coerce') -
                         pd.to_numeric(raw_data['date_of_birth'].str[:4], errors='coerce')).round(0).fillna(0).astype(int)


crime_per_age['age'] = (crime_per_age[(crime_per_age['age'] > 10) &
                        (crime_per_age['age'] < 100)])

crime_per_age[['crime','sex','education']] = raw_data[['crime','sex','education']]

print(crime_per_age.groupby(['crime', 'sex'])[['age']].count())