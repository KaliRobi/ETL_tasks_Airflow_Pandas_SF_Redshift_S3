import pandas as pd
from elasticsearch import Elasticsearch


raw_data = pd.read_csv('../data/captives.csv')
raw_data = raw_data.drop(['username', 'record_session', 'created_at'], axis=1)
int_data = pd.DataFrame()

# A. Age at Time of Sentence 
# Feature: Instead of just calculating age at the time of sentence, break it down by age groups.
# Purpose: This allows analysis by generational group and reveals how crime and sentencing patterns varied by age in different eras.

int_data[['sex', 'occupation_2']] = raw_data[['sex', 'occupation_2']]

# print(int_data.columns)

int_data = int_data[int_data['sex'].isin(['f', 'n'])]

int_data['age'] = ( pd.to_numeric(raw_data['id'].str[:4], errors='coerce' ) - 
                   pd.to_numeric(raw_data['date_of_birth'].str[:4], errors='coerce' )).round(0).fillna(0).astype(int)

int_data = int_data[(int_data['age'] > 6 )&
                     (int_data['age'] < 100
                     )]

group_by_sex = int_data.groupby('sex').count()

cohorts = sorted(int_data['age'].unique())

print(cohorts)


# B. Prison Term in Months/Years 
# Feature: Calculate the mean sentence duration for different crime types (e.g., theft, alcohol-related crimes, etc.).
# Purpose: Understand if certain crimes (like excise violations) led to significantly shorter or longer sentences compared to violent crimes.

# C. Repeat Offender Indicator
# Feature: Extend this by calculating the number of times an individual has been imprisoned.
# Sub-feature: Create a recidivism risk score based on their criminal history (number of prior arrests, types of previous crimes).
# Purpose: Research could focus on whether certain types of individuals (e.g., agricultural workers, illiterates) had higher rates of recidivism.