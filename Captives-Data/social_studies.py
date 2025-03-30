import pandas as pd
from elasticsearch import Elasticsearch


raw_data = pd.read_csv('data/captives.csv')
raw_data = raw_data.drop(['username', 'record_session', 'created_at'], axis=1)
int_data = pd.DataFrame()


# A. Age at Time of Sentence 
# Feature: Instead of just calculating age at the time of sentence, break it down by age groups.
# Purpose: This allows analysis by generational group and reveals how crime and sentencing patterns varied by age in different eras.

int_data[['sex', 'occupation_2']] = raw_data[['sex', 'occupation_2']]

# print(int_data.columns)

int_data = int_data[int_data['sex'].isin(['f', 'n'])]

int_data['commited_crime'] = raw_data['crime']
#int_data['commited_crime'] = raw_data['crime']

int_data['age'] = ( pd.to_numeric(raw_data['id'].str[:4], errors='coerce' ) - 
                   pd.to_numeric(raw_data['date_of_birth'].str[:4], errors='coerce' )).round(0).fillna(0).astype(int)

int_data = int_data[(int_data['age'] > 6 )&
                     (int_data['age'] < 100
                     )]

group_by_sex = int_data.groupby('sex').count()

cohorts = int_data['age'].unique()

ages = [0, 20, 40, 60, 80, 100]
labels = ["I", "II", "III", "IV", "V"]


int_data['cohort'] = pd.cut(int_data['age'], bins=ages, labels=labels)

# top 3 for each cohort and sex per crime

crime_counts = ( int_data.groupby(['cohort', 'sex'])['commited_crime']
                .value_counts()
                .groupby(level=[0,1]).head(3)
                .reset_index(name='count')
                )
#print(crime_counts)
# B. Prison Term in Months/Years 
# Feature: Calculate the mean sentence duration for different crime types (e.g., theft, alcohol-related crimes, etc.).
# Purpose: Understand if certain crimes (like excise violations) led to significantly shorter or longer sentences compared to violent crimes.

term_per_crime = pd.DataFrame()

term_per_crime[['crime','prison_term_day', 'sex', 'id']] = raw_data[['crime','prison_term_day', 'sex', 'id']]
term_per_crime.rename(columns={'prison_term_day': 'sentence_in_days'}, inplace=True)
term_per_crime = term_per_crime[
                 ~term_per_crime['sentence_in_days'].isin(['n.a', 'n.a.']) &
                 ~term_per_crime['sentence_in_days'].isna()

]
term_per_crime['sentence_in_days'] = term_per_crime['sentence_in_days'].astype(int)
term_per_crime['year'] = term_per_crime['id'].str.slice(start=0, stop=4)
term_per_crime.drop(columns=['id'], inplace=True)


#print(term_per_crime.groupby(['crime', 'sex'])['sentence_in_days'].mean().astype(int).sort_values())
term_per_crime.groupby(['crime', 'sex'])['sentence_in_days'].mean().astype(int).sort_values()


# C. Repeat Offender Indicator
# Feature: Extend this by calculating the number of times an individual has been imprisoned.
# Sub-feature: Create a recidivism risk score based on their criminal history (number of prior arrests, types of previous crimes).
# Purpose: Research could focus on whether certain types of individuals (e.g., agricultural workers, illiterates) had higher rates of recidivism.

#individual

returners = pd.DataFrame()

returners[['name', 'id', 'occupation_2', 'criminal_history', 'crime', 'date_of_birth', 'sex' ]] = raw_data[['name', 'id', 'occupation_2', 'criminal_history', 'crime', 'date_of_birth', 'sex' ]]

returners['date_of_birth'] =  pd.to_datetime(returners['date_of_birth'], errors='coerce' ,format='mixed' )
returners['date_of_birth'] = returners['date_of_birth'].dt.strftime('%Y-%m-%d')


returners = returners[
            ~returners['name'].isin(['n.a'])
]

returners = returners.groupby(['name', 'sex', 'date_of_birth', 'occupation_2' ])['id'].count().rename("crime_count").sort_values(ascending=False).reset_index()

# occupation categories where most of the repeaters came from

per_occupation = pd.DataFrame()
per_occupation =  returners[returners['crime_count'] > 1]


per_occupation.groupby('occupation_2')['name'].count().sort_values(ascending=False)