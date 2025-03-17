import pandas as pd
import numpy as np
import os
import datetime



esg = pd.read_csv(r'data/esg_scores.csv')
emissions = pd.read_csv('data/carbon_emissions.csv')

# empty values?

# print(emissions.isna().sum())
# print(esg.isna().sum())

# ESG Performance Report for Leadership

merge_table = esg.merge(emissions, on='company_id', how='left')
print(merge_table.columns)
# "We need an overview of our ESG scores and company distribution. Please provide:

#     The number of companies in each industry.

companies_per_industry = merge_table['company_id'].groupby(merge_table['industry']).count().reset_index()

# print(companies_per_industry)

#     The top 10 companies with the highest environmental scores.

top_10_in_environment_score = merge_table.sort_values('environment_score', ascending=False).head(10)

# print(top_10_in_environment_score)

#     A list of all energy sector companies rated A in ESG performance."_

energy_companies_A_rate = merge_table[(merge_table['esg_rating']== 'A') &
                                       (merge_table['industry'] == 'Energy') ]

# 2. Compliance & Risk Assessment

# "Our compliance team needs a report identifying companies that failed to meet ESG reporting requirements.
# Can you provide:

#     A list of non-compliant companies.
non_compilant_company = merge_table[merge_table['reporting_compliance'] == 'No']

# print(non_compilant_company)
#     Their ESG ratings and total emissions for the latest available year.

ncc_lst_year_emission = non_compilant_company[
    (non_compilant_company['year'] == 2023)
    ][['company_id','company_name','total_emissions_tons', 'esg_rating',  'year']].reset_index(drop=True)


# print(ncc_lst_year_emission)
# Any trends in compliance failures by industry."
# Are some industries consistently failing to report ESG data more often than others?
grouped_by_industry = merge_table.groupby(['industry'])['reporting_compliance']
industry_trend_on_reporting = grouped_by_industry.value_counts().unstack(fill_value=0)['No'] /  grouped_by_industry.count()  
least_reproting_industries =industry_trend_on_reporting.sort_values(ascending=False).head(int(np.ceil(industry_trend_on_reporting.count() / 2 )))

 
# Is non-compliance increasing or decreasing in specific industries over time?

sorted_by_time_idustry = merge_table[['reporting_compliance','industry', 'year']].groupby(['industry', 'year' ]).value_counts().unstack(fill_value=0).sort_values(['industry'])
sorted_by_time_idustry['total'] =  merge_table.groupby(['industry', 'year' ]).count()['company_id']
sorted_by_time_idustry['rate'] =  (sorted_by_time_idustry['No'] /  sorted_by_time_idustry['total']).round(2)
result_table = sorted_by_time_idustry.drop(columns='Yes')


# Are industries with low ESG scores also the ones failing compliance more frequently?


is_failing_equals_low_score = merge_table.groupby(['industry', 'esg_rating']).size().reset_index(name='count')
is_failing_equals_low_score['total'] = is_failing_equals_low_score.groupby('industry')['count'].transform('sum')# 

is_failing_equals_low_score['rate'] = (is_failing_equals_low_score['count'] / is_failing_equals_low_score['total']).round(2)

is_failing_equals_low_score = is_failing_equals_low_score.sort_values(by='rate', ascending=False)


print(is_failing_equals_low_score)
# 3. Industry-Wide ESG Benchmarking

# "We want to compare ESG performance across industries. Generate:

#     The average environmental score per industry.
#     The total sustainability budget per country.
#     Insights on whether any industries significantly lag behind in governance scores."_

# 4. Sustainability Trend Analysis

# "We’re analyzing how companies have progressed in renewable energy adoption. Please identify:

#     Companies whose renewable energy usage has increased over the years.
#     Whether companies with a net-zero target of 2030 have lower emissions than those targeting 2050.
#     Any industry-wide patterns in carbon emissions reduction."_

# 5. Investment & Strategy Insights

# "We need a new internal sustainability index to rank companies.
# Create a scoring system that considers:

#     High environmental scores
#     Low carbon emissions
#     High renewable energy usage
#     Higher sustainability budgets

# Rank companies based on this new metric and highlight any high-risk companies that score poorly."_
# 6. Anomaly Detection for Auditing

# "We suspect some companies are reporting inconsistent sustainability data. Please identify:

#     Companies with unusually high emissions compared to their industry average.
#     Any companies spending significantly less on sustainability than expected for their emissions levels.
#     Any companies with drastic ESG score changes that might indicate reporting inconsistencies."_