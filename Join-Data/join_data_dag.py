import pandas as pd
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

companies_per_industry = merge_table['company_id'].groupby(merge_table['industry']).count()

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


print(ncc_lst_year_emission)
#     Any trends in compliance failures by industry."
                # Are some industries consistently failing to report ESG data more often than others?
                # Is non-compliance increasing or decreasing in specific industries over time?
                # Are industries with low ESG scores also the ones failing compliance more frequently?

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