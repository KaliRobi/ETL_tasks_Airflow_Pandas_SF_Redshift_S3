import pandas as pd

# Goal: Approximate surface gravity for exoplanets using the formula:
# g=GMR2
# g=R2GM​

# where:

#     GG (gravitational constant) = 6.674 × 10⁻¹¹

#     MM = planet_mass_jupiter_units * 1.898 × 10²⁷ kg

#     RR = planet_radius_earth_units * 6.371 × 10⁶ m

# Steps:

#     Compute surface_gravity_m_s2 using the formula.

#     Compare with Earth's gravity (9.81 m/s²) by adding a column:

#         is_higher_gravity_than_earth (True/False).

#     Show the top 10 strongest gravity planets.




df = pd.read_csv('data/exoplanet_data.csv')

print(df.columns)

grouped_by_stars = df.groupby()