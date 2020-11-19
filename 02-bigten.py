# coding=utf-8

#
# Data Pipeline Piece #2: Adding BigTen Data
# - Input: Formatted County Data (from `01`)
#           + College COVID-19 Dataset (from git or external)
# - Output: Formatted CSV merging population, county data, and university data, with one entry (day+county) per row
#


import pandas as pd
import os

# BigTen Data:
df = pd.read_csv("../website/pages/covid-19-at-big-ten-conference-schools/data/data-daily.csv")


df = df.rename(columns={
  "Country_Region": "University"
})
df.set_index("University", inplace=True)


countyData = [
  { "University": "Illinois", "CountyKey": "Champaign, Illinois, US", "FIPS": 17019},
  { "University": "UW-Madison", "CountyKey": "Dane, Wisconsin, US", "FIPS": 55025},
  { "University": "Ohio State", "CountyKey": "Franklin, Ohio, US", "FIPS": 39049},
  { "University": "Purdue", "CountyKey": "Tippecanoe, Indiana, US", "FIPS": 18157},
]

df_county = pd.DataFrame(countyData)
df_county.set_index("University", inplace=True)

df = df.join(df_county)
df = df.reset_index()

df["CountyKeyAndDate"] = df["CountyKey"] + " @ " + df["Date"]
df.set_index("CountyKeyAndDate", inplace=True)


# Merge w/ 01
df_cd = pd.read_csv("county-data.csv")
df = df.dropna()
df_cd["CountyKeyAndDate"] = df_cd["Admin2"] + ", " + df_cd["Province_State"] + ", US @ " + df_cd["Date"]
df_cd.set_index("CountyKeyAndDate", inplace=True)

df = df.join(df_cd, rsuffix="_County")

# Add state-level data




df.to_csv('county-and-college-v2.csv', index=False)
