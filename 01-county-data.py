# coding=utf-8

#
# Data Pipeline Piece #1: Formatting County Data
# - Input: Raw county-level COVID-19 data from c3.ai (cached in `data-cache`) 
#           + Population Data
# - Output: Formatted CSV merging population and county data, with one entry (day+county) per row
#

import os

path = 'data-cache/'  # csse_covid_19_daily_reports

stateTranslation = [
  ['Arizona', 'AZ'], ['Alabama', 'AL'], ['Alaska', 'AK'], ['Arkansas', 'AR'], ['California', 'CA'],
  ['Colorado', 'CO'], ['Connecticut', 'CT'], ['Delaware', 'DE'], ['Florida', 'FL'], ['Georgia', 'GA'],
  ['Hawaii', 'HI'], ['Idaho', 'ID'], ['Illinois', 'IL'], ['Indiana', 'IN'], ['Iowa', 'IA'],
  ['Kansas', 'KS'], ['Kentucky', 'KY'], ['Louisiana', 'LA'], ['Maine', 'ME'], ['Maryland', 'MD'],
  ['Massachusetts', 'MA'], ['Michigan', 'MI'], ['Minnesota', 'MN'], ['Mississippi', 'MS'], ['Missouri', 'MO'],
  ['Montana', 'MT'], ['Nebraska', 'NE'], ['Nevada', 'NV'], ['New Hampshire', 'NH'], ['New Jersey', 'NJ'],
  ['New Mexico', 'NM'], ['New York', 'NY'], ['North Carolina', 'NC'], ['North Dakota', 'ND'], ['Ohio', 'OH'],
  ['Oklahoma', 'OK'], ['Oregon', 'OR'], ['Pennsylvania', 'PA'], ['Rhode Island', 'RI'], ['South Carolina', 'SC'],
  ['South Dakota', 'SD'], ['Tennessee', 'TN'], ['Texas', 'TX'], ['Utah', 'UT'], ['Vermont', 'VT'],
  ['Virginia', 'VA'], ['Washington', 'WA'], ['West Virginia', 'WV'], ['Wisconsin', 'WI'], ['Wyoming', 'WY'],
  ['American Samoa', 'AS'], ['Guam', 'GU'], ['Northern Mariana Islands', 'MP'], ['Puerto Rico', 'PR'], ['Virgin Islands', 'VI'],
  ['District of Columbia', 'DC']
]


stateDict = {}

for el in stateTranslation:
  stateDict[ el[1] ] = el[0]




def processDate(fileName):
  date = fileName[0:10]
  dateFileName = fileName[0:10]

  datePieces = date.split("-")
  date = datePieces[2] + "-" + datePieces[0] + "-" + datePieces[1]
  print(date)


  df = pd.read_csv(path + dateFileName + ".csv")
  if "Admin2" not in df:
    return None

  if 'Country/Region' in df:
    df = df.rename(columns={
      'Country/Region': 'Country_Region',
      'Province/State': 'Province_State'
    })

  df = df[ df["Country_Region"] == "US" ]

  df = df[ ["Admin2", "Province_State", "Confirmed", "Deaths"] ] 
  df = df.astype({"Confirmed": "int32", "Deaths": "int32"})
  df["Date"] = date

  def createIndex(row):
    state = row["Province_State"]
    if "Admin2" in row:
      admin2 = row["Admin2"]
    else:
      admin2 = pd.np.NaN

    if pd.isnull(admin2):
      keyValue = state
    else:
      keyValue = admin2 + ", " + state

    return keyValue

  df["keyValue"] = df.apply(createIndex, axis=1)
  df = df.set_index('keyValue')
  return df



import pandas as pd
import os

files = os.listdir(path)

print("Reading Data...")
df_array = []
for i in range( len(files) ):
  f = files[i]
  if not f.endswith(".csv"): continue

  df = processDate( f )
  if df is None: continue

  df_array.append(df)


df = pd.concat(df_array)



def removeLastIndex(s):
  arr = s.split(" ")
  if arr[-1] == "County" or arr[-1] == "Borough" or arr[-1] == "Parish":
    arr = arr[:-1]
  elif arr[-1] == "Area" and arr[-2] == "Census":
    arr = arr[:-2]

  return " ".join(arr)


print("Processing - Loading Population")

df_pop = pd.read_csv("county_population.csv")
df_pop = df_pop[ df_pop["population"] > 0 ]
df_pop["State"] = df_pop["State"].replace(stateDict)
df_pop["County"] = df_pop["County Name"].apply(removeLastIndex)
df_pop["keyValue"] = df_pop["County"] + ", " + df_pop["State"]
df_pop.set_index("keyValue", inplace=True)


print("Processing - Adding Population")

df = df.join(df_pop)






print(df)
df.to_csv('county-data.csv', index=False)
