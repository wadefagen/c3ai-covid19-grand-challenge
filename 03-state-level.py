#
# Data Pipeline Piece #3: Adding State-level Data
# - Input: Formatted County + University Data (from `02`)
#           + State-level COVID-19 data
# - Output: Formatted CSV merging population, state data, county data, and university data, with one entry (day+county) per row
#

import pandas as pd
import os

df = pd.read_csv("county-and-college-v2.csv")
df["KeyValue"] = df["Province_State"] + " @ " + df["Date"]
df.set_index("KeyValue", inplace=True)

path = 'data-cache/'  # csse_covid_19_daily_reports_us


def processUSDailyReport(date):
  df = pd.read_csv(path + date + ".csv")

  if 'Total_Test_Results' in df:
    df = df.rename(columns={
      'Total_Test_Results': 'People_Tested'
    })

  # Re-arrange date to YYYY-MM-DD format:
  datePieces = date.split("-")
  date = datePieces[2] + "-" + datePieces[0] + "-" + datePieces[1]
  print("US - " + date)

  df = df[ ["Province_State", "People_Tested", "Confirmed",	"Deaths", "FIPS"] ] 
  df["Date"] = date

  return df



df_array = []
for filename in os.listdir(path):
  if not filename.endswith(".csv"): continue
  date = filename[0:10]
  df_array.append(processUSDailyReport(date))

df_statedata = pd.concat(df_array)
df_statedata["KeyValue"] = df_statedata["Province_State"] + " @ " + df_statedata["Date"]
df_statedata.set_index("KeyValue", inplace=True)

df = df.join(df_statedata, rsuffix="_State")


df.to_csv('03-uni-county-state.csv', index=False)
