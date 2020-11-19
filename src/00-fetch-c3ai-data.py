# coding=utf-8

#
# Data Pipeline Piece #0: Caching Data
# - Output: Cached population and COVID-19 for use in later scripts
#

import pandas as pd
import os

if not os.path.exists('data-cache'):
  os.makedirs('data-cache')


# C3.ai Data Link Requests
import c3aidatalake
today = pd.Timestamp.now().strftime("%Y-%m-%d")

for loc in ["Champaign_Illinois_UnitedStates", "Dane_Wisconsin_UnitedStates", "Franklin_Ohio_UnitedStates", "Tippecanoe_Indiana_UnitedStates"]:
  df_r1 = c3aidatalake.evalmetrics(
    "outbreaklocation",
    {
      "spec" : {
        "ids" : [loc],
        "expressions" : ["JHU_ConfirmedCases"],
        "start" : "2020-01-01",
        "end" : today,
        "interval" : "DAY",
      }
    }
  )

  df_r2 = c3aidatalake.evalmetrics(
    "outbreaklocation",
    {
      "spec" : {
        "ids" : [loc],
        "expressions" : ["CovidTrackingProject_ConfirmedCases"],
        "start" : "2020-01-01",
        "end" : today,
        "interval" : "DAY",
      }
    }
  )

  df_r1.to_csv(f'data-cache/r1-{loc}.csv')
  df_r1.to_csv(f'data-cache/r2-{loc}.csv')




# External Gits
os.chdir('data-cache')

if not os.path.exists("COVID-19"):
  os.system("git clone https://github.com/CSSEGISandData/COVID-19")
else:
  os.chdir('COVID-19')
  os.system("git pull")
  os.chdir('..')


if not os.path.exists("college-covid19-dataset"):
  os.system("git clone https://github.com/wadefagen/college-covid19-dataset")
else:
  os.chdir('college-covid19-dataset')
  os.system("git pull")
  os.chdir('..')

