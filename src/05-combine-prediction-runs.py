#
# Data Pipeline Piece #5: Data Formatting
# - Input: LSTM Prediction Outputs
# - Output: `combined_results` for further analysis (CSV files)
#


import pandas as pd

from os import listdir
from os.path import isfile, join

path = "results"
resultFileNames = [f for f in listdir(path) if isfile(join(path, f))]


universityResults = {}

for fileName in resultFileNames:
  fullPath = join(path, fileName)
  
  fileNamePieces = fileName[:-4].split("_")
  university = fileNamePieces[0]
  date = fileNamePieces[1]

  df = pd.read_csv(fullPath, index_col="Date")

  if university not in universityResults:
    universityResults[university] = df
    universityResults[university] = universityResults[university].join(df, rsuffix="_" + date, how="right")
  else:
    universityResults[university] = universityResults[university].join(df, rsuffix="_" + date, how="right")


for university in universityResults:
  df = universityResults[university]
  cols = sorted(list(df.columns.values))
  df = df[ cols ]
  df.to_csv(f"../combined_results/v2_{university}.csv")




def rollForwardDate(date, amount=1):
  from datetime import datetime, timedelta

  if amount == 0: return date

  dateObj = datetime.strptime(date, "%Y-%m-%d")
  dateObj = dateObj + timedelta(days=amount)
  return dateObj.strftime("%Y-%m-%d")


for university in universityResults:
#for university in ["Illinois"]:
  df = universityResults[university]
  print(university)

  curDate = "2020-10-14"
  agg = {}
  df_data = []

  while curDate <= "2020-11-14":
    agg[curDate] = {}

    for predictionDaysForward in range(1, 14+1):
      predictDate = rollForwardDate(curDate, predictionDaysForward)
      print(f"On {curDate}, the prediction of {predictDate}:")

      daysToLookBack = predictionDaysForward
      if daysToLookBack > 3: daysToLookBack = 3
      data_sum = 0
      data_ct = 0
      for deltaBackwards in range(0, daysToLookBack):
        dataDate = rollForwardDate(curDate, -deltaBackwards)
        try:
          value = df[f"County_{dataDate}"].loc[predictDate]

          numInclude = 3 - deltaBackwards
          print(f"+ On {dataDate}, the prediction for {predictDate} is: {value}. (Weight: {numInclude})")

          import math
          if not math.isnan(value):
            data_sum += numInclude * value
            data_ct += numInclude
        except:
          a = 1
          # nothing

      if data_ct > 0:
        prediction = data_sum / data_ct
        try:
          actual = df[f"County_{predictDate}"].loc[predictDate]
          error = prediction - actual
          errorPct = error / actual
        except:
          actual = error = errorPct = "?"

        print(f"= Prediction: {prediction} vs {actual}")
        agg[curDate][predictDate] = {
          "currentDate": curDate,
          "predictionDate": predictDate,
          "daysForward": predictionDaysForward,
          "predictionValue": prediction,
          "actualValue": actual,
          "error": error,
          "errorPct": errorPct
        }
        df_data.append(agg[curDate][predictDate])

    curDate = rollForwardDate(curDate, 1)

  # Raw Results:
  pd.DataFrame(df_data).to_csv(f"../combined_results/computed-predictions-{university}.csv")

  # For Graphs:
  curDate = "2020-11-01"
  df_chart = []
  labels = []
  while curDate <= "2020-11-14":
    d = {}
    for i in range(1, 15):
      if i < 10:
        deltaStr = f"D-0{i}"
      else:
        deltaStr = f"D-{i}"

      if deltaStr not in labels: labels.append(deltaStr)
      deltaDate = rollForwardDate(curDate, -i)

      # deltaDate's prediction for curDate:
      predictionValue = agg[deltaDate][curDate]["errorPct"]
      print(f"{university} {deltaDate} prediction of {curDate} ({deltaStr}): {predictionValue}")

      d[deltaStr] = predictionValue

    d["Date"] = curDate

    df_chart.append(d)
    curDate = rollForwardDate(curDate, 1)

  labels = sorted(labels, reverse=True)
  labels.insert(0, "Date")
  pd.DataFrame(df_chart).to_csv(f"../combined_results/chart-{university}.csv", columns=labels)

