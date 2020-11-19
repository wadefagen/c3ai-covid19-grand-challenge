#
# Data Pipeline Piece #4: LSTM Model
# - Input: Formatted university, county, and state data, with populations, via `03`
# - Output: LSTM Prediction Outputs
#


import pandas as pd
from numpy import array
import numpy as np

def rollForwardDate(date, amount=1):
  from datetime import datetime, timedelta

  dateObj = datetime.strptime(date, "%Y-%m-%d")
  dateObj = dateObj + timedelta(days=amount)
  return dateObj.strftime("%Y-%m-%d")


def toDelta7(arr):
  delta = []
  for i in range(len(arr) - 1):
    delta.append( arr[i + 1] - arr[i] )
  
  d7 = []
  rollingSum = 0
  rollingCount = 0
  for i in range(len(delta)):
    rollingSum += delta[i]
    if rollingCount != 7:
      rollingCount += 1
    else:
      rollingSum -= delta[i - 7]
    d7.append( rollingSum / rollingCount )

  return d7




def run(university, dateCutoffString):
  df = pd.read_csv('03-uni-county-state.csv')

  df = df[ df["University"] == university ]
  df = df[ df["Date"] <= dateCutoffString ]

  cases_uni = df["Confirmed"].values
  cases_county = df["Confirmed_County"].values
  cases_state = df["Confirmed_State"].values

  cases_state = (cases_state - cases_county)
  cases_county = (cases_county - cases_uni)

  cases_uni = toDelta7(cases_uni)
  cases_county = toDelta7(cases_county)
  cases_state = toDelta7(cases_state)




  uni_nf = np.linalg.norm(cases_uni)
  county_nf = np.linalg.norm(cases_county)
  state_nf = np.linalg.norm(cases_state)

  cases_uni = cases_uni / uni_nf
  cases_county = cases_county / county_nf
  cases_state = cases_state / state_nf

  from keras.models import Sequential
  from keras.layers import LSTM
  from keras.layers import Dense
  from numpy import hstack

  # split a multivariate sequence into samples:
  def split_sequences(sequences, n_steps):
    X, y = list(), list()
    for i in range(len(sequences)):
      end_ix = i + n_steps
      if end_ix > len(sequences)-1:
        break
      seq_x, seq_y = sequences[i:end_ix, :], sequences[end_ix, :]
      X.append(seq_x)
      y.append(seq_y)
    return array(X), array(y)  

  in_seq1 = cases_uni.reshape((len(cases_uni), 1))
  in_seq2 = cases_county.reshape((len(cases_county), 1))
  in_seq3 = cases_state.reshape((len(cases_state), 1))
  dataset = hstack((in_seq1, in_seq2, in_seq3))

  n_steps = 14
  X, y = split_sequences(dataset, n_steps)

  n_features = X.shape[2]


  # Build and compile an LSTM NN:
  model = Sequential()
  model.add(LSTM(100, activation='relu', return_sequences=True, input_shape=(n_steps, n_features)))
  model.add(LSTM(100, activation='relu'))
  model.add(Dense(n_features))
  model.compile(optimizer='adam', loss='mse')

  model.fit(X, y, epochs=1000, verbose=0)

  for i in range(14):
    z0 = cases_uni[-14:]
    z1 = cases_county[-14:]
    z2 = cases_state[-14:]

    z = []
    for j in range(14):
      z.append( [ z0[j], z1[j], z2[j] ] )

    z = array(z)
    z = z.reshape((1, n_steps, n_features))
    prediction = model.predict(z)
    prediction = prediction[0]

    cases_uni = list(cases_uni)
    cases_uni.append(prediction[0])

    cases_county = list(cases_county)
    cases_county.append(prediction[1])

    cases_state = list(cases_state)
    cases_state.append(prediction[2])


  cases_uni = array(cases_uni) * uni_nf
  cases_county = array(cases_county) * county_nf
  cases_state = array(cases_state) * state_nf

  date = df.iloc[1]["Date"]   # skip 0 due to delta calcs
  data = []
  for i in range(len(cases_uni)):
    if date <= dateCutoffString:
      status = "Actual"
    else:
      status = "Predicted"

    data.append({
      "Date": date,
      "Status": status,
      "University": cases_uni[i],
      "County": cases_county[i],
      "State": cases_state[i]
    })

    date = rollForwardDate(date)

  df_result = pd.DataFrame(data)
  df_result.to_csv(f'../results/{university}_{dateCutoffString}.csv', index=False)



#for uni in ["Illinois"]:
for uni in ["UW-Madison", "Ohio State", "Purdue", "Illinois"]:
  curDate = "2020-10-14"
  while curDate < "2020-11-15":
    print(f"{uni} @ {curDate}")
    run(uni, curDate)
    curDate = rollForwardDate(curDate)