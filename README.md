# C3.ai Grand Challenge Submission (91-DIVOC)

This repo contains the source code for Team 91-DIVOC's submission to the [C3.ai Grand Challenge](https://c3.ai/c3-ai-covid-19-grand-challenge/) by Wade Fagen-Ulmschneider and Karle Flanagan.

## Abstract

Throughout the world, areas of high-frequency surveillance testing for COVID-19 have emerged in small, tightly-knit communities such as university campuses.  For the c3.ai grand challenge, we created a new, public dataset with daily COVID-19 testing statistics of all Big Ten University COVID-19 testing programs; used the C3.ai Data Lake to gather time-series data on county, state, and national-levels; and employed a neural network to accurately predict the new cases of COVID-19 within the full county communities of The University of Illinois, Purdue University, and The Ohio State University with minimal error up to seven days into the future.

## Source Code

The source code for this submission is made up of six files that create a data pipeline.  Each file runs independently, but depends on output created by the sequentially numbered previous file.

- Step 0 (`src/00-fetch-c3ai-data.py`): Fetch Data from C3.ai Data Lake related to county-level and state-level COVID-19 cases and testing.  This data is cached locally as a CSV file used in several future steps.
- Step 1 (`src/01-county-data.py`): Combine the county-level COVID-19 data from [Step 0] with US Census data on the population of each county in the U.S.
- Step 2 (`src/02-bigten.py`): Combine the data from [Step 1] with University data from all target universities, using our College COVID-19 dataset.
- Step 3 (`src/03-state-level.py`): Combine the data from [Step 2] with state-level data from the C3.ai Data Lake to create a complete dataset containing university-level, county-level, state-level data on COVID-19 cases, testing, and populations.
- Step 4 (`src/04-predict.py`): Format the data from [Step 3] as time-series vectors for use in an LSTM neural network. After the network is trained, predictions are made for future cases of COVID-19.  (Specific details on our model provided in the “Technical Details” section of this paper.)
- Step 5 (`src/05-combine-prediction-runs.py`): Processing of model outputs from [Step 4] into various CSV results files for analysis and visualization.

### Running the Data Pipeline

To run the source code manually, navigate into `src` and run the files sequentially:

```
cd src
python 00-fetch-c3ai-data.py  # Fetches data and clones git repos, slow
python 01-county-data.py
python 02-bigten.py
python 03-state-level.py
python 04-predict.py  # Trains an LSTM model, very slow
python 05-combine-prediction-runs.py
```

## Sample Results

The following image appears in Figure 2 in our paper, created from the data in `combined_results/computed-predictions-Ohio State.csv`:

![Results for OSU](https://github.com/wadefagen/c3ai-covid19-grand-challenge/blob/main/img/osu.png)

