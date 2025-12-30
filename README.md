# GitHub Actions Stock Pipeline
## Overview

For this project my objective was to create a data pipeline where I pull daily stock prices from yfinance and push the data into a Delta table in my data warehouse within Databricks. Up to this point I would manually pull stock data from yfinance, analyze with pandas, create visualizations, and then save my Jupyter notebook. For historical analysis this was ok, but for daily analysis this was an issue as my data was static and would become outdated very quickly. 

I also wanted to develop an ETL process that was free to run, so using GitHub Actions along with the free edition of Databricks was the best option for me. Ultimately, I would like to use this data to create real-time candlestick charts for day trading purposes.

## Key Features

**GitHub Actions**
- Runs on a schedule (cron) via a YAML file (weekdays only)
- Executes the `pull_stock_data.py` script, which sends a request to yfinance to pull daily stock prices
- Pushes the data into Databricks via the Databricks REST API

## Results

Was shocked that it worked! Tested manually and had 2 workflow failures due to file not found issues, corrected the issue and have had successful workflow runs ever sense. The first official trading day data was pulled from was on 12/19/2025. Due to my workflows running close to the holidays an unintentional issue came up. My schedule pulled data on Christmas day, but no trading took place on that day so duplicate data was added to my Databricks Delta table.  

Also had some duplicate data added do to manually executing my first workflow and then the schedule running as it should which added data from 12/19/2025 twice. I went ahead and manually deleted the duplicates from my table using PySpark. The next step in this project is to add logic to account for market holidays to improve the data pipeline. 


