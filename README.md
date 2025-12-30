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

Was shocked that it worked! Tested manually and had two workflow failures due to file-not-found issues, corrected the issue and have had successful workflow runs ever since. The first official trading day of data was pulled on 12/19/2025. Due to my workflows running close to the holidays an unintentional issue came up. The schedule pulled data on Christmas Day, but since no trading took place, duplicate data was added to my Databricks Delta table.  

Additional duplicate data was also added due to manually executing the workflow and then allowing the scheduled run to execute, which resulted in data from 12/19/2025 being inserted twice. I manually deleted the duplicates from the table using PySpark. The next step in this project is to add logic to account for market holidays to improve the data pipeline. 


