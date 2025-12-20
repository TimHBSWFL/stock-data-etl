#%%
import os
import requests
import yfinance as yf
import pandas as pd
from datetime import datetime, UTC

#%%
file_path = "files/sp500_watchlist.csv"

df = pd.read_csv(file_path)
tickers = df['tickers'].tolist()

# -- Download stock data --
data = yf.download(
    tickers=tickers,
    period='1d',
    interval='1d',
    group_by='ticker',
    auto_adjust=True,
    threads=False
)

#%%
rows = []
run_ts = datetime.now(UTC)

for ticker in tickers:
    if ticker not in data:
        print(f"Skipping {ticker}, no data found")
        continue
    df_ticker = data[ticker].dropna(how="all")
    if df_ticker.empty:
        print(f"Skipping {ticker}, empty dataframe")
        continue
    
    row = df_ticker.iloc[-1]
    
    rows.append({
        "ticker": ticker,
        "trade_date": row.name.date(),
        "open": float(row["Open"]),
        "high": float(row["High"]),
        "low": float(row["Low"]),
        "close": float(row["Close"]),
        "volume": int(row["Volume"]),
        "run_ts": run_ts
    })
    
df_stocks = pd.DataFrame(rows)

if df_stocks.empty:
    print("No stock data to insert, exiting.")
    exit(0)

# %%
# -- Build SQL Insert ----
values_sql = ",".join([
    f"('{r.ticker}', '{r.trade_date}', {r.open}, {r.high}, {r.low}, {r.close}, {r.volume}, TIMESTAMP '{r.run_ts.strftime('%Y-%m-%d %H:%M:%S.%f')}')"
    for r in df_stocks.itertuples()
])

# %%
sql = f"""
INSERT INTO analytics.stock_prices 
(ticker, trade_date, open, high, low, close, volume, run_ts)
VALUES {values_sql}
"""
# %%
# -- Databricks SQL API Call --
# ---- Config from GitHub Secrets ----
DATABRICKS_HOST = os.environ["DATABRICKS_HOST"]
DATABRICKS_TOKEN = os.environ["DATABRICKS_TOKEN"]
WAREHOUSE_ID = os.environ["DATABRICKS_WAREHOUSE_ID"]


url = f"{DATABRICKS_HOST}/api/2.0/sql/statements"
headers = {
    "Authorization": f"Bearer {DATABRICKS_TOKEN}"
    }

payload = {
    "statement": sql,
    "warehouse_id": WAREHOUSE_ID
}

response = requests.post(url, json=payload, headers=headers)

if response.status_code != 200:
    print(response.text)
    response.raise_for_status()
