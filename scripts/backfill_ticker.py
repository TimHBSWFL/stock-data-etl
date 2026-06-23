#%%
# One-off backfill script for a full trading date missed by the daily pipeline
# (e.g. token expiry, outage, etc.). Set BACKFILL_DATE and run manually.
import os
import requests
import yfinance as yf
import pandas as pd
from datetime import datetime, timezone

#%%
# -- Edit this for whatever date you're backfilling --
BACKFILL_DATE = "2026-06-22"

#%%
file_path = "files/sp500_watchlist.csv"
df = pd.read_csv(file_path)
tickers = df['tickers'].tolist()

#%%
# yfinance end date is exclusive, so add one day to capture BACKFILL_DATE
start = BACKFILL_DATE
end = str(pd.Timestamp(BACKFILL_DATE) + pd.Timedelta(days=1))[:10]

data = yf.download(
    tickers=tickers,
    start=start,
    end=end,
    interval='1d',
    group_by='ticker',
    auto_adjust=True,
    threads=False
)

if data.empty:
    print(f"No data returned for {BACKFILL_DATE}. Exiting.")
    exit(0)

#%%
rows = []
run_ts = datetime.now(timezone.utc)

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
        "trade_date": BACKFILL_DATE,
        "open": float(row["Open"]),
        "high": float(row["High"]),
        "low": float(row["Low"]),
        "close": float(row["Close"]),
        "volume": int(row["Volume"]),
        "run_ts": run_ts
    })

df_backfill = pd.DataFrame(rows)
print(f"Backfilling {len(df_backfill)} tickers for {BACKFILL_DATE}")

if df_backfill.empty:
    print("No data to insert. Exiting.")
    exit(0)

#%%
values_sql = ",".join([
    f"('{r.ticker}', '{r.trade_date}', {r.open}, {r.high}, {r.low}, {r.close}, {r.volume}, TIMESTAMP '{r.run_ts.strftime('%Y-%m-%d %H:%M:%S.%f')}')"
    for r in df_backfill.itertuples()
])

sql = f"""
MERGE INTO analytics.stock_prices AS target
USING (
  SELECT ticker, trade_date, open, high, low, close, volume, run_ts
  FROM VALUES {values_sql}
  AS source(ticker, trade_date, open, high, low, close, volume, run_ts)
)
ON target.ticker = source.ticker AND target.trade_date = source.trade_date
WHEN MATCHED THEN UPDATE SET
  open = source.open,
  high = source.high,
  low = source.low,
  close = source.close,
  volume = source.volume,
  run_ts = source.run_ts
WHEN NOT MATCHED THEN INSERT (ticker, trade_date, open, high, low, close, volume, run_ts)
VALUES (source.ticker, source.trade_date, source.open, source.high, source.low, source.close, source.volume, source.run_ts)
"""

#%%
DATABRICKS_HOST = os.environ["DATABRICKS_HOST"]
DATABRICKS_TOKEN = os.environ["DATABRICKS_TOKEN"]
WAREHOUSE_ID = os.environ["DATABRICKS_WAREHOUSE_ID"]

url = f"{DATABRICKS_HOST}/api/2.0/sql/statements"
headers = {"Authorization": f"Bearer {DATABRICKS_TOKEN}"}
payload = {"statement": sql, "warehouse_id": WAREHOUSE_ID}

response = requests.post(url, json=payload, headers=headers)

if response.status_code != 200:
    print(response.text)
    response.raise_for_status()
else:
    print(f"Backfill for {BACKFILL_DATE} succeeded.")
