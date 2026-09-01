#%%
import os
import sys
import numpy as np
import yfinance as yf
import pandas as pd
from datetime import datetime, timezone
import pandas_market_calendars as mcal

# Running the file as a script puts scripts/ on the path automatically; this
# keeps the import working when the cells are run from the repo root.
sys.path.append(os.path.join(os.getcwd(), "scripts"))
from databricks_sql import execute_merge

#%%
file_path = "files/sp500_watchlist.csv"

df = pd.read_csv(file_path)
tickers = df['tickers'].tolist()

#%%
# NYSE calendar
nyse = mcal.get_calendar('NYSE')

# Determine the last completed trading session.
# period='1d' returns intraday data if the market is still open, so we
# explicitly target the most recent session that has closed.
now_et = pd.Timestamp.now(tz="America/New_York")
market_close = now_et.normalize() + pd.Timedelta(hours=16)

sessions = nyse.valid_days(
    start_date=now_et.date() - pd.Timedelta(days=7),
    end_date=now_et.date()
)

if sessions.empty:
    print("No recent trading sessions found. Exiting.")
    exit(0)

# Use today's session only if market has closed; otherwise use the previous session
if now_et >= market_close and sessions[-1].date() == now_et.date():
    target_date = now_et.date()
else:
    completed = [s for s in sessions if s.date() < now_et.date()]
    if not completed:
        print("No completed trading session available yet. Exiting.")
        exit(0)
    target_date = completed[-1].date()

print(f"Targeting trading date: {target_date}")

#%%
# -- Download stock data --
start = str(target_date)
end = str(target_date + pd.Timedelta(days=1))

data = yf.download(
    tickers=tickers,
    start=start,
    end=end,
    interval='1d',
    group_by='ticker',
    auto_adjust=True,
    threads=False
)

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

    # A partial row from Yahoo would otherwise reach the SQL as a bare `nan`
    # literal and fail the whole MERGE.
    ohlcv = pd.to_numeric(row[["Open", "High", "Low", "Close", "Volume"]], errors="coerce")
    if not np.isfinite(ohlcv).all():
        print(f"Skipping {ticker}, incomplete OHLCV data")
        continue

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
df_stocks.drop_duplicates(subset=['ticker', 'trade_date'], inplace=True)

if df_stocks.empty:
    print("No stock data to insert, exiting.")
    exit(0)

# %%
# -- Build SQL values for MERGE --
def build_merge_sql(batch):
    values_sql = ",".join([
        f"('{r.ticker}', '{r.trade_date}', {r.open}, {r.high}, {r.low}, {r.close}, {r.volume}, TIMESTAMP '{r.run_ts.strftime('%Y-%m-%d %H:%M:%S.%f')}')"
        for r in batch
    ])

    # MERGE instead of INSERT to handle reruns and duplicate prevention
    return f"""
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

# %%
# -- Databricks SQL API Call --
loaded = execute_merge(list(df_stocks.itertuples()), build_merge_sql, label="tickers")
print(f"Loaded {loaded} tickers for {target_date}.")
