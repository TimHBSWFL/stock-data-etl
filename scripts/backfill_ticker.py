#%%
"""Backfill trading dates missed by the daily pipeline (token expiry, outage, etc.).

    python scripts/backfill_ticker.py 2026-09-21 2026-09-24   # inclusive range
    python scripts/backfill_ticker.py 2026-09-21              # single date
    python scripts/backfill_ticker.py                         # the dates set below

yfinance returns nothing for weekends and holidays, so a range may span them --
only real sessions come back. Each row's trade_date comes from the bar itself
rather than from the requested dates, so a range cannot mislabel a session.
"""
import os
import re
import sys
import numpy as np
import yfinance as yf
import pandas as pd
from datetime import datetime, timezone

# Running the file as a script puts scripts/ on the path automatically; this
# keeps the import working when the cells are run from the repo root.
sys.path.append(os.path.join(os.getcwd(), "scripts"))
from databricks_sql import execute_merge

#%%
# -- Used when no dates are given on the command line, e.g. running as cells --
BACKFILL_DATE = "2026-09-21"
BACKFILL_END_DATE = "2026-09-24"   # same as BACKFILL_DATE for a single day

#%%
# Match only YYYY-MM-DD so a notebook kernel's own argv is ignored here.
dates = [a for a in sys.argv[1:] if re.fullmatch(r"\d{4}-\d{2}-\d{2}", a)]
start_date = dates[0] if dates else BACKFILL_DATE
end_date = dates[1] if len(dates) > 1 else (start_date if dates else BACKFILL_END_DATE)

start_ts = pd.Timestamp(start_date)
end_ts = pd.Timestamp(end_date)
if end_ts < start_ts:
    sys.exit(f"End date {end_date} is before start date {start_date}.")

print(f"Backfilling {start_ts:%Y-%m-%d} through {end_ts:%Y-%m-%d}")

#%%
file_path = "files/sp500_watchlist.csv"
df = pd.read_csv(file_path)
tickers = df['tickers'].tolist()

#%%
# yfinance end date is exclusive, so add one day to include end_ts itself
data = yf.download(
    tickers=tickers,
    start=f"{start_ts:%Y-%m-%d}",
    end=f"{end_ts + pd.Timedelta(days=1):%Y-%m-%d}",
    interval='1d',
    group_by='ticker',
    auto_adjust=True,
    threads=False
)

if data.empty:
    print(f"No data returned for {start_date}..{end_date}. Exiting.")
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

    for stamp, row in df_ticker.iterrows():
        # A partial row from Yahoo would otherwise reach the SQL as a bare `nan`
        # literal and fail the whole MERGE.
        ohlcv = pd.to_numeric(row[["Open", "High", "Low", "Close", "Volume"]], errors="coerce")
        if not np.isfinite(ohlcv).all():
            print(f"Skipping {ticker} {stamp.date()}, incomplete OHLCV data")
            continue

        rows.append({
            "ticker": ticker,
            "trade_date": stamp.date(),
            "open": float(row["Open"]),
            "high": float(row["High"]),
            "low": float(row["Low"]),
            "close": float(row["Close"]),
            "volume": int(row["Volume"]),
            "run_ts": run_ts
        })

df_backfill = pd.DataFrame(rows)

if df_backfill.empty:
    print("No data to insert. Exiting.")
    exit(0)

df_backfill.drop_duplicates(subset=['ticker', 'trade_date'], inplace=True)

sessions = sorted(df_backfill['trade_date'].unique())
print(f"Found {len(sessions)} session(s): {', '.join(str(s) for s in sessions)}")
for session in sessions:
    print(f"  {session}: {(df_backfill['trade_date'] == session).sum()} tickers")

#%%
def build_merge_sql(batch):
    values_sql = ",".join([
        f"('{r.ticker}', '{r.trade_date}', {r.open}, {r.high}, {r.low}, {r.close}, {r.volume}, TIMESTAMP '{r.run_ts.strftime('%Y-%m-%d %H:%M:%S.%f')}')"
        for r in batch
    ])

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

#%%
loaded = execute_merge(list(df_backfill.itertuples()), build_merge_sql, label="rows")
print(f"Backfill complete. {loaded} rows across {len(sessions)} session(s).")
