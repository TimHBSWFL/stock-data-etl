import os
import sys
import pandas as pd
from datetime import datetime, timezone

# Running the file as a script puts scripts/ on the path automatically; this
# keeps the import working when the cells are run from the repo root.
sys.path.append(os.path.join(os.getcwd(), "scripts"))
from databricks_sql import execute_merge, execute_statement, fetch_rows

query = """
    SELECT ticker, trade_date, close
    FROM analytics.stock_prices
    ORDER BY ticker, trade_date
"""
body = execute_statement(query)

columns = [col["name"] for col in body["manifest"]["schema"]["columns"]]
rows = fetch_rows(body)

df = pd.DataFrame(rows, columns=columns)
df["close"] = df["close"].astype(float)
df["trade_date"] = pd.to_datetime(df["trade_date"])

df["ema_12"] = df.groupby("ticker")["close"].transform(lambda x: x.ewm(span=12, adjust=False).mean())
df["ema_26"] = df.groupby("ticker")["close"].transform(lambda x: x.ewm(span=26, adjust=False).mean())
df["macd_line"] = df["ema_12"] - df["ema_26"]

df["signal_line"] = df.groupby("ticker")["macd_line"].transform(lambda x: x.ewm(span=9, adjust=False).mean())

df["histogram"] = df["macd_line"] - df["signal_line"]
df["prev_macd"] = df.groupby("ticker")["macd_line"].shift(1)
df["prev_signal"] = df.groupby("ticker")["signal_line"].shift(1)

signals = df[
    (df["prev_macd"] < df["prev_signal"]) &
    (df["macd_line"] >= df["signal_line"])
].copy()

signals = signals[["ticker", "trade_date", "close", "macd_line", "signal_line", "histogram"]].reset_index(drop=True)

run_ts = datetime.now(timezone.utc)


def build_merge_sql(batch):
    values_sql = ",".join([
        f"('{r.ticker}', '{str(r.trade_date)[:10]}', {r.close}, {r.macd_line}, {r.signal_line}, {r.histogram}, TIMESTAMP '{run_ts.strftime('%Y-%m-%d %H:%M:%S.%f')}')"
        for r in batch
    ])

    return f"""
MERGE INTO analytics.macd_signals AS target
USING (
    SELECT ticker, trade_date, close, macd_line, signal_line, histogram, run_ts
    FROM VALUES {values_sql}
    AS source(ticker, trade_date, close, macd_line, signal_line, histogram, run_ts)
)
ON target.ticker = source.ticker AND target.trade_date = source.trade_date
WHEN MATCHED THEN UPDATE SET
    close = source.close,
    macd_line = source.macd_line,
    signal_line = source.signal_line,
    histogram = source.histogram,
    run_ts = source.run_ts
WHEN NOT MATCHED THEN INSERT (ticker, trade_date, close, macd_line, signal_line, histogram, run_ts)
VALUES (source.ticker, source.trade_date, source.close, source.macd_line, source.signal_line, source.histogram, source.run_ts)
"""


loaded = execute_merge(list(signals.itertuples()), build_merge_sql, label="crossover signals")
print(f"MACD signals written successfully. {loaded} crossover signals loaded.")
