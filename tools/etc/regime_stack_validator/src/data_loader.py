import pandas as pd


def load_market_data(csv_path, start_date, days):
    """
    Load market data from CSV snapshot.

    Required CSV columns:
    - SYMBOL
    - EXCHANGE
    - TRADE_DATE (datetime with or without time)
    - CLOSE

    Extra columns are ignored.
    """

    # 1. Read CSV
    df = pd.read_csv(
        csv_path,
        parse_dates=["TRADE_DATE"],
        dtype={
            "SYMBOL": str,
            "EXCHANGE": str
        }
    )

    # 2. Normalize code
    df["code"] = df["SYMBOL"].str.zfill(6) + "." + df["EXCHANGE"].str.upper()

    # 3. Sort by code + trade date
    df = df.sort_values(["code", "TRADE_DATE"])

    # 4. Slice backtest window (交易日，不用自然日)
    start = pd.to_datetime(start_date)
    end = start + pd.tseries.offsets.BDay(days)

    df = df[(df["TRADE_DATE"] >= start) & (df["TRADE_DATE"] <= end)]

    # 5. Derive prev_close per stock
    df["prev_close"] = df.groupby("code")["CLOSE"].shift(1)

    # 6. Standardize output schema
    df_out = df.rename(columns={
        "TRADE_DATE": "date",
        "CLOSE": "close"
    })[
        ["date", "code", "close", "prev_close"]
    ]

    return df_out
