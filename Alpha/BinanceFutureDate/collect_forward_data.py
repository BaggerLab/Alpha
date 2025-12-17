import time
import pandas as pd
from datetime import timedelta
import collect_function as FUNC

FUTURES_URL = "https://fapi.binance.com"
OUT_DIR = "./DB/forwardTest"

KLINE_INTERVALS = ["1h", "4h", "1d"]
OI_INTERVALS = ["1h", "4h", "1d"]


def fetch_symbols():
    url = f"{FUTURES_URL}/fapi/v1/exchangeInfo"
    data = FUNC.http_get(url, {})
    return [
        s["symbol"]
        for s in data["symbols"]
        if s["contractType"] == "PERPETUAL"
        and s["quoteAsset"] == "USDT"
        and s["status"] == "TRADING"
    ]

# Kline
def fetch_klines(symbol, interval, start_ms, end_ms):
    url = f"{FUTURES_URL}/fapi/v1/klines"
    rows, cur = [], start_ms

    while True:
        data = FUNC.http_get(url, {
            "symbol": symbol,
            "interval": interval,
            "startTime": cur,
            "endTime": end_ms,
            "limit": 1500
        })
        if not data:
            break

        rows.extend(data)
        next_cur = int(data[-1][0]) + 1
        if next_cur >= end_ms or len(data) < 1500:
            break
        cur = next_cur
        time.sleep(0.2)

    if not rows:
        return pd.DataFrame()

    cols = [
        "open_time","open","high","low","close","volume",
        "close_time","quote_asset_volume","num_trades",
        "taker_buy_base_volume","taker_buy_quote_volume","ignore"
    ]
    df = pd.DataFrame(rows, columns=cols).drop(columns=["ignore"])

    df["symbol"] = symbol
    df["interval"] = interval
    df["open_time"] = pd.to_datetime(df["open_time"], unit="ms", utc=True)
    df["close_time"] = pd.to_datetime(df["close_time"], unit="ms", utc=True)

    return df


# Funding
def fetch_funding(symbol, start_ms, end_ms):
    url = f"{FUTURES_URL}/fapi/v1/fundingRate"
    rows, cur = [], start_ms

    while True:
        data = FUNC.http_get(url, {
            "symbol": symbol,
            "startTime": cur,
            "endTime": end_ms,
            "limit": 1000
        })
        if not data:
            break

        rows.extend(data)
        next_cur = int(data[-1]["fundingTime"]) + 1
        if next_cur >= end_ms or len(data) < 1000:
            break
        cur = next_cur

    if not rows:
        return pd.DataFrame()

    df = pd.DataFrame(rows)
    df["fundingTime"] = pd.to_datetime(df["fundingTime"], unit="ms", utc=True)
    df["symbol"] = symbol
    return df


# oi
# OI는 최근 30일 제한 (주의!)
def fetch_oi(symbol, interval, start_ms, end_ms):
    url = f"{FUTURES_URL}/futures/data/openInterestHist"
    data = FUNC.http_get(url, {
        "symbol": symbol,
        "period": interval,
        "startTime": start_ms,
        "endTime": end_ms,
        "limit": 500
    })
    if not data:
        return pd.DataFrame()

    df = pd.DataFrame(data)
    df["timestamp"] = pd.to_datetime(df["timestamp"], unit="ms", utc=True)
    df["symbol"] = symbol
    df["interval"] = interval
    return df


if __name__ == "__main__":
    FUNC.ensure_dir(OUT_DIR)

    start_dt = FUNC.parse_dt_utc("2025-12-01")
    end_dt = FUNC.now_utc()

    start_ms = FUNC.utc_ms(start_dt)
    end_ms   = FUNC.utc_ms(end_dt)

    for sym in fetch_symbols():
        print(f"[FORWARD] {sym}")

        df_f = fetch_funding(sym, start_ms, end_ms)
        FUNC.save_csv_append(df_f, f"{OUT_DIR}/funding_8h.csv")

        for iv in OI_INTERVALS:
            df_oi = fetch_oi(sym, iv, start_ms, end_ms)
            FUNC.save_csv_append(df_oi, f"{OUT_DIR}/oi_{iv}.csv")

        for iv in KLINE_INTERVALS:
            df_k = fetch_klines(sym, iv, start_ms, end_ms)
            FUNC.save_csv_append(df_k, f"{OUT_DIR}/kline_{iv}.csv")

        time.sleep(0.3)
