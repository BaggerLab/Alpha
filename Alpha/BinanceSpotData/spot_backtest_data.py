import time
import pandas as pd
import json
import Alpha.Function.collect_data_function as FUNC

"""
파일 실행방법: mac: python3 -m Alpha.BinanceSpotData.spot_backtest_data
파일 실행방법: window: python -m Alpha.BinanceSpotData.spot_backtest_data
"""

SPOT_URL = "https://api.binance.com"
OUT_DIR = "./DB/backtest"

INTERVALS = ["1h", "4h", "1d"]

# 비트, 이더리움, 리플, 솔라나, BNB
COINS = ["BTC", "ETH", "SOL", "XRP", "BNB"]
TARGET_SYMBOLS = [c + "USDT" for c in COINS]


# 바이낸스 현물 Order Book(호가창)의 상위 일부 요약
def get_orderbook_summary(symbol: str, limit: int = 20) -> str | None:
    url = f"{SPOT_URL}/api/v3/depth"
    try:
        ob = FUNC.http_get(url, {"symbol": symbol, "limit": limit})
        summary = {
            "bids": ob.get("bids", [])[:5],
            "asks": ob.get("asks", [])[:5],
        }

        # 기존 json 방식 사용
        return json.dumps(summary, ensure_ascii=False)
    except Exception:
        return None


# ---------- Kline ----------
# VOLUME: 기초 자산 기준 거래량
# QUOTE_VOLUME: USDT 기준 거래대금 (VOLUME x 체결 가격)
# NUM_TRADES: 해당 캔들 내 총 체결 횟수 (체결 건수)
# TAKER_BUY_BASE_VOLUME: 매수자가 시장가로 체결한 수량 (매수 주도 세기)
# TAKER_BUY_QUOTE_VOLUME: TAKER_BUY_BASE_VOLUME x 체결가(USDT)
# BUY_VOLUME: TAKER_BUY_BASE_VOLUME
# SELL_VOLUME: (VOLUME - TAKER_BUY_BASE_VOLUME)
# DELTA_VOLUME: (BUY_VOLUME - SELL_VOLUME) = (2 × BUY_VOLUME) - VOLUME
# CVD: 누적 합(DELTA_VOLUME)
def fetch_spot_klines(
    symbol: str, interval: str, start_ms: int, end_ms: int
) -> pd.DataFrame:
    url = f"{SPOT_URL}/api/v3/klines"
    rows, cur = [], start_ms

    while True:
        data = FUNC.http_get(
            url,
            {
                "symbol": symbol,
                "interval": interval,
                "startTime": cur,
                "endTime": end_ms,
                "limit": 1000,
            },
        )
        if not data:
            break

        rows.extend(data)
        next_cur = int(data[-1][0]) + 1
        if next_cur >= end_ms or len(data) < 1000:
            break

        cur = next_cur
        time.sleep(0.15)

    if not rows:
        return pd.DataFrame()

    # Spot Kline schema (12 fields)
    cols = [
        "OPEN_TIME_MS",
        "OPEN",
        "HIGH",
        "LOW",
        "CLOSE",
        "VOLUME",
        "CLOSE_TIME_MS",
        "QUOTE_VOLUME",
        "NUM_TRADES",
        "TAKER_BUY_BASE_VOLUME",
        "TAKER_BUY_QUOTE_VOLUME",
        "IGNORE",
    ]
    # 필요없는 항목 제거
    df = pd.DataFrame(rows, columns=cols).drop(columns=["IGNORE"])

    # 대문자 형식 지정
    df["SYMBOL"] = symbol
    df["COIN"] = symbol.replace("USDT", "")
    df["INTERVAL"] = interval.upper()

    # OPEN_TIME 기준으로 KST -> DATE/TIME 분리
    df["DT_KST"] = FUNC.ms_to_kst_dt(df["OPEN_TIME_MS"])
    # df["DATE"], df["TIME"] 추가
    df = FUNC.split_date_time(df, "DT_KST")

    # 숫자형 변환
    num_cols = [
        "OPEN",
        "HIGH",
        "LOW",
        "CLOSE",
        "VOLUME",
        "QUOTE_VOLUME",
        "TAKER_BUY_BASE_VOLUME",
        "TAKER_BUY_QUOTE_VOLUME",
    ]
    for c in num_cols:
        df[c] = pd.to_numeric(df[c], errors="coerce")
    df["NUM_TRADES"] = pd.to_numeric(df["NUM_TRADES"], errors="coerce").astype("Int64")

    # CVD 계산 (캔들에 이미 포함된 taker buy 사용)
    df["BUY_VOLUME"] = df["TAKER_BUY_BASE_VOLUME"]
    df["SELL_VOLUME"] = df["VOLUME"] - df["TAKER_BUY_BASE_VOLUME"]
    df["DELTA_VOLUME"] = df["BUY_VOLUME"] - df["SELL_VOLUME"]  # = (2 x BUY) - VOLUME

    # CVD는 코인/주기별 누적
    df = df.sort_values(["COIN", "INTERVAL", "DATE", "TIME"]).reset_index(drop=True)
    df["CVD"] = df.groupby(["COIN", "INTERVAL"])["DELTA_VOLUME"].cumsum()

    out_cols = [
        "COIN",
        "SYMBOL",
        "INTERVAL",
        "DATE",
        "TIME",
        "OPEN",
        "HIGH",
        "LOW",
        "CLOSE",
        "VOLUME",
        "QUOTE_VOLUME",
        "NUM_TRADES",
        "TAKER_BUY_BASE_VOLUME",
        "TAKER_BUY_QUOTE_VOLUME",
        "BUY_VOLUME",
        "SELL_VOLUME",
        "DELTA_VOLUME",
        "CVD",
    ]
    return df[out_cols]


if __name__ == "__main__":
    FUNC.ensure_dir(OUT_DIR)

    start_dt = FUNC.parse_dt_utc("2020-01-01")
    end_dt = FUNC.parse_dt_utc("2025-11-30", is_end=True)

    start_ms = FUNC.utc_ms(start_dt)
    end_ms = FUNC.utc_ms(end_dt)

    KEY_COLS = ["COIN", "SYMBOL", "INTERVAL", "DATE", "TIME"]
    SORT_COLS = ["COIN", "INTERVAL", "DATE", "TIME"]

    out_path = f"{OUT_DIR}/spot_kline.csv"

    for sym in TARGET_SYMBOLS:
        print(f"[SPOT] {sym}")

        ob_snapshot = get_orderbook_summary(sym, limit=20)

        for iv in INTERVALS:
            df = fetch_spot_klines(sym, iv, start_ms, end_ms)
            if df.empty:
                continue

            # 원하면 스냅샷 1개를 전체 행에 넣기(기존방식)
            df["ORDERBOOK"] = ob_snapshot

            FUNC.save_csv_upsert_sorted(df, out_path, KEY_COLS, SORT_COLS)

            time.sleep(0.1)

        time.sleep(0.3)
