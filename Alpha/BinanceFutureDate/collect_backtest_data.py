import time
import pandas as pd
import collect_function as FUNC

"""
바이낸스는 최근 30일 기준의 OI만 호출 가능
Liquidation은 미제공
>> Funding과 캔들데이터만 불러오는 스크립트
"""

FUTURES_URL = "https://fapi.binance.com"
OUT_DIR = "./DB/backtest"

KLINE_INTERVALS = ["1h", "4h", "1d"]
FUNDING_INTERVAL = "8h"

# 비트, 이더리움, 리플, 솔라나, BNB
COINS = ["BTC", "ETH", "SOL", "XRP", "BNB"]
TARGET_SYMBOLS = [c + "USDT" for c in COINS]


# ---------- Kline ----------
# VOLUME: 기초 자산 기준 거래량
# QUOTE_ASSET_VOLUME: USDT 기준 거래대금 (VOLUME x 평균 가격)
# NUM_TRADES: 해당 캔들 내 총 체결 횟수 (체결 건수)
# TAKER_BUY_BASE_VOLUME: 매수자가 시장가로 체결한 수량 (매수 주도 세기)
# TAKER_BUY_QUOTE_VOLUME: TAKER_BUY_BASE_VOLUME x 체결가(USDT)
def fetch_klines(
    symbol: str, interval: str, start_ms: int, end_ms: int
) -> pd.DataFrame:
    url = f"{FUTURES_URL}/fapi/v1/klines"
    rows, cur = [], start_ms

    while True:
        data = FUNC.http_get(
            url,
            {
                "symbol": symbol,
                "interval": interval,
                "startTime": cur,
                "endTime": end_ms,
                "limit": 1500,
            },
        )
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
        "OPEN_TIME_MS",
        "OPEN",
        "HIGH",
        "LOW",
        "CLOSE",
        "VOLUME",
        "CLOSE_TIME_MS",
        "QUOTE_ASSET_VOLUME",
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
    df["INTERVAL"] = interval.upper()  # 1H/4H/1D

    # OPEN_TIME 기준으로 KST -> DATE/TIME 분리
    df["DT_KST"] = FUNC.ms_to_kst_dt(df["OPEN_TIME_MS"])
    # df["DATE"], df["TIME"] 추가
    df = FUNC.split_date_time(df, "DT_KST")

    # 숫자 변환
    num_cols = [
        "OPEN",
        "HIGH",
        "LOW",
        "CLOSE",
        "VOLUME",
        "QUOTE_ASSET_VOLUME",
        "TAKER_BUY_BASE_VOLUME",
        "TAKER_BUY_QUOTE_VOLUME",
    ]
    for c in num_cols:
        df[c] = pd.to_numeric(df[c], errors="coerce")
    df["NUM_TRADES"] = pd.to_numeric(df["NUM_TRADES"], errors="coerce").astype("Int64")

    # 시간 컬럼은 저장에서 제거(단순화)
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
        "QUOTE_ASSET_VOLUME",
        "NUM_TRADES",
        "TAKER_BUY_BASE_VOLUME",
        "TAKER_BUY_QUOTE_VOLUME",
    ]
    return df[out_cols]


# ---------- Funding ----------
# FUNDING_RATE: 펀딩비
def fetch_funding(symbol, start_ms, end_ms):
    url = f"{FUTURES_URL}/fapi/v1/fundingRate"
    rows, cur = [], start_ms

    while True:
        data = FUNC.http_get(
            url,
            {
                "symbol": symbol,
                "startTime": cur,
                "endTime": end_ms,
                "limit": 1000,
            },
        )
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

    # 대문자 + 스키마 정리
    df.rename(
        columns={
            "symbol": "SYMBOL",
            "fundingTime": "FUNDING_TIME_MS",
            "fundingRate": "FUNDING_RATE",
            "markPrice": "MARK_PRICE",
        },
        inplace=True,
    )

    df["SYMBOL"] = symbol
    df["COIN"] = symbol.replace("USDT", "")
    df["INTERVAL"] = FUNDING_INTERVAL.upper()  # 8H

    # 시간 통일 / FUNDING_TIME 기준으로 KST -> DATE/TIME 분리
    df["DT_KST"] = FUNC.ms_to_kst_dt(df["FUNDING_TIME_MS"])
    # df["DATE"], df["TIME"] 추가
    df = FUNC.split_date_time(df, "DT_KST")

    df["FUNDING_RATE"] = pd.to_numeric(df["FUNDING_RATE"], errors="coerce")
    if "MARK_PRICE" in df.columns:
        df["MARK_PRICE"] = pd.to_numeric(df["MARK_PRICE"], errors="coerce")

    out_cols = ["COIN", "SYMBOL", "INTERVAL", "DATE", "TIME", "FUNDING_RATE"]
    if "MARK_PRICE" in df.columns:
        out_cols.append("MARK_PRICE")
    return df[out_cols]


if __name__ == "__main__":
    FUNC.ensure_dir(OUT_DIR)

    start_dt = FUNC.parse_dt_utc("2020-01-01")
    end_dt = FUNC.parse_dt_utc("2025-11-30", is_end=True)

    start_ms = FUNC.utc_ms(start_dt)
    end_ms = FUNC.utc_ms(end_dt)

    kline_path = f"{OUT_DIR}/kline.csv"
    funding_path = f"{OUT_DIR}/funding.csv"

    # 심볼 필터
    for sym in TARGET_SYMBOLS:
        print(f"[BACKTEST] {sym}")

        df_f = fetch_funding(sym, start_ms, end_ms)
        FUNC.save_csv_append(df_f, funding_path)

        for iv in KLINE_INTERVALS:
            df_k = fetch_klines(sym, iv, start_ms, end_ms)
            FUNC.save_csv_append(df_k, kline_path)
