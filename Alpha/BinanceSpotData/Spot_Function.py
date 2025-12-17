import os
import time
from datetime import datetime, timedelta
import pytz
import pandas as pd
from tqdm import tqdm
from binance.client import Client
from binance.exceptions import BinanceAPIException
import json

from dotenv import load_dotenv
import os
from binance.client import Client

load_dotenv()

API_KEY = os.getenv("BINANCE_API_KEY")
API_SECRET = os.getenv("BINANCE_API_SECRET")

client = Client(api_key=API_KEY, api_secret=API_SECRET)

SYMBOLS = ["BTCUSDT", "ETHUSDT"]
INTERVALS = ["1h", "4h", "1d"]

KST = pytz.timezone("Asia/Seoul")
start_kst = KST.localize(datetime(2020, 1, 1, 9, 0, 0))
end_kst = KST.localize(datetime(2025, 12, 1, 8, 59, 0))

start_utc = start_kst.astimezone(pytz.utc)
end_utc = end_kst.astimezone(pytz.utc)

start_ts_ms = int(start_utc.timestamp() * 1000)
end_ts_ms = int(end_utc.timestamp() * 1000)

print("UTC time range:", start_utc, "~", end_utc)
print("=" * 80)


def fetch_klines_with_cvd(symbol, interval, start_ms, end_ms, limit=1000, sleep_sec=0.3):
    """
    캔들 정보 + CVD(buy_volume - sell_volume 누적) 계산
    """
    all_rows = []
    current_start = start_ms

    coin = symbol.replace("USDT", "")

    pbar = tqdm(desc=f"{symbol}-{interval}", unit="candles", position=0, leave=True)

    while current_start < end_ms:
        try:
            klines = client.get_klines(
                symbol=symbol,
                interval=interval,
                startTime=current_start,
                endTime=end_ms,
                limit=limit,
            )
        except BinanceAPIException as e:
            print(f"[KLINES ERROR] {symbol} {interval}: {e}")
            time.sleep(2)
            continue

        if not klines:
            break

        for k in klines:
            open_time = k[0]
            close_time = k[6]
            open_price = float(k[1])
            high_price = float(k[2])
            low_price = float(k[3])
            close_price = float(k[4])
            volume = float(k[5]) 
            quote_vol = float(k[7]) 
            trades = int(k[8])

            buy_vol = 0.0
            sell_vol = 0.0

            from_id = None
            retry_count = 0
            max_retries = 3

            while retry_count < max_retries:
                try:
                    agg = client.get_aggregate_trades(
                        symbol=symbol,
                        startTime=open_time,
                        endTime=close_time,
                        fromId=from_id,
                    )
                except BinanceAPIException as e:
                    retry_count += 1
                    if retry_count >= max_retries:
                        print(
                            f"[AGG RETRY FAILED] {symbol} {interval} {open_time}: {e}"
                        )
                        break
                    time.sleep(1)
                    continue

                if not agg:
                    break

                for t in agg:
                    qty = float(t["q"])
                    if t["m"]:  
                        sell_vol += qty
                    else:  
                        buy_vol += qty

                from_id = agg[-1]["a"] + 1

                if len(agg) < 1000:
                    break

                time.sleep(0.05)

            delta_volume = buy_vol - sell_vol

            dt_utc = datetime.utcfromtimestamp(close_time / 1000)
            dt_utc = pytz.utc.localize(dt_utc)
            dt_kst = dt_utc.astimezone(KST)

            row = {
                "DATE": dt_kst.strftime("%Y-%m-%d %H:%M:%S"),  
                "COIN": coin,
                "INTERVAL": interval,
                "OPEN": open_price,
                "HIGH": high_price,
                "LOW": low_price,
                "CLOSE": close_price,
                "VOLUME": volume,  
                "QUOTE_VOLUME": quote_vol,
                "TRADES": trades,
                "BUY_VOLUME": buy_vol,
                "SELL_VOLUME": sell_vol,
                "DELTA_VOLUME": delta_volume,
                "CVD": None,  
                "ORDERBOOK": None,  
            }
            all_rows.append(row)

        last_close = klines[-1][6]
        current_start = last_close + 1

        pbar.update(len(klines))
        time.sleep(sleep_sec)

        if last_close >= end_ms:
            break

    pbar.close()

    if not all_rows:
        return pd.DataFrame()

    df = pd.DataFrame(all_rows)

    df = df.sort_values(["INTERVAL", "DATE"]).reset_index(drop=True)
    df["CVD"] = df.groupby(["INTERVAL"])["DELTA_VOLUME"].cumsum()

    return df


def get_orderbook_summary(symbol, limit=20):
    """
    현재 시점 orderbook 요약 (JSON 문자열로)
    """
    try:
        ob = client.get_order_book(symbol=symbol, limit=limit)
        summary = {
            "bids": ob["bids"][:5],  
            "asks": ob["asks"][:5],
        }
        return json.dumps(summary, ensure_ascii=False)
    except Exception as e:
        print(f"[OB ERROR] {symbol}: {e}")
        return None


all_dfs = []

for symbol in SYMBOLS:
    coin = symbol.replace("USDT", "")

    for interval in INTERVALS:
        print(f"\n[수집 중] {symbol} {interval}")

        df = fetch_klines_with_cvd(
            symbol=symbol,
            interval=interval,
            start_ms=start_ts_ms,
            end_ms=end_ts_ms,
        )

        if df.empty:
            print(f"  ⚠️  {symbol} {interval}: 데이터 없음")
            continue

        ob_snapshot = get_orderbook_summary(symbol, limit=20)
        df["ORDERBOOK"] = ob_snapshot

        all_dfs.append(df)

        print(f"  ✓ {symbol} {interval}: {len(df)} 행 수집 완료")
        time.sleep(0.5)

if all_dfs:
    merged_df = pd.concat(all_dfs, ignore_index=True)

    merged_df = merged_df.sort_values(["DATE", "COIN", "INTERVAL"]).reset_index(
        drop=True
    )

    final_cols = [
        "DATE",
        "COIN",
        "INTERVAL",
        "OPEN",
        "HIGH",
        "LOW",
        "CLOSE",
        "VOLUME",
        "QUOTE_VOLUME",
        "TRADES",
        "BUY_VOLUME",
        "SELL_VOLUME",
        "DELTA_VOLUME",
        "CVD",
        "ORDERBOOK",
    ]

    merged_df = merged_df[final_cols]

    output_dir = "./binance_spot_data"
    os.makedirs(output_dir, exist_ok=True)

    output_file = os.path.join(
        output_dir, "binance_spot_BTC_ETH_2020-2025_merged.csv"
    )
    merged_df.to_csv(output_file, index=False, encoding="utf-8-sig")

    print("\n" + "=" * 80)
    print(f"✅ 병합 완료! 파일 저장: {output_file}")
    print(f"   총 행 수: {len(merged_df)}")
    print("\n[샘플 데이터]")
    print(merged_df.head(10))

    print("\n[통계]")
    print(f"수집 기간: {merged_df['DATE'].min()} ~ {merged_df['DATE'].max()}")
    print(f"BTC 행 수: {len(merged_df[merged_df['COIN'] == 'BTC'])}")
    print(f"ETH 행 수: {len(merged_df[merged_df['COIN'] == 'ETH'])}")
    print(f"\nInterval별 행 수:")
    print(merged_df.groupby("INTERVAL").size())
else:
    print("❌ 수집된 데이터가 없습니다.")

