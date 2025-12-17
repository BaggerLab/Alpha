import os
import time
import requests
import pandas as pd
from datetime import datetime, timezone


# -------------------------
# Time utils
def utc_ms(dt: datetime) -> int:
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return int(dt.timestamp() * 1000)


def now_utc() -> datetime:
    return datetime.now(timezone.utc)


def parse_dt_utc(s: str, is_end: bool = False) -> datetime:
    fmts = [
        "%Y-%m-%d",
        "%Y-%m-%d %H:%M",
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%dT%H:%M",
        "%Y-%m-%dT%H:%M:%S",
    ]
    for f in fmts:
        try:
            dt = datetime.strptime(s, f)
            break
        except ValueError:
            dt = None
    if dt is None:
        raise ValueError(f"Invalid datetime format: {s}")

    if len(s) == 10:  # YYYY-MM-DD
        if is_end:
            dt = dt.replace(hour=23, minute=59, second=59)
        else:
            dt = dt.replace(hour=0, minute=0, second=0)

    return dt.replace(tzinfo=timezone.utc)


# -------------------------
# FS utils
def ensure_dir(path: str):
    os.makedirs(path, exist_ok=True)

# -------------------------
# HTTP
def http_get(url: str, params: dict, max_retries: int = 5):
    last_err = None
    for i in range(max_retries):
        try:
            r = requests.get(url, params=params, timeout=20)
            if r.status_code == 429:
                time.sleep(1.0 + i)
                continue
            r.raise_for_status()
            data = r.json()

            # Binance error JSON 방어
            if isinstance(data, dict) and "code" in data and "msg" in data:
                raise RuntimeError(data)

            return data

        except Exception as e:
            last_err = e
            time.sleep(0.5 * (i + 1))

    raise RuntimeError(f"GET failed: {url} params={params} err={last_err}")

# -------------------------
# CSV save
def save_csv_append(df: pd.DataFrame, out_path: str):
    if df is None or df.empty:
        return
    header = not os.path.exists(out_path)
    df.to_csv(out_path, mode="a", header=header, index=False)
    print(f"[append] {out_path} rows={len(df)}")
