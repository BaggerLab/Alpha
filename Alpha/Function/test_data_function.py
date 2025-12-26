import os
import time
import requests
import pandas as pd
from datetime import datetime, timezone


# -------------------------
# Time utils


# "2025.12.01" -> "2025-12-01"
def date_dot_to_bar(s: pd.Series) -> pd.Series:
    return s.astype(str).str.strip().str.replace(".", "-", regex=False)


# DATE/TIME -> TIMESTAMP 형식으로 변환
def to_ts(df: pd.DataFrame) -> pd.DataFrame:
    if "DATE" not in df.columns or "TIME" not in df.columns:
        raise ValueError("DATE/TIME 컬럼이 필요합니다.")
    data = df.copy()

    date_norm = date_dot_to_bar(data["DATE"])
    time_norm = data["TIME"].astype(str).str.strip()

    data["TIMESTAMP"] = pd.to_datetime(date_norm + " " + time_norm, errors="coerce")
    data = data.dropna(subset=["TIMESTAMP"])
    return data


# -------------------------
# DATA filter


# interval 필터링된 종가
def filt_interval_close_df(df: pd.DataFrame, interval: str) -> pd.DataFrame:
    # TIMESTAMP 추가
    data = to_ts(df)
    # INTERVAL 필터
    data = data[data["INTERVAL"] == interval.upper()].copy()
    if data.empty:
        raise ValueError(f"INTERVAL={interval} 선물 데이터 없음")

    # 선물 가격 numeric 변환
    data["CLOSE"] = pd.to_numeric(data["CLOSE"], errors="coerce")
    data = data.dropna(subset=["CLOSE"])
    # 1. 코인별 / 2. 시간순
    data = data.sort_values(["COIN", "TIMESTAMP"]).reset_index(drop=True)
    # 종가 데이터만 취급
    return data[["COIN", "TIMESTAMP", "CLOSE"]]


# funding Rate만 필터링
def filt_funding(df: pd.DataFrame) -> pd.DataFrame:
    x = to_ts(df)
    if x.empty:
        raise ValueError("펀딩 데이터가 비었습니다.")

    x["FUNDING_RATE"] = pd.to_numeric(x["FUNDING_RATE"], errors="coerce")
    x = x.dropna(subset=["FUNDING_RATE"])

    # funding은 (COIN, TIMESTAMP) 중복 제거
    x = (
        x.sort_values(["COIN", "TIMESTAMP"])
        .drop_duplicates(["COIN", "TIMESTAMP"], keep="last")
        .reset_index(drop=True)
    )
    return x[["COIN", "TIMESTAMP", "FUNDING_RATE"]]


# 변화율 칼럼 추가
def add_vol(data: pd.DataFrame, vol_abs_th: float) -> pd.DataFrame:
    x = data.sort_values(["COIN", "TIMESTAMP"]).copy()
    # pct_change()는 “이전 행 대비 변화율” 계산 (정렬 필수)
    # RoC: Rate of Change
    x["RoC"] = x.groupby("COIN")["CLOSE"].pct_change()
    x["VOL_BLOCK"] = x["RoC"].abs() >= vol_abs_th
    return x[["COIN", "TIMESTAMP", "RoC", "VOL_BLOCK"]]


# -------------------------
# CSV load
def load_csv_data(file_path: str) -> pd.DataFrame:
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"파일이 없습니다: {file_path}")
    return pd.read_csv(file_path)
