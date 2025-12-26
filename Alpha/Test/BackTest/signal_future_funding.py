import os
import numpy as np
import pandas as pd
import Alpha.Function.test_data_function as FUNC

"""
파일 실행방법: mac: python3 -m Alpha.Test.BackTest.signal_future_funding
파일 실행방법: window: python -m Alpha.Test.BackTest.signal_future_funding
"""

INPUT_DIR = "./DB/backtest"
OUT_DIR = "./DB/Result"


# Kline 데이터와 Funding 데이터 (COINT, TIMESTAMP) 기준으로 병합
def sum_funding_kline(
    funding: pd.DataFrame,
    kline: pd.DataFrame,
) -> pd.DataFrame:

    # COIN | TIMESTAMP | FUNDING_RATE
    funding_df = funding.copy()
    # COIN | TIMESTAMP | RoC | VOL_BLOCK
    kline_df = kline.copy()

    funding_df = funding_df.sort_values(["COIN", "TIMESTAMP"]).reset_index(drop=True)
    kline_df = kline_df.sort_values(["COIN", "TIMESTAMP"]).reset_index(drop=True)

    results = []
    coins = sorted(set(funding_df["COIN"]).intersection(set(kline_df["COIN"])))

    for c in coins:
        fc = (
            funding_df[funding_df["COIN"] == c]
            .sort_values("TIMESTAMP")
            .reset_index(drop=True)
        )
        vc = (
            kline_df[kline_df["COIN"] == c]
            .sort_values("TIMESTAMP")
            .reset_index(drop=True)
        )

        merged = pd.merge_asof(
            fc,
            vc[["TIMESTAMP", "RoC", "VOL_BLOCK"]],
            on="TIMESTAMP",
            direction="backward",
            allow_exact_matches=True,
        )
        results.append(merged)

    out = pd.concat(results, ignore_index=True)
    out["VOL_BLOCK"] = out["VOL_BLOCK"].fillna(False).astype(bool)

    return out.sort_values(["COIN", "TIMESTAMP"]).reset_index(drop=True)


# -------------------------
# FUNDING CARRY (DELTA-NEUTRAL)
def funding_carry_Strategy(
    funding_evt: pd.DataFrame,
    min_abs_funding: float,
    confirm_n: int,
    notional_usdt: float,
    fee_bps_roundtrip: float,
) -> pd.DataFrame:
    """
    전략:
      - funding_rate > 0: 롱이 지불 → 선물 숏(= POSITION = -1)으로 funding 수취
      - funding_rate < 0: 숏이 지불 → 선물 롱(= POSITION = +1)으로 funding 수취
    """

    event_df = funding_evt.sort_values(["COIN", "TIMESTAMP"]).copy()

    event_df["ABS_F"] = event_df["FUNDING_RATE"].abs()
    event_df["SIGN_F"] = np.where(event_df["FUNDING_RATE"] >= 0, 1, -1)

    same = True
    for i in range(1, confirm_n + 1):
        same = same & (
            event_df["SIGN_F"] == event_df.groupby("COIN")["SIGN_F"].shift(i)
        )

    event_df["PERSIST_OK"] = same.fillna(False)

    event_df["ENTRY_OK"] = (
        (event_df["ABS_F"] >= min_abs_funding)
        & event_df["PERSIST_OK"]
        & (~event_df["VOL_BLOCK"])
    )

    event_df["TARGET_POS"] = np.where(
        event_df["ENTRY_OK"],
        np.where(event_df["FUNDING_RATE"] > 0, -1, 1),
        0,
    ).astype(int)

    event_df["POS"] = event_df["TARGET_POS"]

    event_df["PNL_FUNDING"] = (
        notional_usdt * event_df["FUNDING_RATE"] * (-event_df["POS"])
    )

    fee = fee_bps_roundtrip / 10000.0
    event_df["TURNOVER"] = event_df.groupby("COIN")["POS"].diff().abs().fillna(0)
    event_df["PNL_FEE"] = -notional_usdt * fee * event_df["TURNOVER"]

    event_df["PNL_NET"] = event_df["PNL_FUNDING"] + event_df["PNL_FEE"]

    event_df["EQUITY"] = event_df.groupby("COIN")["PNL_NET"].transform(
        lambda s: (1.0 + (s / notional_usdt).fillna(0)).cumprod()
    )

    return event_df


def summarize_carry(df: pd.DataFrame, notional_usdt: float) -> pd.DataFrame:
    out = []
    for coin, g in df.groupby("COIN"):
        g = g.dropna(subset=["PNL_NET"])
        if len(g) == 0:
            continue
        cum = (1.0 + (g["PNL_NET"] / notional_usdt)).prod() - 1.0
        mean = (g["PNL_NET"] / notional_usdt).mean()
        std = (g["PNL_NET"] / notional_usdt).std(ddof=0)
        sharpe = (mean / std) * np.sqrt(365 * 3) if std and std > 0 else np.nan
        trades = int((g["TURNOVER"] > 0).sum())
        out.append([coin, len(g), trades, cum, sharpe])
    return pd.DataFrame(
        out, columns=["COIN", "N_FUND_EVENTS", "N_TURNS", "CUM_RETURN", "SHARPE_APPROX"]
    )


def make_funding_thresholds(
    funding_evt: pd.DataFrame,
    quantiles: list,
) -> list:
    abs_f = funding_evt["FUNDING_RATE"].abs()
    return sorted(abs_f.quantile(quantiles).dropna().unique())


def grid_search_funding_carry(
    funding_evt: pd.DataFrame,
    notional_usdt: float,
    fee_bps_roundtrip: float,
    min_abs_funding_list: list,
    confirm_n_list: list,
) -> pd.DataFrame:

    results = []

    for min_f in min_abs_funding_list:
        for conf in confirm_n_list:

            bt = funding_carry_Strategy(
                funding_evt=funding_evt,
                min_abs_funding=min_f,
                confirm_n=conf,
                notional_usdt=notional_usdt,
                fee_bps_roundtrip=fee_bps_roundtrip,
            )

            summary = summarize_carry(bt, notional_usdt)

            for _, row in summary.iterrows():
                results.append(
                    {
                        "COIN": row["COIN"],
                        "MIN_ABS_FUNDING": min_f,
                        "CONFIRM_N": conf,
                        "N_FUND_EVENTS": row["N_FUND_EVENTS"],
                        "N_TURNS": row["N_TURNS"],
                        "CUM_RETURN": row["CUM_RETURN"],
                        "SHARPE_APPROX": row["SHARPE_APPROX"],
                    }
                )

    return pd.DataFrame(results)


# -------------------------
# MAIN
def main():
    os.makedirs(OUT_DIR, exist_ok=True)

    funding_path = os.path.join(INPUT_DIR, "future_funding.csv")
    kline_path = os.path.join(INPUT_DIR, "future_kline.csv")

    # 변동성은 고려하지 않는 것으로 확정 (0.03 ~ 1 까지 점차 늘려나갔지면 결론적으로 변동성을 고려하지 않을때 수익률 개선되며 점차 수렴하는 형태)
    VOL_ABS = 1
    CONFIRM_N_LIST = [1, 2, 3, 4, 5]
    NOTIONAL = 1_000_000.0
    FEE_BPS_ROUNDTRIP = 6.0

    funding_df = FUNC.filt_funding(FUNC.load_csv_data(funding_path))

    kline_4h = FUNC.filt_interval_close_df(
        FUNC.load_csv_data(kline_path), interval="4H"
    )

    kline_df = FUNC.add_vol(kline_4h, vol_abs_th=VOL_ABS)

    funding_evt = sum_funding_kline(funding_df, kline_df)

    min_abs_funding_list = make_funding_thresholds(
        funding_evt,
        quantiles=np.linspace(0.6, 0.95, 15),
    )

    grid_result = grid_search_funding_carry(
        funding_evt=funding_evt,
        notional_usdt=NOTIONAL,
        fee_bps_roundtrip=FEE_BPS_ROUNDTRIP,
        min_abs_funding_list=min_abs_funding_list,
        confirm_n_list=CONFIRM_N_LIST,
    )

    out_path = os.path.join(
        OUT_DIR,
        "signal_future_funding_grid_result.csv",
    )
    grid_result.to_csv(out_path, index=False)
    print(f"[OK] saved: {out_path}")


if __name__ == "__main__":
    main()
