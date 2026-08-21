# -*- coding: utf-8 -*-
"""
deribit_pipeline.py — 阶段 5: Deribit 期权数据 L0 -> L1 -> L2 管线
===================================================================
数据集 (L2 certified):
  dvol_15m               DVOL 波动率指数 (BTC/ETH; 实测官方无 15M 粒度, 取 1H)
  options_chain_snapshot 期权链快照 (mark_iv/bid_iv/ask_iv/OI/量/underlying)
  options_term_structure 期限结构 (按到期日: ATM IV / 中位数 IV / 点数)
  options_skew           近月 skew (OTM put 平均 IV - OTM call 平均 IV)
  options_greeks         Black-Scholes delta/gamma/vega (r=0.05)

用法:
  python -m data_foundation.deribit_pipeline [--days 90] [--dvol-resolution 3600]
  python deribit_pipeline.py                 (自包含 sys.path)
环境:
  DERIBIT_PROXY=http://127.0.0.1:7897        (默认本地代理; 置空则直连)
"""
from __future__ import annotations

import argparse
import os
import sys

_HERE = os.path.dirname(os.path.abspath(__file__))
_PARENT = os.path.dirname(_HERE)
if _PARENT not in sys.path:
    sys.path.insert(0, _PARENT)

import numpy as np
import pandas as pd
import pyarrow.parquet as pq

# 绝对导入 (相对导入在 `python deribit_pipeline.py` 直跑时会失败;
# 上方 sys.path 引导使两种入口都可用)
from data_foundation.config import CERTIFIED_DIR
from data_foundation.derivatives import write_derivatives_parquet
from data_foundation.l2 import (build_dataset_manifest, certify_derivatives,
                                write_certified_derivatives)

try:  # scipy 可用时用精确 erf, 否则用 Abramowitz-Stegun 近似 (结果一致到 ~1e-7)
    from scipy.special import erf as _erf
except Exception:  # noqa: BLE001
    def _erf(x):
        # 基于 erf 定义与 A&S 26.2.17 的 N(x) 近似反解, 足够用于 Greeks
        t = 1.0 / (1.0 + 0.2316419 * np.abs(x))
        d = 0.3989422804014327 * np.exp(-0.5 * x * x)
        poly = ((((1.330274429 * t - 1.821255978) * t + 1.781477937) * t
                 - 0.356563782) * t + 0.319381530) * t
        n = np.where(x >= 0, 1.0 - d * poly, d * poly)
        return 2.0 * n - 1.0


def _norm_cdf(x):
    return 0.5 * (1.0 + _erf(x / np.sqrt(2.0)))


def build_term_structure(chain: pd.DataFrame) -> pd.DataFrame:
    """期限结构: 每个 (currency, snapshot, expiry) 的 ATM IV / 中位 IV / 点数。

    ATM IV = 行权价最接近 underlying 的期权 mark_iv (min |strike - underlying|)。
    """
    recs = []
    for (cur, snap), g in chain.groupby(["currency", "snapshot_utc"], sort=False):
        for exp, h in g.groupby("expiry_utc", sort=False):
            valid = h.dropna(subset=["mark_iv"])
            if valid.empty:
                continue
            d = (valid["strike"] - valid["underlying_price"]).abs()
            idx = d.idxmin()
            atm = valid.loc[idx]
            recs.append({
                "venue_id": "deribit",
                "currency": cur,
                "snapshot_utc": snap,
                "expiry_utc": exp,
                "days_to_expiry": int((exp - snap).days),
                "atm_iv": float(atm["mark_iv"]),
                "atm_strike": float(atm["strike"]),
                "atm_cp": str(atm["cp"]),
                "median_iv": float(valid["mark_iv"].median()),
                "option_count": int(len(valid)),
                "data_available_at": snap,
                "source_batch_id": "deribit_term_structure_v1",
            })
    return pd.DataFrame(recs)


def build_skew(chain: pd.DataFrame) -> pd.DataFrame:
    """skew: 近 2 个到期日, OTM put (strike<underlying 且 |moneyness|<=10%)
    平均 mark_iv 减 OTM call 平均 mark_iv。"""
    recs = []
    for (cur, snap), g in chain.groupby(["currency", "snapshot_utc"], sort=False):
        g = g.dropna(subset=["mark_iv", "underlying_price"])
        if g.empty:
            continue
        expiries = sorted(g["expiry_utc"].unique())
        near = expiries[:2]
        h = g[g["expiry_utc"].isin(near)].copy()
        if h.empty:
            continue
        h["moneyness"] = (h["strike"] - h["underlying_price"]) / h["underlying_price"]
        h = h[h["moneyness"].abs() <= 0.10]
        puts = h[(h["cp"] == "P") & (h["strike"] < h["underlying_price"])]
        calls = h[(h["cp"] == "C") & (h["strike"] > h["underlying_price"])]
        if puts.empty or calls.empty:
            continue
        put_iv = float(puts["mark_iv"].mean())
        call_iv = float(calls["mark_iv"].mean())
        recs.append({
            "venue_id": "deribit",
            "currency": cur,
            "snapshot_utc": snap,
            "skew": put_iv - call_iv,
            "put_avg_iv": put_iv,
            "call_avg_iv": call_iv,
            "n_puts": int(len(puts)),
            "n_calls": int(len(calls)),
            "expiries_used": ",".join(e.strftime("%Y-%m-%d") for e in near),
            "data_available_at": snap,
            "source_batch_id": "deribit_skew_v1",
        })
    return pd.DataFrame(recs)


def build_greeks(chain: pd.DataFrame, r: float = 0.05) -> pd.DataFrame:
    """Black-Scholes Greeks (每张期权, 用 mark_iv 作为 sigma)。

    假设: r=0.05 常数 (按任务约定, 可 --risk-free-rate 覆盖);
    T = (expiry 08:00 UTC - snapshot) / 365.25 年;
    vega = S*phi(d1)*sqrt(T) 为每 1.0 绝对波动率单位 (每 1 vol point ≈ vega/100);
    T<=0 (已到期) 或无效 IV 的行剔除。
    """
    cols = ["venue_id", "currency", "instrument_name", "expiry_utc", "strike",
            "cp", "mark_iv", "underlying_price", "snapshot_utc"]
    df = chain[cols].copy()
    # 注意: 列名避免含 "time" 子串, 否则 l2.write_certified_derivatives 的
    # datetime 强转会把数值列转成 Timestamp (如 0.19 -> 1970-01-01)。
    df["years_to_expiry"] = (
        (df["expiry_utc"] - df["snapshot_utc"]).dt.total_seconds()
        / (365.25 * 24 * 3600))
    finite = np.isfinite(df[["mark_iv", "underlying_price", "strike"]].to_numpy())
    keep = (df["years_to_expiry"] > 0) & (df["mark_iv"] > 0) \
        & (df["underlying_price"] > 0) & (df["strike"] > 0) \
        & finite.all(axis=1)
    df = df[keep].copy()
    if df.empty:
        return df
    S = df["underlying_price"].to_numpy(dtype=float)
    K = df["strike"].to_numpy(dtype=float)
    T = df["years_to_expiry"].to_numpy(dtype=float)
    sig = df["mark_iv"].to_numpy(dtype=float)
    sq = sig * np.sqrt(T)
    d1 = (np.log(S / K) + (r + 0.5 * sig ** 2) * T) / sq
    n1 = _norm_cdf(d1)
    phi1 = np.exp(-0.5 * d1 * d1) / np.sqrt(2.0 * np.pi)
    is_call = (df["cp"].to_numpy() == "C")
    df["delta"] = np.where(is_call, n1, n1 - 1.0)
    df["gamma"] = phi1 / (S * sq)
    df["vega"] = S * phi1 * np.sqrt(T)
    df["data_available_at"] = df["snapshot_utc"]
    df["source_batch_id"] = "deribit_greeks_v1"
    df["theta"] = -(S * phi1 * sig) / (2.0 * np.sqrt(T))
    return df


def _stats(df: pd.DataFrame, time_col: str) -> dict:
    return {
        "row_count": int(len(df)),
        "duplicate_count": 0,
        "gap_count": 0,
        "suspect_count": int(df["is_suspect"].sum()) if "is_suspect" in df else 0,
        "coverage_start": str(df[time_col].min()),
        "coverage_end": str(df[time_col].max()),
    }


def run_pipeline(days: int = 90, dvol_resolution: int = 3600,
                 currencies=("BTC", "ETH"), r: float = 0.05) -> dict:
    from data_foundation.ingest_deribit import ingest_deribit_all
    from data_foundation.l1_deribit import (normalize_chain, normalize_dvol,
                                            normalize_index_price)

    print("== 阶段5: Deribit 期权数据 (L0 -> L1 -> L2) ==", flush=True)

    # ---------- L0 ----------
    print("-- L0 摄取 --", flush=True)
    ingest_deribit_all(days=days, dvol_resolution=dvol_resolution,
                       currencies=currencies)

    # ---------- L1 ----------
    print("-- L1 标准化 --", flush=True)
    dvol = normalize_dvol()
    if dvol.empty:
        raise RuntimeError("dvol L1 为空: 检查 L0 批次 (网络或 resolution 参数)")
    write_derivatives_parquet(dvol, "dvol_15m", "deribit", "all", "timestamp_utc")
    print(f"  dvol_15m: {len(dvol)} 行, {dvol['currency'].nunique()} 币, "
          f"{dvol['timestamp_utc'].min()} ~ {dvol['timestamp_utc'].max()}",
          flush=True)

    chain = normalize_chain()
    if chain.empty:
        raise RuntimeError("options_chain_snapshot L1 为空")
    write_derivatives_parquet(chain, "options_chain_snapshot", "deribit", "all",
                              "snapshot_utc")
    n_btc = int((chain["currency"] == "BTC").sum())
    n_eth = int((chain["currency"] == "ETH").sum())
    print(f"  options_chain_snapshot: {len(chain)} 行 "
          f"(BTC {n_btc} / ETH {n_eth}), snapshot={chain['snapshot_utc'].iloc[0]}",
          flush=True)

    idx = normalize_index_price()
    if not idx.empty:
        write_derivatives_parquet(idx, "index_price", "deribit", "all",
                                  "timestamp_utc")
        print("  index_price:", idx[["currency", "index_price"]].to_dict("records"),
              flush=True)

    # ---------- L2 认证 ----------
    print("-- L2 认证 --", flush=True)
    accum = {}

    def acc(ds, s):
        a = accum.setdefault(ds, {"row_count": 0, "duplicate_count": 0,
                                  "gap_count": 0, "suspect_count": 0,
                                  "coverage_start": None, "coverage_end": None})
        a["row_count"] += s["row_count"]
        a["duplicate_count"] += s["duplicate_count"]
        a["gap_count"] += s["gap_count"]
        a["suspect_count"] += s["suspect_count"]
        if a["coverage_start"] is None or s["coverage_start"] < a["coverage_start"]:
            a["coverage_start"] = s["coverage_start"]
        if a["coverage_end"] is None or s["coverage_end"] > a["coverage_end"]:
            a["coverage_end"] = s["coverage_end"]

    dvol_c = certify_derivatives(dvol, "timestamp_utc", core_numeric_cols=["dvol"],
                                 key_cols=["currency", "timestamp_utc"])
    write_certified_derivatives(dvol_c, "dvol_15m", "deribit", "all",
                                "timestamp_utc")
    acc("dvol_15m", _stats(dvol_c, "timestamp_utc"))
    print(f"  dvol_15m certified: {len(dvol_c)} 行, "
          f"suspect={int(dvol_c['is_suspect'].sum())}", flush=True)

    chain_c = certify_derivatives(chain, "snapshot_utc",
                                  core_numeric_cols=["mark_iv"],
                                  key_cols=["instrument_name", "snapshot_utc"])
    write_certified_derivatives(chain_c, "options_chain_snapshot", "deribit",
                                "all", "snapshot_utc")
    acc("options_chain_snapshot", _stats(chain_c, "snapshot_utc"))
    print(f"  options_chain_snapshot certified: {len(chain_c)} 行, "
          f"suspect={int(chain_c['is_suspect'].sum())}", flush=True)

    # ---------- 派生数据集 ----------
    print("-- 派生: 期限结构 / skew / Greeks --", flush=True)
    ts = build_term_structure(chain_c)
    if not ts.empty:
        ts_c = certify_derivatives(ts, "snapshot_utc",
                                   core_numeric_cols=["atm_iv", "median_iv"],
                                   key_cols=["currency", "snapshot_utc",
                                             "expiry_utc"])
        write_certified_derivatives(ts_c, "options_term_structure", "deribit",
                                    "all", "snapshot_utc")
        acc("options_term_structure", _stats(ts_c, "snapshot_utc"))
        print(f"  options_term_structure: {len(ts_c)} 行 "
              f"({ts_c['currency'].nunique()} 币, "
              f"{ts_c['expiry_utc'].nunique()} 个到期日)", flush=True)
    else:
        print("  [warn] term_structure 为空", flush=True)

    sk = build_skew(chain_c)
    if not sk.empty:
        sk_c = certify_derivatives(sk, "snapshot_utc",
                                   core_numeric_cols=["skew", "put_avg_iv",
                                                      "call_avg_iv"],
                                   key_cols=["currency", "snapshot_utc"])
        write_certified_derivatives(sk_c, "options_skew", "deribit", "all",
                                    "snapshot_utc")
        acc("options_skew", _stats(sk_c, "snapshot_utc"))
        print("  options_skew:", sk_c[["currency", "skew", "put_avg_iv",
                                       "call_avg_iv", "expiries_used"]]
              .to_dict("records"), flush=True)
    else:
        print("  [warn] skew 为空", flush=True)

    gk = build_greeks(chain_c, r=r)
    if not gk.empty:
        gk_c = certify_derivatives(gk, "snapshot_utc",
                                   core_numeric_cols=["delta", "gamma", "vega"],
                                   key_cols=["instrument_name", "snapshot_utc"])
        write_certified_derivatives(gk_c, "options_greeks", "deribit", "all",
                                    "snapshot_utc")
        acc("options_greeks", _stats(gk_c, "snapshot_utc"))
        print(f"  options_greeks: {len(gk_c)} 行 (delta∈"
              f"[{gk_c['delta'].min():.3f},{gk_c['delta'].max():.3f}], "
              f"vega∈[{gk_c['vega'].min():.1f},{gk_c['vega'].max():.1f}])",
              flush=True)
    else:
        print("  [warn] greeks 为空", flush=True)

    # ---------- manifest ----------
    rules = {
        "dvol_15m": {
            "note": "DVOL 波动率指数; 官方 get_volatility_index_data 的 "
                    "resolution 枚举仅 [1,60,3600,43200,'1D'] 秒 (无 15M), "
                    "当前取 3600s=1H; dvol=close/100 (百分数→小数); "
                    "历史深度实测多年可用, 默认回看 90 天"},
        "options_chain_snapshot": {
            "note": "get_book_summary_by_currency kind=option 全链快照; "
                    "bid_iv/ask_iv 该端点不提供→NaN; mark_iv=百分数/100; "
                    "expiry_utc=到期日 08:00 UTC; open_interest/volume 单位为币"},
        "options_term_structure": {
            "method": "按 (currency, snapshot_utc, expiry_utc) 聚合: "
                      "ATM IV=min|strike-underlying| 期权的 mark_iv, "
                      "median_iv=该到期所有有效 IV 中位数, "
                      "option_count=有效 IV 期权数",
            "inputs": ["options_chain_snapshot"]},
        "options_skew": {
            "method": "近 2 个到期日: OTM put(strike<underlying 且 "
                      "|moneyness|<=10%) 平均 mark_iv - OTM call 平均 mark_iv",
            "inputs": ["options_chain_snapshot"]},
        "options_greeks": {
            "method": "Black-Scholes: r=0.05 常数(可覆盖), "
                      "T=(expiry 08:00UTC - snapshot)/365.25 年, "
                      "sigma=mark_iv(小数); delta=N(d1)(call)/N(d1)-1(put), "
                      "gamma=phi(d1)/(S*sigma*sqrt(T)), "
                      "vega=S*phi(d1)*sqrt(T)(每 1.0 绝对波动率单位, "
                      "每 1 vol point≈vega/100); T<=0 或无效 IV 剔除",
            "inputs": ["options_chain_snapshot"]},
    }
    src_batches = {
        "dvol_15m": [f"dvol_{c.lower()}_v1" for c in currencies],
        "options_chain_snapshot": [f"chain_{c.lower()}_v1" for c in currencies],
        "options_term_structure": [f"chain_{c.lower()}_v1" for c in currencies],
        "options_skew": [f"chain_{c.lower()}_v1" for c in currencies],
        "options_greeks": [f"chain_{c.lower()}_v1" for c in currencies],
    }
    for ds, s in accum.items():
        mtype = "volatility" if ds == "dvol_15m" else "options"
        build_dataset_manifest(ds, "deribit", mtype, "all", "*", s,
                               src_batches.get(ds, []), rules.get(ds))
        print(f"  manifest {ds}: certified, {s['row_count']} 行, "
              f"coverage {s['coverage_start']} ~ {s['coverage_end']}", flush=True)

    print("-- 验证 --", flush=True)
    verify()
    return accum


def verify() -> None:
    """读取 certified 结果并打印验收要点。"""
    def load(ds):
        root = os.path.join(CERTIFIED_DIR, ds, "deribit", "all")
        df = pq.read_table(root).to_pandas()
        for c in df.columns:
            if "time" in c or c == "data_available_at":
                df[c] = pd.to_datetime(df[c], utc=True)
        return df

    dvol = load("dvol_15m")
    chain = load("options_chain_snapshot")
    print(f"[verify] dvol_15m: {len(dvol)} 行, "
          f"{dvol['currency'].nunique()} 币, "
          f"{dvol['timestamp_utc'].min()} ~ {dvol['timestamp_utc'].max()}, "
          f"dvol∈[{dvol['dvol'].min():.4f},{dvol['dvol'].max():.4f}]", flush=True)
    print(f"[verify] options_chain_snapshot: {len(chain)} 行 "
          f"(BTC {int((chain['currency']=='BTC').sum())} / "
          f"ETH {int((chain['currency']=='ETH').sum())}), "
          f"mark_iv∈[{chain['mark_iv'].min():.4f},{chain['mark_iv'].max():.4f}], "
          f"snapshot={chain['snapshot_utc'].iloc[0]}", flush=True)
    ts = load("options_term_structure")
    if not ts.empty:
        print("[verify] term_structure 样例 (按 currency 各取 3 个最近到期):",
              flush=True)
        for cur, g in ts.groupby("currency"):
            s = g.sort_values("expiry_utc").head(3)
            for _, row in s.iterrows():
                print(f"  {cur} {row['expiry_utc'].date()} "
                      f"days={row['days_to_expiry']} "
                      f"atm_iv={row['atm_iv']:.4f} "
                      f"median_iv={row['median_iv']:.4f} "
                      f"n={row['option_count']}", flush=True)
    sk = load("options_skew")
    if not sk.empty:
        print("[verify] skew:", sk[["currency", "skew", "put_avg_iv",
                                    "call_avg_iv", "expiries_used"]]
              .to_dict("records"), flush=True)
    gk = load("options_greeks")
    if not gk.empty:
        btc = gk[gk["currency"] == "BTC"].copy()
        if not btc.empty:
            btc["dist"] = (btc["strike"] - btc["underlying_price"]).abs()
            row = btc.loc[btc["dist"].idxmin()]  # 近 ATM 期权样例
            print(f"[verify] greeks 样例 (BTC 近ATM): "
                  f"{row['instrument_name']} snapshot={row['snapshot_utc']} "
                  f"S={row['underlying_price']:.2f} K={row['strike']:.0f} "
                  f"iv={row['mark_iv']:.4f} T={row['years_to_expiry']:.4f}y "
                  f"delta={row['delta']:.4f} gamma={row['gamma']:.6f} "
                  f"vega={row['vega']:.2f}", flush=True)
        for cur, g in gk.groupby("currency"):
            print(f"[verify] greeks {cur}: {len(g)} 行, delta∈"
                  f"[{g['delta'].min():.3f},{g['delta'].max():.3f}], "
                  f"gamma∈[{g['gamma'].min():.4f},{g['gamma'].max():.4f}], "
                  f"vega∈[{g['vega'].min():.1f},{g['vega'].max():.1f}]", flush=True)


def main():
    ap = argparse.ArgumentParser(
        description="阶段5: Deribit 期权数据 L0->L1->L2 管线")
    ap.add_argument("--days", type=int, default=90, help="DVOL 回看天数 (默认 90)")
    ap.add_argument("--dvol-resolution", type=int, default=3600,
                    help="DVOL 分辨率(秒): 60/3600/43200/86400, 默认 3600=1H "
                         "(官方枚举无 15M)")
    ap.add_argument("--currencies", default="BTC,ETH",
                    help="币种列表, 逗号分隔 (默认 BTC,ETH)")
    ap.add_argument("--risk-free-rate", type=float, default=0.05,
                    help="BS 无风险利率 (默认 0.05)")
    args = ap.parse_args()
    cur = tuple(c.strip().upper() for c in args.currencies.split(",") if c.strip())
    run_pipeline(days=args.days, dvol_resolution=args.dvol_resolution,
                 currencies=cur, r=args.risk_free_rate)
    print("阶段5 完成", flush=True)


if __name__ == "__main__":
    main()
