# -*- coding: utf-8 -*-
"""vision_to_raw.py — 把 Vision metrics 深回填的 L1 转成标准 raw 批次。

目的: run_daily rebuild 的 normalize_open_interest/normalize_ratio 只认
raw/binance/derivatives_{open_interest,ratio_*} 下的标准批次, 不认
futures_metrics 目录 -> 每晚 rebuild 会把 certified 冲回 21 天。
本脚本把 L1 全历史按 normalize 期望的 CSV 列名写回标准 raw 目录,
此后 rebuild 天然产出全史。幂等 (batch 已存在跳过)。

比率列约定: Vision 只有比值无多空账户数, 按 short=1, long=ratio 的
恒等式填充 (long/short == ratio 数学不变), 已在 meta 注明。
"""
import gzip
import os
import sys

HERE = r"D:\Documents\z_python_data_analy\Quent\workspace_0817\Data_pipeline\data_foundation"
sys.path.insert(0, HERE)
os.chdir(HERE)

import pandas as pd  # noqa: E402

from data_foundation.config import MVP_ASSETS, RAW_DIR  # noqa: E402
from data_foundation.l0 import list_raw_batches, write_raw_file  # noqa: E402

DATASETS = [
    ("derivatives_open_interest", "oi_vision_v3"),
    ("derivatives_ratio_glsr", "glsr_vision_v3"),
    ("derivatives_ratio_tlsr_acct", "tlsracct_vision_v3"),
    ("derivatives_ratio_tlsr_pos", "tlsrpos_vision_v3"),
    ("derivatives_ratio_taker", "taker_vision_v3"),
]


def make_out(df, ds):
    ts = pd.to_datetime(df["timestamp_utc"], utc=True)
    out = pd.DataFrame({"time": ts.dt.strftime("%Y-%m-%d %H:%M:%S")})
    if ds == "derivatives_open_interest":
        out["sumOpenInterest"] = pd.to_numeric(df["open_interest_contracts"],
                                               errors="coerce")
        out["sumOpenInterestValue"] = pd.to_numeric(df["open_interest_notional"],
                                                    errors="coerce")
    elif ds == "derivatives_ratio_taker":
        r = pd.to_numeric(df["long_short_ratio"], errors="coerce")
        out["buySellRatio"] = r
        out["sellVol"] = 1.0
        out["buyVol"] = r
    else:
        r = pd.to_numeric(df["long_short_ratio"], errors="coerce")
        out["longAccount"] = r
        out["longShortRatio"] = r
        out["shortAccount"] = 1.0
    val_cols = [c for c in out.columns if c != "time"]
    return out.dropna(subset=val_cols)


def main():
    n = 0
    for ds, suffix in DATASETS:
        for a in MVP_ASSETS:
            inst, sym = f"{a}-USDT", f"{a}USDT"
            batch = f"{sym}_{suffix}"
            if any(m.get("batch_id") == batch
                   for m in list_raw_batches("binance", ds)):
                continue
            p = os.path.join(HERE, "data", "l1", ds, "binance", inst,
                             "data.parquet")
            if not os.path.exists(p):
                print(f"[skip] {ds}/{inst}: 无 L1")
                continue
            df = pd.read_parquet(p)
            if df.empty:
                continue
            out = make_out(df, ds)
            tmp_dir = os.path.join(RAW_DIR, "_tmp")
            os.makedirs(tmp_dir, exist_ok=True)
            tmp = os.path.join(tmp_dir, f"{batch}.csv.gz")
            with gzip.open(tmp, "wt", encoding="utf-8", newline="\n") as f:
                out.to_csv(f, index=False)
            write_raw_file(tmp, "binance", ds, batch,
                           source={"api": "binance-vision metrics 回填转标准批次",
                                   "symbol": sym,
                                   "rows": int(len(out)),
                                   "note": "Vision 5min->1h 末值; "
                                           "ratio 恒等式填充(short=1,long=ratio)"},
                           timestamp_unit="ms", ext="csv.gz")
            os.remove(tmp)
            n += 1
            print(f"[ok] {ds}/{inst}: {len(out)} 行 -> {batch}")
    print(f"完成: 新写 {n} 个批次")


if __name__ == "__main__":
    main()
