# -*- coding: utf-8 -*-
"""
_J3_verify.py — 子代理 J3 收尾验证
==================================
独立重算 (不依赖运行 state):
  * 每数据集: 有 L2 certified 的 symbol 数 / 总行数 / 覆盖区间 / suspect 数
  * 抽样 3 个新币: 资金费起点与行数
  * 失败清单 (读 _J3_state.json)
  * 磁盘用量
"""
from __future__ import annotations

import json
import os
import sys

import pandas as pd

HERE = os.path.dirname(os.path.abspath(__file__))
if HERE not in sys.path:
    sys.path.insert(0, HERE)
os.chdir(HERE)

from data_foundation.config import CERTIFIED_DIR  # noqa: E402
from data_foundation import l1 as l1m  # noqa: E402

import pyarrow.parquet as pq  # noqa: E402

DATASETS = {
    "derivatives_funding": ("binance", None, "funding_time_utc"),
    "derivatives_mark_price": ("binance", None, "open_time_utc"),
    "derivatives_index_price": ("binance", None, "open_time_utc"),
    "market_candle_perpetual_1h": ("binance", "perpetual", "open_time_utc"),
}
SAMPLES = ["ALGOUSDT", "ZECUSDT", "SUIUSDT"]


def main():
    targets = [s.strip() for s in open("_J3_targets.csv", encoding="utf-8")
               if s.strip() and not s.startswith("symbol")]
    print(f"目标 symbol: {len(targets)}\n")

    print("==== 每数据集汇总 (L2 certified, 仅 J3 目标 symbol) ====")
    for ds, (venue, mtype, tc) in DATASETS.items():
        root = os.path.join(CERTIFIED_DIR, ds, venue)
        if not os.path.isdir(root):
            print(f"  {ds}: (无目录)"); continue
        have = 0
        total = 0
        suspect = 0
        start = end = None
        for inst_dir in sorted(os.listdir(root)):
            inst_path = os.path.join(root, inst_dir)
            if not os.path.isdir(inst_path):
                continue
            if mtype:
                cand_path = os.path.join(inst_path, "interval=1h", "data.parquet")
            else:
                cand_path = os.path.join(inst_path, "data.parquet")
            if not os.path.isfile(cand_path):
                continue
            df = pq.read_table(cand_path, columns=[tc, "is_suspect"]).to_pandas()
            ts = pd.to_datetime(df[tc], utc=True)
            have += 1
            total += len(df)
            suspect += int(df["is_suspect"].sum())
            s0, s1 = ts.min(), ts.max()
            start = s0 if start is None or s0 < start else start
            end = s1 if end is None or s1 > end else end
        print(f"  {ds}: {have}/{len(targets)} symbol, {total:,} 行, "
              f"suspect={suspect}, {start} ~ {end}")

    print("\n==== 抽样 3 个新币: 资金费起点/行数 ====")
    for sym in SAMPLES:
        inst = l1m.instrument_id(sym)
        p = os.path.join(CERTIFIED_DIR, "derivatives_funding", "binance",
                         inst, "data.parquet")
        if not os.path.isfile(p):
            print(f"  {sym}: 无 certified funding"); continue
        df = pq.read_table(p).to_pandas()
        ts = pd.to_datetime(df["funding_time_utc"], utc=True)
        print(f"  {sym}: {len(df):,} 行, {ts.min()} ~ {ts.max()}")

    print("\n==== 运行状态 (failures) ====")
    if os.path.exists("_J3_state.json"):
        st = json.load(open("_J3_state.json", encoding="utf-8"))
        fails = st.get("failures", [])
        done4 = sum(1 for s in targets
                    if len(st.get("done", {}).get(s, [])) == 4)
        print(f"  四数据集齐全 symbol: {done4}/{len(targets)}")
        print(f"  failures: {len(fails)}")
        for f in fails[:40]:
            print(f"    {f['symbol']}/{f['dataset']}: {f['error'][:100]}")
    else:
        print("  (无 _J3_state.json)")

    print("\n==== 磁盘 ====")
    import shutil
    free = shutil.disk_usage(os.path.dirname(HERE)).free / 1e9
    print(f"  剩余 {free:.2f}GB")


if __name__ == "__main__":
    main()
