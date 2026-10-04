"""各数据集真实起点/终点 (只读 parquet 元数据统计, 不载数据, 秒级)。"""
import os
import sys

import pandas as pd
import pyarrow.parquet as pq

ROOT = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, ROOT)
from data_foundation.config import CERTIFIED_DIR  # noqa: E402

DS = {
    "现货1h": "market_candle_spot_1h",
    "现货4h": "market_candle_spot_4h",
    "永续1h": "market_candle_perpetual_1h",
    "资金费率": "derivatives_funding",
    "未平仓OI": "derivatives_open_interest",
    "标记价": "derivatives_mark_price",
    "指数价": "derivatives_index_price",
    "基差": "basis_1h",
    "多空比": "derivatives_ratio_glsr",
    "跨所OI": "derivatives_oi_cross",
    "稳定币供应": "stablecoin_supply",
    "稳定币锚定": "stablecoin_peg",
    "稳定币流向": "stablecoin_flows",
    "恐惧贪婪": "sentiment_fng",
    "宏观": "macro_daily",
    "BTC网络": "btc_network_daily",
    "CM资产": "cm_asset_daily",
    "链上聚合": "onchain_daily_aggregate",
    "链上转账": "token_transfer",
    "DEX量": "dex_volume",
    "上市宇宙": "listing_universe",
    "三层宇宙": "universe_membership",
    "资产主档": "asset_master",
    "交易对元数据": "instrument",
}
PREF = ("open_time_utc", "funding_time_utc", "timestamp_utc", "date_utc",
        "time_utc", "block_timestamp_utc", "data_available_at")


def mm(p):
    """从 parquet 元数据统计读时间列 min/max (无统计则退回读该列)。"""
    pf = pq.ParquetFile(p)
    names = pf.schema_arrow.names
    c = next((x for x in PREF if x in names), None)
    if c is None:
        return None
    ci = names.index(c)
    vals = []
    md = pf.metadata
    ok = True
    for rg in range(md.num_row_groups):
        cc = md.row_group(rg).column(ci)
        if not cc.is_stats_set:
            ok = False
            break
        s = cc.statistics
        if s.min is not None:
            vals.append(s.min)
        if s.max is not None:
            vals.append(s.max)
    if ok and vals:
        return pd.to_datetime(min(vals), utc=True), pd.to_datetime(max(vals), utc=True)
    t = pd.read_parquet(p, columns=[c])[c].dropna()
    if len(t) == 0:
        return None
    return pd.to_datetime(t.min(), utc=True), pd.to_datetime(t.max(), utc=True)


res = {}
print(f"{'因子':<14}{'数据集':<32}{'起点':<12}{'终点':<12}{'天数':>7}")
print("-" * 78)
for label, ds in DS.items():
    root = os.path.join(CERTIFIED_DIR, ds)
    if not os.path.isdir(root):
        continue
    mn = mx = None
    for dp, _dn, fns in os.walk(root):
        if "data.parquet" not in fns:
            continue
        try:
            r = mm(os.path.join(dp, "data.parquet"))
        except Exception:
            continue
        if not r:
            continue
        mn = r[0] if mn is None or r[0] < mn else mn
        mx = r[1] if mx is None or r[1] > mx else mx
    if mn is None:
        continue
    res[label] = (mn, mx)
    print(f"{label:<14}{ds:<32}{str(mn)[:10]:<12}{str(mx)[:10]:<12}{(mx-mn).days:>7}")


def inter(labels):
    starts = [res[l][0] for l in labels if l in res]
    return max(starts) if starts else None


print("\n=== 因子组合可用性交集 (起点=最晚因子) ===")
for name, labels in [
    ("现货+宏观+情绪", ["现货1h", "宏观", "恐惧贪婪"]),
    ("+稳定币供应", ["现货1h", "宏观", "恐惧贪婪", "稳定币供应"]),
    ("+永续衍生品", ["现货1h", "宏观", "恐惧贪婪", "永续1h", "资金费率"]),
    ("+链上聚合", ["现货1h", "宏观", "链上聚合"]),
    ("+链上转账", ["现货1h", "宏观", "链上转账"]),
]:
    s = inter(labels)
    print(f"{name:<20} 起点 = {str(s)[:10] if s else '无'}")