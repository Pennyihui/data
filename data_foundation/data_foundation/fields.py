# -*- coding: utf-8 -*-
"""fields.py — F2 字段层 + PIT 引擎

设计文档: docs/feature-foundation-design.md 第 7、8 节 (v0.4)

职责
----
1. **字段注册表** `FIELD_REGISTRY`: 特征表达式里可用的原始量 (close/funding_rate/...)
   到认证数据集列的映射, 含事件时间列与市场类型。
2. **面板装载** `load_panel`: 经 PoolScope (时间墙) 从 data_foundation.reader 取数,
   组成 MultiIndex (base_asset, time) 面板 —— 值面板 + data_available_at 面板。
3. **PIT 引擎** `propagate_availability`: 核心规则 ——
       特征在事件时间 t 的 data_available_at = 该特征所用全部输入中最大的
       data_available_at (按算子的取数窗口传播)。
   ts_ 算子 → 窗口内滚动 max; cs_ → 同刻截面 max; group_ → (time,group) 内 max;
   pp_ 点态 → 原值; ewma 等无界历史 → expanding max。
4. **泄漏自检** `assert_no_leakage`: 特征可用时间 >= 输入最大可用时间, 违反即抛错。

面板约定 (与 operators.py 一致)
------------------------------
* 索引 = (base_asset, time): operators 的 instrument 别名表认识 "base_asset",
  时序算子按资产分组、截面算子按 time 分组。用 base_asset 而不是 symbol 做
  资产主键, 是因为一个资产的现货与永续是两个 instrument (BTC-USDT vs
  BTC-USDT-SWAP), 而特征 (如基差) 天然要跨市场拼接; 每个资产的"主交易所"
  按 PIT 快照里近30日成交额最高的 venue 确定 (不手工维护)。
* 时间格语义 = **bar 数**: 每 (asset, time) 行只在真实 bar 存在时才出现
  (不填充完整日历网格)。ts_mean(x, 24) 即"最近 24 根 bar", 缺 bar 时窗口
  跨更长的墙钟时间 —— WorldQuant/qlib 的 bar 语义, 显式声明而非隐含。
* 预热窗口 warmup: 池边界是**研究边界**不是数据边界 —— 计算池起点处的
  ts_zscore(x, 168) 必须能读池起点之前 168h 的历史 (那是更早的数据, 不是
  另一个池)。load_panel 用 warmup 参数显式声明预热, 默认 0 (严格)。
"""
from __future__ import annotations

from dataclasses import dataclass, field as dc_field
from typing import Iterable

import numpy as np
import pandas as pd

from .pool_registry import PoolScope, factor_available
from . import reader
from .features.operators import (
    _shift,
    group_codes,
    inst_codes,
    time_level,
    ts_sum,
)

__all__ = [
    "FieldSpec", "FIELD_REGISTRY", "list_fields", "get_field", "Panel",
    "load_panel", "propagate_availability", "assert_no_leakage",
    "avail_ts_max", "avail_cs_max", "avail_group_max", "avail_expanding_max",
]

# 时间墙已判死的源 (修订污染 / 无 PIT 列) —— 字段层再拦一道, 报错更早更清楚
_BLOCKED_DATASETS = ("macro_daily", "cm_asset_daily", "btc_network_daily",
                     "stablecoin_supply", "stablecoin_flows", "dex_volume")


# ---------------------------------------------------------------------------
# 字段注册表
# ---------------------------------------------------------------------------
@dataclass(frozen=True)
class FieldSpec:
    """一个可取数原始量的完整描述。"""

    name: str              # 表达式里的字段名 (唯一)
    dataset: str           # 认证数据集 (market_candle_spot_1h / derivatives_funding ...)
    column: str            # 值列名
    time_column: str       # 事件时间列 (open_time_utc / funding_time_utc / timestamp_utc)
    market_type: str       # spot | perpetual (决定宇宙行与取数路径)
    desc: str = ""
    quality_flag: bool = False   # is_gap / is_suspect 这类布尔质量位
    grid: str = "1h"       # 事件时间对齐网格 (funding 8h 结算带毫秒抖动, 地板到小时)

    @property
    def family(self) -> str:
        if self.dataset.startswith("market_candle"):
            return "price"
        if self.dataset.startswith(("derivatives", "basis")):
            return "derivatives"
        return self.dataset


_CANDLE_TIME = "open_time_utc"

_FIELDS: list[FieldSpec] = [
    # -- 现货 1h K线 -------------------------------------------------------
    FieldSpec("open", "market_candle_spot_1h", "open", _CANDLE_TIME, "spot", "现货开盘价"),
    FieldSpec("high", "market_candle_spot_1h", "high", _CANDLE_TIME, "spot", "现货最高价"),
    FieldSpec("low", "market_candle_spot_1h", "low", _CANDLE_TIME, "spot", "现货最低价"),
    FieldSpec("close", "market_candle_spot_1h", "close", _CANDLE_TIME, "spot", "现货收盘价"),
    FieldSpec("volume_base", "market_candle_spot_1h", "volume_base", _CANDLE_TIME, "spot", "现货成交量(币)"),
    FieldSpec("volume_quote", "market_candle_spot_1h", "volume_quote", _CANDLE_TIME, "spot", "现货成交额(计价)"),
    FieldSpec("trade_count", "market_candle_spot_1h", "trade_count", _CANDLE_TIME, "spot", "现货成交笔数"),
    FieldSpec("taker_buy_volume_base", "market_candle_spot_1h", "taker_buy_volume_base", _CANDLE_TIME, "spot", "现货主动买入量(币)"),
    FieldSpec("taker_buy_volume_quote", "market_candle_spot_1h", "taker_buy_volume_quote", _CANDLE_TIME, "spot", "现货主动买入额(计价)"),
    FieldSpec("is_gap", "market_candle_spot_1h", "is_gap", _CANDLE_TIME, "spot", "现货K线缺口位 (质量位)", quality_flag=True),
    FieldSpec("is_suspect", "market_candle_spot_1h", "is_suspect", _CANDLE_TIME, "spot", "现货K线质量可疑位", quality_flag=True),
    # -- 永续 1h K线 (p_ 前缀) ---------------------------------------------
    FieldSpec("p_open", "market_candle_perpetual_1h", "open", _CANDLE_TIME, "perpetual", "永续开盘价"),
    FieldSpec("p_high", "market_candle_perpetual_1h", "high", _CANDLE_TIME, "perpetual", "永续最高价"),
    FieldSpec("p_low", "market_candle_perpetual_1h", "low", _CANDLE_TIME, "perpetual", "永续最低价"),
    FieldSpec("p_close", "market_candle_perpetual_1h", "close", _CANDLE_TIME, "perpetual", "永续收盘价"),
    FieldSpec("p_volume_base", "market_candle_perpetual_1h", "volume_base", _CANDLE_TIME, "perpetual", "永续成交量(币)"),
    FieldSpec("p_volume_quote", "market_candle_perpetual_1h", "volume_quote", _CANDLE_TIME, "perpetual", "永续成交额(计价)"),
    FieldSpec("p_trade_count", "market_candle_perpetual_1h", "trade_count", _CANDLE_TIME, "perpetual", "永续成交笔数"),
    FieldSpec("p_taker_buy_volume_quote", "market_candle_perpetual_1h", "taker_buy_volume_quote", _CANDLE_TIME, "perpetual", "永续主动买入额(计价)"),
    # -- 衍生品 -------------------------------------------------------------
    FieldSpec("funding_rate", "derivatives_funding", "funding_rate", "funding_time_utc", "perpetual", "资金费率 (结算时点值)"),
    FieldSpec("funding_mark_price", "derivatives_funding", "mark_price_at_funding", "funding_time_utc", "perpetual", "资金费结算时标记价"),
    FieldSpec("oi_contracts", "derivatives_open_interest", "open_interest_contracts", "timestamp_utc", "perpetual", "未平仓合约量(张)"),
    FieldSpec("oi_notional", "derivatives_open_interest", "open_interest_notional", "timestamp_utc", "perpetual", "未平仓合约名义值"),
    FieldSpec("mark_open", "derivatives_mark_price", "mark_open", _CANDLE_TIME, "perpetual", "标记价开"),
    FieldSpec("mark_high", "derivatives_mark_price", "mark_high", _CANDLE_TIME, "perpetual", "标记价高"),
    FieldSpec("mark_low", "derivatives_mark_price", "mark_low", _CANDLE_TIME, "perpetual", "标记价低"),
    FieldSpec("mark_close", "derivatives_mark_price", "mark_close", _CANDLE_TIME, "perpetual", "标记价收 (抗插针)"),
    FieldSpec("index_open", "derivatives_index_price", "index_open", _CANDLE_TIME, "perpetual", "指数价开"),
    FieldSpec("index_close", "derivatives_index_price", "index_close", _CANDLE_TIME, "perpetual", "指数价收"),
    FieldSpec("glsr", "derivatives_ratio_glsr", "long_short_ratio", "timestamp_utc", "perpetual", "全市场多空账户比"),
    FieldSpec("tlsr_acct", "derivatives_ratio_tlsr_acct", "long_short_ratio", "timestamp_utc", "perpetual", "大户多空账户比"),
    FieldSpec("tlsr_pos", "derivatives_ratio_tlsr_pos", "long_short_ratio", "timestamp_utc", "perpetual", "大户多空持仓比"),
    FieldSpec("taker_ratio", "derivatives_ratio_taker", "long_short_ratio", "timestamp_utc", "perpetual", "主动买卖比 (taker)"),
    FieldSpec("basis", "basis_1h", "basis", _CANDLE_TIME, "perpetual", "基差 (永续/现货-1, 派生)"),
]

FIELD_REGISTRY: dict[str, FieldSpec] = {f.name: f for f in _FIELDS}


def list_fields(family: str | None = None) -> list[dict]:
    """字段目录 (给 MCP / 特征库展示)。"""
    out = []
    for name, f in FIELD_REGISTRY.items():
        if family is not None and f.family != family:
            continue
        out.append({"name": name, "dataset": f.dataset, "column": f.column,
                    "market_type": f.market_type, "family": f.family,
                    "desc": f.desc, "quality_flag": f.quality_flag})
    return out


def get_field(name: str) -> FieldSpec:
    if name not in FIELD_REGISTRY:
        raise KeyError(f"未知字段 {name!r}; 见 list_fields() "
                       f"(共 {len(FIELD_REGISTRY)} 个)")
    return FIELD_REGISTRY[name]


# ---------------------------------------------------------------------------
# 面板
# ---------------------------------------------------------------------------
@dataclass
class Panel:
    """字段面板: 值 + PIT 可用时间 (同形状)。

    values / avail 的索引都是 MultiIndex (base_asset, time), 列 = 字段名。
    provenance[(field, base_asset)] = (venue, instrument_id) 供血缘/审计。
    """

    values: pd.DataFrame
    avail: pd.DataFrame
    scope: PoolScope
    as_of: pd.Timestamp
    start: pd.Timestamp
    end: pd.Timestamp
    warmup: pd.Timedelta
    provenance: dict = dc_field(default_factory=dict)
    excluded: list = dc_field(default_factory=list)

    @property
    def fields(self) -> list[str]:
        return list(self.values.columns)

    def field(self, name: str) -> pd.Series:
        if name not in self.values.columns:
            raise KeyError(f"面板中没有字段 {name!r}; 有: {self.fields}")
        return self.values[name]

    def avail_of(self, name: str) -> pd.Series:
        if name not in self.avail.columns:
            raise KeyError(f"面板中没有字段 {name!r}; 有: {self.fields}")
        return self.avail[name]

    def __repr__(self) -> str:
        return (f"<Panel {self.values.shape[0]:,} 行 x {len(self.fields)} 字段 "
                f"[{self.scope.pool_id}] {self.start.date()} ~ {self.end.date()} "
                f"warmup={self.warmup}>")


def _utc(ts) -> pd.Timestamp:
    """任意时间戳 -> UTC aware (naive 视为 UTC), 不接受非时区对象。"""
    t = pd.Timestamp(ts)
    return t.tz_localize("UTC") if t.tzinfo is None else t.tz_convert("UTC")


def _primary_venue(uni: pd.DataFrame, market_type: str) -> pd.DataFrame:
    """每个 base_asset 在给定 market_type 下的主交易所: 按 PIT 快照里的
    近30日日均成交额取最高 (确定性规则, 不手工维护)。"""
    m = uni[uni["market_type"] == market_type]
    if m.empty:
        return m
    m = m.sort_values(["base_asset", "avg_volume_30d_usd"], ascending=[True, False])
    return m.groupby("base_asset", as_index=False, sort=False).head(1)


def load_panel(scope: PoolScope, fields: Iterable[str],
               assets: Iterable[str] | None = None,
               start=None, end=None, as_of=None,
               warmup: str | pd.Timedelta = "0h") -> Panel:
    """经时间墙装载字段面板 (唯一入口)。

    scope        : PoolScope —— 必填, 没有作用域就没有数据 (墙在 API 层)
    fields       : 字段名列表 (FIELD_REGISTRY 的键); 污染源在 pool_registry
                   因子屏蔽层直接拒绝
    assets       : base_asset 列表; 缺省 = 本池宇宙 (宇宙门控)
    start/end    : 研究窗口 [start, end], 双向钳制到池内; 缺省 = 整个池
    as_of        : PIT 时点, 钳制到 min(end, 池终点); 缺省 = end
    warmup       : 预热窗口 (Timedelta 或如 "168h"), 用于池起点处的滚动窗口;
                   预热行在 start 之前, 属于更早的历史 (不是别的池)
    """
    fspecs = [get_field(f) for f in fields]
    if len({f.name for f in fspecs}) != len(fspecs):
        raise ValueError("fields 里有重复项")
    for f in fspecs:
        if f.dataset in _BLOCKED_DATASETS:
            raise ValueError(f"字段 {f.name} 来自 {f.dataset}: 修订污染/无 PIT 列, "
                             f"被时间墙硬屏蔽 (见 pool_registry)")

    # 窗口钳制 = 与池**求交集** (不是逐端点 clamp): PoolScope.clamp 是点时间
    # 语义 (as_of 只能往前), 对区间会把 start=2026-09-01 钳到池终点而让窗口
    # 塌成空 —— 区间要的是 max(pool.start, start) ~ min(pool.end, end)。
    start = _utc(scope.pool.start_ts if start is None else start)
    end = _utc(scope.pool.end_ts if end is None else end)
    start = max(start, scope.pool.start_ts)
    if end is not None and scope.pool.end_ts is not None:
        end = min(end, scope.pool.end_ts)
    if end is None:
        raise ValueError(f"开放池 {scope.pool_id} 必须显式指定 end")
    if start > end:
        raise ValueError(
            f"研究窗口与池 {scope.pool_id} 无交集: 请求 [{start} ~ {end}] vs 池 "
            f"[{scope.pool.start_ts.date()} ~ "
            f"{scope.pool.end_ts.date() if scope.pool.end_ts else 'open'}]")
    as_of = end if as_of is None else _utc(as_of)
    as_of = scope.clamp(min(as_of, end))
    if isinstance(warmup, str):
        warmup = pd.Timedelta(warmup)
    if warmup < pd.Timedelta(0):
        raise ValueError("warmup 不能为负")
    if start > end:
        raise ValueError(f"start({start}) > end({end})")

    uni = reader.load_universe(as_of=as_of, layer=scope.layer)
    if uni.empty:
        raise ValueError(f"宇宙快照为空 (as_of={as_of}, layer={scope.layer})")

    # 宇宙/资产主数据里的 symbol 是交易所原始符号 (BTCUSDT), 认证 parquet 路径
    # 用 instrument_id (BTC-USDT) —— 经 instrument 元数据桥接。注意用**最新**
    # 快照: instrument 元数据的 data_available_at 是构建时刻 (2026-08), 按历史
    # as_of 过滤会全空; 而 symbol->instrument_id 是确定性的命名方案, 不是时变
    # 研究数据, 用最新映射不影响 PIT (事件时间由数据行自己的 data_available_at
    # 决定, 与路径命名无关)。
    inst = reader.load_instruments()
    sym2inst = {(r.venue_id, r.market_type, r.symbol): r.instrument_id
                for r in inst.itertuples(index=False)}

    if assets is not None:
        allowed = set(assets)
        uni = uni[uni["base_asset"].isin(allowed)]
        missing = allowed - set(uni["base_asset"])
        if missing:
            raise ValueError(f"资产不在本池宇宙 (as_of={as_of.date()}, "
                             f"layer={scope.layer}): {sorted(missing)[:8]} ...")

    lo = start - warmup
    values_parts: dict[str, list[pd.Series]] = {}
    avail_parts: dict[str, list[pd.Series]] = {}
    provenance: dict = {}
    excluded: list = []

    # 按 (market_type, dataset) 分组装载; 每个资产只读一次 parquet
    primary_cache: dict[str, pd.DataFrame] = {}
    for f in fspecs:
        if f.market_type not in primary_cache:
            primary_cache[f.market_type] = _primary_venue(uni, f.market_type)
            if primary_cache[f.market_type].empty and "spot" not in primary_cache:
                primary_cache["spot"] = _primary_venue(uni, "spot")
        primary = primary_cache[f.market_type]
        if primary.empty:
            # 宇宙快照目前只含现货 (universe_membership 由 Binance Vision 现货
            # 归档构建) —— 永续/衍生品字段沿用同一资产的现货主交易所 (同所同
            # 资产的衍生品市场); instrument 解析在 requested market_type 下做。
            primary = primary_cache.get("spot")
        if primary is None or primary.empty:
            excluded.append((f.name, "*", f"宇宙中无 {f.market_type} 行"))
            continue
        for row in primary.itertuples(index=False):
            instrument_id = sym2inst.get((row.venue_id, f.market_type, row.symbol))
            if instrument_id is None:
                excluded.append((f.name, row.base_asset, row.venue_id,
                                 f"instrument 元数据缺 {row.symbol}"))
                continue
            df = _read_field(f.dataset, row.venue_id, instrument_id, f, as_of, lo, end)
            if df is None or df.empty:
                excluded.append((f.name, row.base_asset, row.venue_id, "窗口内无数据"))
                continue
            asset = row.base_asset
            idx = pd.MultiIndex.from_arrays(
                [[asset] * len(df), df["time"].to_numpy()],
                names=["base_asset", "time"])
            s = pd.Series(df["value"].to_numpy(), index=idx, name=f.name)
            a = pd.Series(df["data_available_at"].to_numpy(), index=idx,
                          name=f.name)
            values_parts.setdefault(f.name, []).append(s)
            avail_parts.setdefault(f.name, []).append(a)
            provenance[(f.name, asset)] = (row.venue_id, instrument_id)

    if not values_parts:
        raise ValueError("面板为空: 所有字段/资产都无数据 "
                         f"(fields={list(fields)}, as_of={as_of}, "
                         f"window=[{lo} ~ {end}]) 排除明细: {excluded[:8]}")

    values = pd.DataFrame({k: pd.concat(v).sort_index()
                           for k, v in values_parts.items()})
    avail = pd.DataFrame({k: pd.concat(v).sort_index()
                          for k, v in avail_parts.items()})
    avail = avail.reindex(values.index)     # 外连接对齐; 缺行 = NaT (泄漏自检会拦)
    # 质量位 (is_gap/is_suspect) 转 float64: 外连接引入 NaN 时 bool 列会被 pandas
    # 升成 object dtype, 进而让 pp_is_outlier 之类的算子收到混合类型序列;
    # 质量位作为特征本就是 1.0/0.0, 存 float 语义一致。
    for f in fspecs:
        if f.quality_flag and f.name in values.columns:
            values[f.name] = values[f.name].astype(float)
    return Panel(values=values, avail=avail, scope=scope, as_of=as_of,
                 start=start, end=end, warmup=warmup,
                 provenance=provenance, excluded=excluded)


def _read_field(ds: str, venue: str, symbol: str, spec: FieldSpec,
                as_of: pd.Timestamp, lo: pd.Timestamp, hi: pd.Timestamp):
    """经 reader 取一个字段的 [time, value, data_available_at]。

    只走 data_foundation.reader, 不直接碰 parquet (架构边界)。
    as_of 的 PIT 过滤在 reader 内完成; lo/hi 是研究窗口 + 预热 (按事件时间)。
    缺 data_available_at 列的数据集直接报错 (PIT 硬要求)。
    """
    if ds.startswith("market_candle"):
        interval = ds.rsplit("_", 1)[1]
        try:
            df = reader.load_candles(venue, symbol, interval, as_of=as_of,
                                     market_type=spec.market_type)
        except FileNotFoundError:
            return None                     # 该 (数据集, 交易所, 资产) 无数据
    else:
        try:
            df = reader.load_derivatives(venue, symbol, ds, as_of=as_of)
        except FileNotFoundError:
            return None
    for c in (spec.column, spec.time_column, "data_available_at"):
        if c not in df.columns:
            raise KeyError(f"数据集 {ds} 缺列 {c!r} (PIT 引擎要求三列齐全)")
    out = df[[spec.time_column, spec.column, "data_available_at"]].copy()
    out.columns = ["time", "value", "data_available_at"]
    out = out.dropna(subset=["value"]).sort_values("time")
    # 事件时间对齐到字段网格 (funding 8h 结算实测带毫秒抖动, 如 08:00:00.001,
    # 不对齐会令跨数据集拼接多出"幽灵时间行"); data_available_at 保持原值。
    out["time"] = out["time"].dt.floor(spec.grid)
    out = out.drop_duplicates(subset=["time"], keep="last")   # 同刻取最后版本
    out = out[(out["time"] >= lo) & (out["time"] <= hi)]
    return out.reset_index(drop=True)


# ---------------------------------------------------------------------------
# PIT 引擎: 可用时间传播
# ---------------------------------------------------------------------------
def _to_ns(s: pd.Series) -> pd.Series:
    """datetime64[ns, UTC] -> int64 纳秒 (NaT 保持 INT64_MIN, 在 max 里垫底)。"""
    v = s.astype("int64").to_numpy(dtype=np.int64, copy=True)
    return pd.Series(v, index=s.index)


def _from_ns(s: pd.Series) -> pd.Series:
    """int64 纳秒 -> datetime64[ns, UTC] (INT64_MIN 还原成 NaT)。"""
    v = s.to_numpy(dtype=np.int64, copy=True)
    return pd.Series(pd.to_datetime(v, utc=True), index=s.index)


def avail_ts_max(avail: pd.Series, window: int) -> pd.Series:
    """ts_ 算子输出的可用时间 = trailing 窗口内输入可用时间的最大值。

    实现: **int64 纳秒**上的 w-1 次 shift 逐元素 np.maximum —— 精确且窗口封闭
    (每行输出只是窗口内值的函数)。不经 float64: ns 时间戳 ~1.7e18 超出
    float64 的 2^53 精确范围, 会丢 ~250ns, 可能把可用时间"提前"而违反
    "特征可用时间 >= 输入" 的不变量 (实测踩过 128ns 的坑)。
    NaT = INT64_MIN 垫底, 不抬高 max; 窗口内全 NaT 输出 NaT。
    """
    w = int(window)
    if w < 1:
        raise ValueError("window 必须 >= 1")
    acc = _to_ns(avail).to_numpy(dtype=np.int64, copy=True)
    for k in range(1, w):
        prev = _to_ns(_shift(avail, k)).to_numpy(dtype=np.int64)
        np.maximum(acc, prev, out=acc)
    n_valid = ts_sum(avail.notna().astype(float), w)
    return _from_ns(pd.Series(acc, index=avail.index)).where(n_valid > 0)


def avail_expanding_max(avail: pd.Series) -> pd.Series:
    """无界历史算子 (ewma adjust=False 等) 的可用时间 = 每资产历史以来 max。"""
    codes = inst_codes(avail)
    ns = _to_ns(avail)
    valid = avail.notna().astype(float)
    if codes is None:
        cm, vm = ns.cummax(), valid.cummax()
    else:
        cm = ns.groupby(codes, sort=False).cummax()
        vm = valid.groupby(codes, sort=False).cummax()
    return _from_ns(cm).where(vm > 0)


def avail_cs_max(avail: pd.Series) -> pd.Series:
    """cs_ 算子: 同一时刻截面内的最大可用时间 (pandas 原生 datetime max, 精确)。"""
    codes = pd.factorize(avail.index.get_level_values(time_level(avail)),
                         sort=False)[0]
    return avail.groupby(codes, sort=False).transform("max")


def avail_group_max(avail: pd.Series, g) -> pd.Series:
    """group_ 算子: (time, group) 内的最大可用时间 (原生 datetime max, 精确)。"""
    labels = g if isinstance(g, pd.Series) else pd.Series(
        np.asarray(g), index=avail.index)
    codes = group_codes(avail, labels.to_numpy())
    return avail.groupby(codes, sort=False).transform("max")


def propagate_availability(kind: str, avail: pd.Series, window: int | None = None,
                           g=None) -> pd.Series:
    """按算子族传播 data_available_at (PIT 引擎核心规则, 设计文档 7.1)。

    kind ∈ {"point", "ts", "ts_unbounded", "cs", "group"}:
      point        pp_ 点态变换          -> 原值
      ts           ts_ 滚动窗口算子      -> 窗口内滚动 max (需 window)
      ts_unbounded ts_ 无界历史 (ewma)   -> expanding max
      cs           cs_ 截面算子          -> 同刻截面 max
      group        group_ 分组算子       -> (time,group) 内 max (需 g)
    多输入算子 (ts_corr / 基差): 每个输入各自传播后按行取 max —— 用
    ``pd.concat([...], axis=1).max(axis=1)``。
    """
    if kind == "point":
        return avail
    if kind == "ts":
        if window is None or int(window) < 1:
            raise ValueError("kind='ts' 需要 window >= 1")
        return avail_ts_max(avail, int(window))
    if kind == "ts_unbounded":
        return avail_expanding_max(avail)
    if kind == "cs":
        return avail_cs_max(avail)
    if kind == "group":
        if g is None:
            raise ValueError("kind='group' 需要分组标签 g")
        return avail_group_max(avail, g)
    raise KeyError(f"未知传播类型 {kind!r}; 可用: point/ts/ts_unbounded/cs/group")


def assert_no_leakage(feature_avail: pd.Series, input_avails: dict[str, pd.Series],
                      name: str = "feature",
                      feature_values: pd.Series | None = None) -> pd.Series:
    """泄漏自检: 特征的 data_available_at 必须 >= 全部输入的最大可用时间。

    规则 (设计文档 7.1 / 7.3):
      1. 特征值**非 NaN** 的行必须有可用时间 (值是 NaN 的行没有计算结果,
         无 PIT 要求 —— 例如别的字段带来的联合网格行)。
      2. 特征可用时间 >= 各输入可用时间的按行最大值 (输入某行无数据 = NaT,
         跳过该输入)。
      3. 全部输入的可用时间**整体**为空 => 直接报错 (计算路径绕过了 PIT 引擎)。
      4. 违反即抛 AssertionError —— 引擎的最后闸门, 不许关掉。

    注 (为什么没有"逐行 ghost"规则): 窗口算子在该行输入缺失时仍可能有值
    (它用窗口里更早的 bar), 例如 ts_mean(is_gap, 24) 在缺 bar 的行依然出值 ——
    这种情况是合法的, 其可用时间已由规则 1/2 保证保守; 只有"全无输入可用
    时间"才是真的绕过引擎。

    返回输入的按行最大可用时间 (供血缘/审计记录)。
    """
    if not input_avails:
        raise ValueError("input_avails 为空: 没有输入就没有血缘, 无法验证 PIT")
    if feature_values is not None and not feature_values.index.equals(
            feature_avail.index):
        raise ValueError("feature_values 与 feature_avail 索引不一致")

    cols = {}
    for iname, av in input_avails.items():
        if not av.index.equals(feature_avail.index):
            av = av.reindex(feature_avail.index)
        cols[iname] = av
    wide = pd.DataFrame(cols)
    in_max = wide.max(axis=1)          # skipna: 某输入该行无数据则跳过
    in_max.name = "input_max_available_at"

    # 规则 3: 所有输入的可用时间整体为空
    if bool(wide.isna().all().all()):
        raise AssertionError(
            f"泄漏自检失败 ({name}): 全部输入的 data_available_at 都是 NaT —— "
            f"计算路径可能绕过了 PIT 引擎 (没有任何输入的可用时间)")

    if feature_values is None:
        has_value = feature_avail.notna()
    else:
        has_value = feature_values.notna() | feature_avail.notna()
        # 规则 1: 有值但无可用时间
        n_no_avail = int((feature_values.notna() & feature_avail.isna()).sum())
        if n_no_avail:
            raise AssertionError(
                f"泄漏自检失败 ({name}): {n_no_avail} 行特征有值但 "
                f"data_available_at = NaT —— 无可用时间的值无法证明不泄漏")

    viol = (feature_avail < in_max) & in_max.notna() & has_value
    if bool(viol.any()):
        n = int(viol.sum())
        gap = (in_max[viol] - feature_avail[viol]).max()
        raise AssertionError(
            f"泄漏自检失败 ({name}): {n} 行特征的 data_available_at 早于输入最大"
            f"可用时间 (最大缺口 {gap}); 特征可用时间必须 = 输入可用时间的最大值"
            f" (设计文档 7.1)")
    return in_max


if __name__ == "__main__":
    print(f"字段注册表: {len(FIELD_REGISTRY)} 个")
    for row in list_fields():
        flag = " [质量位]" if row["quality_flag"] else ""
        print(f"  {row['name']:26s} [{row['family']:12s}] {row['desc']}{flag}")
    blocked = [n for n, f in FIELD_REGISTRY.items() if f.dataset in _BLOCKED_DATASETS]
    print(f"\n被时间墙屏蔽的字段: {blocked or '无 (注册表本身不含污染源)'}")
