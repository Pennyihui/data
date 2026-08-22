# -*- coding: utf-8 -*-
"""
metadata_pit.py — 时点元数据 (PIT, point-in-time instrument metadata)
======================================================================
把三所交易所的合约规格/状态元数据建成**快照历史** (每 (venue, symbol)
每次抓取追加一行, data_available_at = 抓取时刻), 支持 as-of 查询任意
历史时刻的合约规格/状态。

数据流:
  fetch (Binance 现货/永续 exchangeInfo, OKX SPOT+SWAP instruments,
         Coinbase products)
    -> 写 L0 raw 日批次 (dataset=instrument_metadata, 幂等)
    -> normalize 成 INSTRUMENT_COLUMNS 行 (含审计派生列 first/last_data_utc)
    -> 追加进 l1/instrument/{venue}/instruments.parquet (PIT 格式)
    -> certify -> l2/certified/instrument/{venue}/all/data.parquet + manifest

网络约定:
  * OKX 域名需本地代理 127.0.0.1:7897 (照抄 run_daily.py 的连接写法)。
  * Binance/Coinbase 优先直连 (api.binance.com 超时则回退官方镜像
    data-api.binance.vision); 本机直连不可达时最终回退本地代理
    (与 run_daily 的既有约定一致: 所有外网访问经 127.0.0.1:7897)。
  * 全部请求 8 次重试 + 退避 time.sleep(min(1.5*(i+1), 12)), 429 -> sleep 5。

用法:
  python -m data_foundation.metadata_pit
"""
from __future__ import annotations

import json
import os
import time
from datetime import datetime, timezone
from decimal import Decimal as _Decimal

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import requests

from .config import CERTIFIED_DIR, L1_DIR, RAW_DIR
from .l0 import list_raw_batches, write_raw_file
from .l1 import instrument_id
from .l2 import (build_dataset_manifest, certify_derivatives,
                 write_certified_derivatives)
from .schema import INSTRUMENT_COLUMNS

UA = {"User-Agent": "Mozilla/5.0 (data-foundation)"}
PROXY = "http://127.0.0.1:7897"
PROXIES = {"http": PROXY, "https": PROXY}
OKX_BASE = "https://www.okx.com/api/v5"
BINANCE_SPOT_BASES = ["https://api.binance.com", "https://data-api.binance.vision"]

# 直连会话 (trust_env=False: 忽略环境代理, 保证"直连"语义确定)
_DIRECT = requests.Session()
_DIRECT.trust_env = False

# 认证主键与核心数值列
KEY_COLS = ["venue_id", "symbol", "data_available_at"]
CORE_NUMERIC = ["contract_size", "tick_size", "lot_size", "min_order_size"]

# 抓取组定义: (venue, key, fetch_fn, api 描述)
GROUPS = [
    ("binance", "binance_spot", "fetch_binance_spot",
     "binance /api/v3/exchangeInfo (现货, 全 symbol)"),
    ("binance", "binance_perp", "fetch_binance_perp",
     "binance fapi /fapi/v1/exchangeInfo (USDT-M 永续, 全 symbol)"),
    ("okx", "okx", "fetch_okx",
     "okx /public/instruments (SPOT+SWAP)"),
    ("coinbase", "coinbase", "fetch_coinbase",
     "coinbase /products (全 products)"),
]

_CANDLE_COV = None  # (venue, market_type, instrument_id) -> (first_open, last_open)


# ============================================================
# HTTP — 统一走 netpath 四级链路 (vision直连 -> 7897域名 -> 专用端口 -> socks5/钉IP)
# ============================================================
def _fetch(url, params=None, retries=8, timeout=30, proxies=None,
           direct=False) -> object:
    """proxies/direct 参数仅为兼容旧调用签名保留, 实际通道选择全权交给 netpath。"""
    from . import netpath
    return netpath.fetch_json(url, params=params, retries=retries, timeout=timeout)


def _okx_get(path, params) -> list:
    j = _fetch(f"{OKX_BASE}{path}", params)
    if j.get("code") != "0":
        raise RuntimeError(f"OKX {path}: {j.get('msg')}")
    return j["data"]


# ============================================================
# fetch: 四组元数据
# ============================================================
def fetch_binance_spot() -> dict:
    """Binance 现货 exchangeInfo (全 symbol)。netpath 链内含 vision 镜像兜底。"""
    return _fetch("https://api.binance.com/api/v3/exchangeInfo", {})


def fetch_binance_perp() -> dict:
    """Binance USDT-M 永续 exchangeInfo (全 symbol)。"""
    return _fetch("https://fapi.binance.com/fapi/v1/exchangeInfo", {})


def fetch_okx() -> dict:
    """OKX SPOT + SWAP instruments。返回 {"spot": [...], "swap": [...]}。"""
    return {"spot": _okx_get("/public/instruments", {"instType": "SPOT"}),
            "swap": _okx_get("/public/instruments", {"instType": "SWAP"})}


def fetch_coinbase() -> list:
    """Coinbase products (全 products)。"""
    return _fetch("https://api.exchange.coinbase.com/products", None)


# ============================================================
# 辅助: 数值/小数位/对拆分
# ============================================================
def _fnum(x):
    try:
        return float(x)
    except (TypeError, ValueError):
        return float("nan")


def _decimals(x):
    """'0.01' -> 2; '0.10' -> 2; '1' -> 0; '1e-8' -> 8; 空 -> None。"""
    if x is None:
        return None
    s = str(x).strip()
    if not s:
        return None
    try:
        d = _Decimal(s)
    except Exception:  # noqa: BLE001
        return None
    if not d.is_finite():
        return None
    exp = d.as_tuple().exponent
    return int(-exp) if exp < 0 else 0


def _split_pair(s):
    """'BTC-USDT' -> ('BTC', 'USDT'); '1000SHIB-USDT' -> ('1000SHIB', 'USDT')。"""
    if not s:
        return "", ""
    parts = str(s).split("-", 1)
    if len(parts) == 2:
        return parts[0], parts[1]
    return str(s), ""


def _binance_filter(symbol: dict, ftype: str) -> dict:
    for f in symbol.get("filters", []):
        if f.get("filterType") == ftype:
            return f
    return {}


# ============================================================
# certified K 线覆盖 (first/last_data_utc 审计列 + 上市时间近似)
# ============================================================
def _candle_coverage() -> dict:
    """扫描 certified market_candle_spot_1h / market_candle_perpetual_1h,
    返回 {(venue, market_type, instrument_id): (first_open, last_open)}。"""
    global _CANDLE_COV
    if _CANDLE_COV is not None:
        return _CANDLE_COV
    cov = {}
    for ds in ("market_candle_spot_1h", "market_candle_perpetual_1h"):
        base = os.path.join(CERTIFIED_DIR, ds)
        if not os.path.isdir(base):
            continue
        for venue in sorted(os.listdir(base)):
            vd = os.path.join(base, venue)
            if not os.path.isdir(vd):
                continue
            # 目录: {venue}/{market_type}/{instrument}/interval=1h/data.parquet
            for mt in sorted(os.listdir(vd)):
                mtd = os.path.join(vd, mt)
                if not os.path.isdir(mtd):
                    continue
                for inst in sorted(os.listdir(mtd)):
                    p = os.path.join(mtd, inst, "interval=1h", "data.parquet")
                    if not os.path.exists(p):
                        continue
                    s = pq.read_table(p, columns=["open_time_utc"]) \
                          .column("open_time_utc").to_pandas()
                    if len(s):
                        cov[(venue, mt, inst)] = (s.min(), s.max())
    _CANDLE_COV = cov
    return cov


def _apply_coverage(df: pd.DataFrame, cov: dict) -> pd.DataFrame:
    df = df.copy()
    keys = list(zip(df["venue_id"], df["market_type"], df["instrument_id"]))
    first = [cov.get(k, (None, None))[0] for k in keys]
    last = [cov.get(k, (None, None))[1] for k in keys]
    df["first_data_utc"] = pd.to_datetime(first, utc=True)
    df["last_data_utc"] = pd.to_datetime(last, utc=True)
    return df


# ============================================================
# normalize: 每 venue 每 symbol 一行, 附本次快照时间
# ============================================================
def _base_cols(snapshot_at, batch_id, venue):
    return {"venue_id": venue, "data_available_at": snapshot_at,
            "source_batch_id": batch_id, "delisting_time": None}


def normalize_binance_spot(payload: dict, snapshot_at, batch_id: str) -> pd.DataFrame:
    """Binance 现货 exchangeInfo -> INSTRUMENT_COLUMNS 行 (全 symbol)。

    - 无 onboardDate/pricePrecision: listing_time 用 certified 现货 K 线
      首根 open_time 近似, 精度由 tickSize/stepSize 小数位推导。
    - tick_size=PRICE_FILTER.tickSize; lot_size=LOT_SIZE.stepSize;
      min_order_size=NOTIONAL.minNotional (或 MIN_NOTIONAL.notional)。
    """
    cov = _candle_coverage()
    rows = []
    for s in payload.get("symbols", []):
        sym = s.get("symbol", "")
        if not sym:
            continue
        inst = instrument_id(sym)
        tick = _binance_filter(s, "PRICE_FILTER").get("tickSize")
        step = _binance_filter(s, "LOT_SIZE").get("stepSize")
        nf = _binance_filter(s, "NOTIONAL") or _binance_filter(s, "MIN_NOTIONAL")
        min_notional = nf.get("minNotional") or nf.get("notional")
        listing = cov.get(("binance", "spot", inst), (None, None))[0]
        rows.append({
            **_base_cols(snapshot_at, batch_id, "binance"),
            "symbol": sym, "instrument_id": inst,
            "base_asset": s.get("baseAsset"), "quote_asset": s.get("quoteAsset"),
            "market_type": "spot", "contract_type": "spot",
            "contract_size": 1.0,          # 现货无合约乘数
            "tick_size": _fnum(tick), "lot_size": _fnum(step),
            "min_order_size": _fnum(min_notional),
            "price_precision": _decimals(tick),
            "quantity_precision": _decimals(step),
            "listing_time": listing,
            "status": s.get("status"),
            "settlement_asset": s.get("quoteAsset"),
            "underlying_asset": s.get("baseAsset"),
        })
    df = pd.DataFrame(rows)
    if df.empty:
        return df
    return _to_schema(_apply_coverage(df, cov))


def normalize_binance_perp(payload: dict, snapshot_at, batch_id: str) -> pd.DataFrame:
    """Binance USDT-M 永续 exchangeInfo -> INSTRUMENT_COLUMNS 行 (全 symbol)。

    - listing_time=onboardDate; 精度直接用 pricePrecision/quantityPrecision;
      settlement_asset=marginAsset; contract_size=1.0 (USDT-M 线性合约乘数=1)。
    """
    cov = _candle_coverage()
    rows = []
    for s in payload.get("symbols", []):
        sym = s.get("symbol", "")
        if not sym:
            continue
        inst = instrument_id(sym)
        tick = _binance_filter(s, "PRICE_FILTER").get("tickSize")
        step = _binance_filter(s, "LOT_SIZE").get("stepSize")
        nf = _binance_filter(s, "MIN_NOTIONAL") or _binance_filter(s, "NOTIONAL")
        min_notional = nf.get("notional") or nf.get("minNotional")
        onboard = s.get("onboardDate")
        rows.append({
            **_base_cols(snapshot_at, batch_id, "binance"),
            "symbol": sym, "instrument_id": inst,
            "base_asset": s.get("baseAsset"), "quote_asset": s.get("quoteAsset"),
            "market_type": "perpetual",
            "contract_type": s.get("contractType") or "PERPETUAL",
            "contract_size": 1.0,          # USDT-M 线性合约乘数=1
            "tick_size": _fnum(tick), "lot_size": _fnum(step),
            "min_order_size": _fnum(min_notional),
            "price_precision": s.get("pricePrecision"),
            "quantity_precision": s.get("quantityPrecision"),
            "listing_time": pd.to_datetime(pd.to_numeric(onboard, errors="coerce"),
                                           unit="ms", utc=True)
            if onboard else None,
            "status": s.get("status"),
            "settlement_asset": s.get("marginAsset") or s.get("quoteAsset"),
            "underlying_asset": s.get("baseAsset"),
        })
    df = pd.DataFrame(rows)
    if df.empty:
        return df
    return _to_schema(_apply_coverage(df, cov))


def normalize_okx(payload: dict, snapshot_at, batch_id: str) -> pd.DataFrame:
    """OKX instruments -> INSTRUMENT_COLUMNS 行 (SPOT + SWAP, 全 instId)。

    - instrument_id 用现有 l1 写法: instId (永续为 'BTC-USDT-SWAP')。
    - listing_time=listTime; tick_size/lot_size/min_order_size 取自
      tickSz/lotSz/minSz (SPOT 无 minSz 用 0); 精度由小数位推导。
    - contract_size: SWAP 的 ctVal; settlement_asset: settleCcy (OKX 结算币,
      如 BTC-USDT-SWAP 的 settleCcy=USDT; 任务稿写 ctValCcy, 但 OKX 的
      ctValCcy 是合约价值币=基础币, 如线性永续 BTC-USDT-SWAP 的
      ctValCcy=BTC, 用其作 settlement 会与 Binance marginAsset 跨所不一致,
      故以 settleCcy 为准, 与现有 l1_okx 写法一致)。
    - status 原文存字符串 (live/suspend/preopen/...)。
    """
    cov = _candle_coverage()
    rows = []
    for itype, data in (("SPOT", payload.get("spot", [])),
                        ("SWAP", payload.get("swap", []))):
        is_swap = itype == "SWAP"
        mtype = "perpetual" if is_swap else "spot"
        for s in data:
            inst = s.get("instId", "")
            if not inst:
                continue
            if is_swap:
                pair = s.get("uly") or (inst[:-5] if inst.endswith("-SWAP")
                                        else inst)
                base, quote = _split_pair(pair)
                ct_val = s.get("ctVal")
                contract_size = _fnum(ct_val) if ct_val else 1.0
                settle = s.get("settleCcy") or s.get("ctValCcy") or quote
            else:
                base = s.get("baseCcy") or _split_pair(inst)[0]
                quote = s.get("quoteCcy") or _split_pair(inst)[1]
                contract_size = 1.0
                settle = quote
            min_sz = s.get("minSz")
            min_order = 0.0 if (not is_swap and not min_sz) else _fnum(min_sz)
            rows.append({
                **_base_cols(snapshot_at, batch_id, "okx"),
                "symbol": inst, "instrument_id": inst,
                "base_asset": base, "quote_asset": quote,
                "market_type": mtype, "contract_type": mtype,
                "contract_size": contract_size,
                "tick_size": _fnum(s.get("tickSz")),
                "lot_size": _fnum(s.get("lotSz")),
                "min_order_size": min_order,
                "price_precision": _decimals(s.get("tickSz")),
                "quantity_precision": _decimals(s.get("lotSz")),
                "listing_time": pd.to_datetime(pd.to_numeric(s.get("listTime", ""),
                                                             errors="coerce"),
                                               unit="ms", utc=True, errors="coerce"),
                "status": s.get("state"),
                "settlement_asset": settle,
                "underlying_asset": base,
            })
    df = pd.DataFrame(rows)
    if df.empty:
        return df
    return _to_schema(_apply_coverage(df, cov))


def normalize_coinbase(payload: list, snapshot_at, batch_id: str) -> pd.DataFrame:
    """Coinbase products -> INSTRUMENT_COLUMNS 行 (全 products)。

    - tick_size=quote_increment; lot_size=base_increment;
      min_order_size=min_market_funds; 精度由 increment 小数位推导。
    - 无上市时间: listing_time 用 certified 现货 K 线首根 open_time 近似。
    - status 原文存字符串 (online/delisted/...)。
    """
    cov = _candle_coverage()
    rows = []
    for p in payload:
        pid = p.get("id", "")
        if not pid:
            continue
        qi = p.get("quote_increment")
        bi = p.get("base_increment")
        listing = cov.get(("coinbase", "spot", pid), (None, None))[0]
        rows.append({
            **_base_cols(snapshot_at, batch_id, "coinbase"),
            "symbol": pid, "instrument_id": pid,
            "base_asset": p.get("base_currency"), "quote_asset": p.get("quote_currency"),
            "market_type": "spot", "contract_type": "spot",
            "contract_size": 1.0,          # 现货无合约乘数
            "tick_size": _fnum(qi), "lot_size": _fnum(bi),
            "min_order_size": _fnum(p.get("min_market_funds")),
            "price_precision": _decimals(qi),
            "quantity_precision": _decimals(bi),
            "listing_time": listing,
            "status": p.get("status"),
            "settlement_asset": p.get("quote_currency"),
            "underlying_asset": p.get("base_currency"),
        })
    df = pd.DataFrame(rows)
    if df.empty:
        return df
    return _to_schema(_apply_coverage(df, cov))


# ============================================================
# schema 规整
# ============================================================
def _to_schema(df: pd.DataFrame) -> pd.DataFrame:
    cols = [c for c, _ in INSTRUMENT_COLUMNS]
    for c in cols:
        if c not in df.columns:
            df[c] = float("nan")
    df = df[cols].copy()
    for c in cols:
        if "time" in c or c == "data_available_at" or c.endswith("_utc"):
            df[c] = pd.to_datetime(df[c], utc=True, errors="coerce") \
                .astype("datetime64[us, UTC]")
    for c in ("contract_size", "tick_size", "lot_size", "min_order_size"):
        df[c] = pd.to_numeric(df[c], errors="coerce").astype("float64")
    for c in ("price_precision", "quantity_precision"):
        df[c] = pd.to_numeric(df[c], errors="coerce").astype("Int64")
    return df


# ============================================================
# L1/L2 写入: PIT 历史 + 认证 + manifest
# ============================================================
def _read_history(venue: str) -> pd.DataFrame:
    p = os.path.join(L1_DIR, "instrument", venue, "instruments.parquet")
    if os.path.exists(p):
        df = pd.read_parquet(p)
        if not df.empty:
            return df
    return pd.DataFrame()


def _migrate_legacy(venue: str) -> pd.DataFrame | None:
    """旧单快照 -> PIT 首个快照 (幂等)。

    读现有 l1/instrument/{venue}/instruments.parquet (run_pipeline 写的
    单次快照), 若已有 PIT 批次 (metadata_* 或 legacy_snapshot) 则跳过;
    否则 data_available_at = 对应 raw meta 的 ingested_at (找不到则今天
    UTC 0 点), source_batch_id = 'legacy_snapshot'。
    """
    p = os.path.join(L1_DIR, "instrument", venue, "instruments.parquet")
    if not os.path.exists(p):
        return None
    df = pd.read_parquet(p)
    if df.empty:
        return None
    bids = df["source_batch_id"].astype(str)
    if bids.str.startswith("metadata_").any() or bids.eq("legacy_snapshot").any():
        return None
    meta_map = {m["batch_id"]: m["ingested_at"]
                for m in list_raw_batches(venue, "exchange_metadata")}
    today00 = pd.Timestamp.now(tz="UTC").normalize()
    avail = []
    for b in bids:
        ing = meta_map.get(b)
        avail.append(pd.Timestamp(ing, tz="UTC") if ing else today00)
    df["data_available_at"] = pd.to_datetime(avail, utc=True)
    df["source_batch_id"] = "legacy_snapshot"
    df = _apply_coverage(df, _candle_coverage())
    df = _to_schema(df)  # 统一 dtype, 避免后续 concat 类型推断警告
    print(f"  [{venue}] 迁移旧单快照 -> PIT 首个快照 ({len(df)} 行, "
          f"data_available_at={df['data_available_at'].min()} "
          f"~ {df['data_available_at'].max()})")
    return df


def _ensure_unique_key(df: pd.DataFrame) -> None:
    """主键 (venue,symbol,data_available_at) 唯一性兜底: 同键跨 market_type
    的行 (如 binance 现货/永续同 symbol 同抓取时刻) 微调 +1us 区分。"""
    g = df.groupby(["venue_id", "symbol", "data_available_at"], dropna=False)
    for _, idx in g.groups.items():
        if len(idx) > 1:
            for k, i in enumerate(sorted(idx)):
                if k > 0 and pd.notna(df.at[i, "data_available_at"]):
                    df.at[i, "data_available_at"] = \
                        df.at[i, "data_available_at"] + pd.Timedelta(microseconds=k)


def _write_venue_history(venue: str, hist: pd.DataFrame):
    """写 PIT l1 parquet + certified parquet + manifest。"""
    hist = _to_schema(hist)
    root = os.path.join(L1_DIR, "instrument", venue)
    os.makedirs(root, exist_ok=True)
    pq.write_table(pa.Table.from_pandas(hist, preserve_index=False),
                   os.path.join(root, "instruments.parquet"), compression="snappy")
    cdf = certify_derivatives(hist, time_col="data_available_at",
                              core_numeric_cols=list(CORE_NUMERIC),
                              key_cols=list(KEY_COLS))
    write_certified_derivatives(cdf, "instrument", venue, "all",
                                "data_available_at")
    stats = {
        "row_count": int(len(hist)),
        "duplicate_count": int(hist.duplicated(subset=KEY_COLS).sum()),
        "gap_count": 0,
        "suspect_count": int(cdf["is_suspect"].sum()),
        "coverage_start": str(hist["data_available_at"].min()),
        "coverage_end": str(hist["data_available_at"].max()),
    }
    batches = sorted(hist["source_batch_id"].astype(str).unique().tolist())
    rules = {
        "note": "PIT 合约规格历史: 每 (venue,symbol) 每次抓取追加一行, "
                "data_available_at=抓取时刻; as_of 查询取最后版本; "
                "listing_time: Binance永续=onboardDate, OKX=listTime, "
                "Binance现货/Coinbase=certified K线首根近似; "
                "first/last_data_utc 为 certified K 线覆盖审计列",
        "key": KEY_COLS, "core_numeric": CORE_NUMERIC,
    }
    build_dataset_manifest("instrument", venue, "all", "*", "*", stats,
                           batches, rules)
    print(f"  [{venue}] PIT instrument: {len(hist)} 行, {len(batches)} 批次 "
          f"({hist['data_available_at'].min()} ~ "
          f"{hist['data_available_at'].max()})")
    return hist, stats


def certify_venue(venue: str, new_df: pd.DataFrame | None = None,
                  batch_ids=None):
    """把新快照并入 PIT 历史并重认证 (幂等: 同批次不重复追加)。

    new_df: 本次新快照 (INSTRUMENT_COLUMNS 行); batch_ids: 本次涉及的
    source_batch_id 集合, 用于幂等判断。None 时只重认证现有历史。
    """
    migrated = _migrate_legacy(venue)
    hist = migrated if migrated is not None else _read_history(venue)
    if new_df is not None and not new_df.empty:
        present = set(hist["source_batch_id"].astype(str)) if not hist.empty \
            else set()
        to_add = new_df[~new_df["source_batch_id"].isin(present)]
        if not to_add.empty:
            hist = pd.concat([hist, to_add], ignore_index=True) \
                if not hist.empty else to_add
            print(f"  [{venue}] 追加新快照: {len(to_add)} 行")
        else:
            print(f"  [{venue}] 本次批次已存在, 无新行追加 (幂等)")
    if hist is None or hist.empty:
        print(f"  [{venue}] 无数据, 跳过")
        return None
    _ensure_unique_key(hist)
    hist = hist.drop_duplicates(subset=KEY_COLS, keep="last")
    hist = hist.sort_values(["data_available_at", "symbol"]) \
        .reset_index(drop=True)
    return _write_venue_history(venue, hist)


# ============================================================
# L0 raw 写入 (standalone 用; run_daily 用自己的 _write_raw)
# ============================================================
def _batch_exists(venue: str, batch_id: str) -> bool:
    return any(m.get("batch_id") == batch_id
               for m in list_raw_batches(venue, "instrument_metadata"))


def _write_raw_payload(payload, venue: str, batch_id: str, api_desc: str) -> str:
    tmp = os.path.join(RAW_DIR, "_tmp", f"{batch_id}.json")
    os.makedirs(os.path.dirname(tmp), exist_ok=True)
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False)
    return write_raw_file(
        tmp, venue, "instrument_metadata", batch_id=batch_id,
        source={"api": api_desc,
                "fetched_at": datetime.now(timezone.utc).isoformat()},
        timestamp_unit="ms", ext="json")


# ============================================================
# 主流程
# ============================================================
def ingest_all(write_raw: bool = True, snapshot_at=None) -> dict:
    """抓 4 组元数据 -> (可选) 写 L0 raw 日批次 -> 归一化 -> 追加 PIT + 认证。"""
    snapshot_at = snapshot_at or pd.Timestamp.now(tz="UTC")
    date = snapshot_at.strftime("%Y%m%d")
    print(f"== metadata PIT 快照 ({snapshot_at:%Y-%m-%d %H:%M:%S} UTC) ==")
    fetch_fns = {"fetch_binance_spot": fetch_binance_spot,
                 "fetch_binance_perp": fetch_binance_perp,
                 "fetch_okx": fetch_okx,
                 "fetch_coinbase": fetch_coinbase}
    # ---- 抓取 (幂等: raw 批次已存在则跳过) ----
    payloads = {}
    for venue, key, fn_name, api in GROUPS:
        bid = f"metadata_{key}_{date}"
        if write_raw and _batch_exists(venue, bid):
            print(f"  [skip] {bid}: raw 批次已存在, 仅重认证")
            payloads[key] = None
            continue
        print(f"  抓取 {api} ...", flush=True)
        payloads[key] = fetch_fns[fn_name]()
        if write_raw:
            _write_raw_payload(payloads[key], venue, bid, api)
    # ---- 归一化 (每组独立 data_available_at = 抓取后时刻) ----
    norm_fns = {"binance_spot": normalize_binance_spot,
                "binance_perp": normalize_binance_perp,
                "okx": normalize_okx,
                "coinbase": normalize_coinbase}
    new_by_venue, bids_by_venue = {}, {}

    def _norm(venue, key):
        payload = payloads.get(key)
        if payload is None:
            return
        bid = f"metadata_{key}_{date}"
        ts = pd.Timestamp.now(tz="UTC")
        df = norm_fns[key](payload, ts, bid)
        new_by_venue.setdefault(venue, []).append(df)
        bids_by_venue.setdefault(venue, set()).add(bid)
        print(f"  {key}: {len(df)} 行 (snapshot_at={ts:%H:%M:%S})")

    for venue, key, _, _ in GROUPS:
        _norm(venue, key)
    # ---- 认证 (追加 + 重认证) ----
    results = {}
    for venue in ("binance", "okx", "coinbase"):
        new_df = pd.concat(new_by_venue.get(venue, []), ignore_index=True) \
            if new_by_venue.get(venue) else None
        out = certify_venue(venue, new_df=new_df,
                            batch_ids=bids_by_venue.get(venue))
        if out is not None:
            results[venue] = out[1]
    return results


def _summarize() -> None:
    print("\n==== 汇总 (l1/instrument PIT) ====")
    for venue in ("binance", "okx", "coinbase"):
        p = os.path.join(L1_DIR, "instrument", venue, "instruments.parquet")
        if not os.path.exists(p):
            print(f"== {venue}: 无数据 ==")
            continue
        df = pd.read_parquet(p)
        n_snap = df["source_batch_id"].nunique()
        print(f"== {venue}: {len(df)} 行, {n_snap} 个快照批次 ==")
        if "market_type" in df.columns:
            print("   market_type:", df["market_type"].value_counts().to_dict())
        if "listing_time" in df.columns:
            lt = pd.to_datetime(df["listing_time"], utc=True, errors="coerce")
            print(f"   listing_time 覆盖: {int(lt.notna().sum())}/{len(df)} "
                  f"({lt.min()} ~ {lt.max()})")
        if "first_data_utc" in df.columns:
            fd = pd.to_datetime(df["first_data_utc"], utc=True, errors="coerce")
            print(f"   first_data_utc 覆盖: {int(fd.notna().sum())}/{len(df)} "
                  f"({fd.min()} ~ {fd.max()})")
        if "status" in df.columns:
            vc = df["status"].value_counts().to_dict()
            print("   status 分布:", dict(list(vc.items())[:8]))
        if "data_available_at" in df.columns:
            da = pd.to_datetime(df["data_available_at"], utc=True)
            print(f"   data_available_at: {da.min()} ~ {da.max()}")


def main() -> None:
    ingest_all()
    _summarize()


if __name__ == "__main__":
    main()
