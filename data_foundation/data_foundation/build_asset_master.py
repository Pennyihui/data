# -*- coding: utf-8 -*-
"""
build_asset_master.py — 资产主映射 (asset_master) 全量重建
============================================================
统一主键: 同一资产跨所 symbol / 链上地址 / CMC id-slug 的映射表。

来源 A: certified/instrument/{binance,okx,coinbase}/all/data.parquet 最新快照
        (每 venue+symbol+market_type 取 max data_available_at)
来源 B: ingest_onchain.TOKENS / ARB_TOKENS (Ethereum+Arbitrum USDT/USDC/DAI
        合约地址) + SOLANA_USDC_MINT (chain_id=solana) -> market_type="onchain"
来源 C: ../data_new/additional/ 下 coingecko/cmc 排名 CSV, 按 symbol
        大小写不敏感匹配填 cmc_slug (可用 CSV 中仅 coingecko 的 id 列为
        slug, CMC 排名 CSV 无数字 id 列, 故 cmc_id 无源可填, 保持空)

合并去重 (asset, venue_id, market_type, instrument_id) -> 全量重建
(幂等覆盖), 写 L1 asset_master/"master" -> L2 certified + manifest。

用法:
  python -m data_foundation.build_asset_master
"""
from __future__ import annotations

import os
from datetime import datetime, timezone

import pandas as pd

from .config import CERTIFIED_DIR, PROJECT_ROOT
from .ingest_onchain import ARB_TOKENS, SOLANA_USDC_MINT, TOKENS
from .l1_onchain import write_onchain_parquet
from .l2 import (build_dataset_manifest, certify_derivatives,
                 write_certified_derivatives)

# 来源 A: certified instrument 快照的 venue 列表
INSTR_VENUES = ["binance", "okx", "coinbase"]

# asset_master 认证主键 (合并去重键)
DEDUP_KEYS = ["asset", "venue_id", "market_type", "instrument_id"]


# ---------------------------------------------------------------------------
# 来源 A: 交易所 certified instrument 最新快照
# ---------------------------------------------------------------------------
def load_instrument_snapshot() -> pd.DataFrame:
    """读三所 certified/instrument 快照, 每 (venue_id, symbol, market_type)
    取 data_available_at 最新一版。"""
    frames = []
    for venue in INSTR_VENUES:
        p = os.path.join(CERTIFIED_DIR, "instrument", venue, "all", "data.parquet")
        if not os.path.exists(p):
            print(f"  [skip] {venue} instrument 快照缺失: {p}")
            continue
        df = pd.read_parquet(p)
        for c in ("listing_time", "delisting_time", "data_available_at"):
            df[c] = pd.to_datetime(df[c], utc=True, errors="coerce")
        df = df.sort_values("data_available_at").drop_duplicates(
            subset=["venue_id", "symbol", "market_type"], keep="last")
        frames.append(df)
        print(f"  [A] {venue}: {len(df)} 行 (去重后)")
    if not frames:
        return pd.DataFrame()
    return pd.concat(frames, ignore_index=True)


def build_source_a() -> pd.DataFrame:
    """certified instrument -> asset_master 行 (asset=base_asset)。"""
    df = load_instrument_snapshot()
    if df.empty:
        return df
    df = df[df["base_asset"].notna() & (df["base_asset"].astype(str) != "")]
    out = pd.DataFrame({
        "asset": df["base_asset"].astype(str).str.upper(),
        "venue_id": df["venue_id"].astype(str),
        "market_type": df["market_type"].astype(str),
        "instrument_id": df["instrument_id"].astype(str),
        "symbol": df["symbol"].astype(str),
        "quote_asset": df.get("quote_asset"),
        "chain_id": None,
        "contract_address": None,
        "cmc_id": None,
        "cmc_slug": None,
        "listing_time": df["listing_time"],
        "delisting_time": df["delisting_time"],
        "status": df["status"].astype(str),
        "data_available_at": df["data_available_at"],
        "source_batch_id": df["source_batch_id"].fillna("").astype(str),
    })
    return out.reset_index(drop=True)


# ---------------------------------------------------------------------------
# 来源 B: 链上 token 静态映射 (market_type="onchain")
# ---------------------------------------------------------------------------
def build_source_b() -> pd.DataFrame:
    """TOKENS/ARB_TOKENS + SOLANA_USDC_MINT -> onchain 行。"""
    now = pd.Timestamp.now(tz="UTC")
    rows = []
    for chain, tokens in (("ethereum", TOKENS), ("arbitrum", ARB_TOKENS)):
        for tok, meta in tokens.items():
            rows.append({
                "asset": tok, "venue_id": chain, "market_type": "onchain",
                "instrument_id": tok, "symbol": tok,
                "quote_asset": None, "chain_id": chain,
                "contract_address": meta["address"], "cmc_id": None,
                "cmc_slug": None, "listing_time": None, "delisting_time": None,
                "status": "active", "data_available_at": now,
                "source_batch_id": "onchain_tokens_v1",
            })
    rows.append({
        "asset": "USDC", "venue_id": "solana", "market_type": "onchain",
        "instrument_id": "USDC", "symbol": "USDC", "quote_asset": None,
        "chain_id": "solana", "contract_address": SOLANA_USDC_MINT,
        "cmc_id": None, "cmc_slug": None, "listing_time": None,
        "delisting_time": None, "status": "active",
        "data_available_at": now, "source_batch_id": "onchain_tokens_v1",
    })
    return pd.DataFrame(rows)


# ---------------------------------------------------------------------------
# 来源 C: CMC id/slug (从 data_new/additional 排名 CSV 按 symbol 匹配)
# ---------------------------------------------------------------------------
def load_slug_map() -> dict:
    """coingecko 排名 CSV 的 id 列即币种 slug (bitcoin/ethereum/...)。

    可用 CSV 中:
      * coingecko_market_cap_ranking.csv / coingecko_daily_marketcap_ranking.csv
        有 id(slug) 列 -> cmc_slug 来源;
      * cmc_daily_*.csv 只有 date/symbol/rank 列, 无数字 CMC id -> cmc_id 无源。
    返回 {SYMBOL_UPPER: slug}。
    """
    additional = os.path.join(os.path.dirname(PROJECT_ROOT),
                              "data_new", "additional")
    sym_slug: dict = {}
    for fname in ("coingecko_market_cap_ranking.csv",
                  "coingecko_daily_marketcap_ranking.csv"):
        p = os.path.join(additional, fname)
        if not os.path.exists(p):
            print(f"  [skip] {fname} 不存在: {p}")
            continue
        df = pd.read_csv(p)
        if "id" not in df.columns or "symbol" not in df.columns:
            print(f"  [skip] {fname} 无 id/symbol 列: {df.columns.tolist()}")
            continue
        for _, r in df.iterrows():
            sym = str(r["symbol"]).strip().upper()
            slug = str(r["id"]).strip()
            if sym and slug and slug.lower() != "nan" and sym not in sym_slug:
                sym_slug[sym] = slug
        print(f"  [C] {fname}: 累计 {len(sym_slug)} 个 symbol->slug")
    return sym_slug


def apply_cmc(df: pd.DataFrame, slug_map: dict) -> pd.DataFrame:
    """按 asset (币种级 symbol, 大小写不敏感) 填 cmc_slug。

    注意: 行内 symbol 列是交易对 (BTCUSDT/BTC-USD), 不能与 CMC/coingecko
    的币种 symbol (BTC) 直接匹配, 故匹配键用 asset (base_asset)。
    只填能匹配上的, 其余保持空。
    """
    df = df.copy()
    df["cmc_slug"] = df["asset"].astype(str).str.upper().map(slug_map)
    return df


# ---------------------------------------------------------------------------
# 主流程
# ---------------------------------------------------------------------------
def build_asset_master(verbose: bool = True) -> pd.DataFrame:
    """全量重建 asset_master: 合并三来源 -> 去重 -> CMC -> L1/L2 + manifest。"""
    parts = []
    a = build_source_a()
    b = build_source_b()
    if not a.empty:
        parts.append(a)
    parts.append(b)
    df = pd.concat(parts, ignore_index=True)

    df["asset"] = df["asset"].astype(str)
    df["symbol"] = df["symbol"].astype(str)
    df["instrument_id"] = df["instrument_id"].astype(str)
    df["venue_id"] = df["venue_id"].astype(str)
    df["market_type"] = df["market_type"].astype(str)

    before = len(df)
    df = df.drop_duplicates(subset=DEDUP_KEYS, keep="first").reset_index(drop=True)
    if verbose and before != len(df):
        print(f"  [去重] {before} -> {len(df)} 行 (键: {DEDUP_KEYS})")

    df = apply_cmc(df, load_slug_map())
    df["data_available_at"] = pd.to_datetime(df["data_available_at"],
                                             utc=True, errors="coerce")
    if df["data_available_at"].isna().any():
        df["data_available_at"] = df["data_available_at"].fillna(
            pd.Timestamp.now(tz="UTC"))
    # L1 统一微秒 (write_onchain_parquet 不处理 data_available_at, 需显式 cast)
    df["data_available_at"] = df["data_available_at"].astype("datetime64[us, UTC]")

    # L1 写入
    write_onchain_parquet(df, "asset_master", "master", "data_available_at")
    print(f"  [L1] asset_master/master: {len(df)} 行")

    # L2 认证 + certified
    cdf = certify_derivatives(df, "data_available_at", core_numeric_cols=[],
                              key_cols=DEDUP_KEYS)
    write_certified_derivatives(cdf, "asset_master", "master", "all",
                                "data_available_at")
    stats = {
        "row_count": int(len(cdf)),
        "duplicate_count": 0,
        "gap_count": 0,
        "suspect_count": int(cdf["is_suspect"].sum()),
        "coverage_start": str(cdf["data_available_at"].min()),
        "coverage_end": str(cdf["data_available_at"].max()),
    }
    build_dataset_manifest(
        "asset_master", "*", "*", "*", "*", stats,
        ["instrument_certified_v1", "onchain_tokens_v1", "coingecko_ranking_v1"],
        {"note": "asset_master 全量重建: A=certified/instrument 最新快照 "
                 "(每 venue+symbol+market_type 取 max data_available_at); "
                 "B=Ethereum/Arbitrum USDT/USDC/DAI 合约 + Solana USDC mint "
                 "(market_type=onchain); C=coingecko 排名 CSV 按 symbol 匹配 "
                 "cmc_slug (CMC 排名 CSV 无数字 id 列, cmc_id 留空); "
                 "去重键 (asset, venue_id, market_type, instrument_id)"})
    print(f"  [L2] asset_master/master/all: {len(cdf)} 行, "
          f"coverage {stats['coverage_start']} ~ {stats['coverage_end']}")
    return df


if __name__ == "__main__":
    t0 = datetime.now(timezone.utc)
    out = build_asset_master()
    print(f"\nasset_master 构建完成: {len(out)} 行, "
          f"耗时 {(datetime.now(timezone.utc) - t0).total_seconds():.1f}s")
