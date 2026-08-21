# -*- coding: utf-8 -*-
"""l1_onchain.py — 链上数据 L1 标准化/解码 (Ethereum + Arbitrum + Solana)。"""
from __future__ import annotations

import json
import os

import pandas as pd

from .config import L1_DIR, RAW_DIR
from .l0 import list_raw_batches
from .ingest_onchain import ARB_TOKENS, TOKENS, TRANSFER

# ERC-20 Transfer: Transfer(address from, address to, uint256 value)
# topics[1]=from, topics[2]=to, data=value (32字节大端)
# chain_id -> venue (raw 目录名) + token 合约表
CHAIN_ERC20 = {
    "ethereum": {"venue": "ethereum", "tokens": TOKENS},
    "arbitrum": {"venue": "arbitrum", "tokens": ARB_TOKENS},
}


def _raw_files(venue: str, dataset: str, batch_id: str) -> list[str]:
    """返回指定 batch_id (精确匹配) 的原始文件。

    精确匹配避免误读并行任务写入的同数据集其他批次 (如 *_daily_* )。
    """
    out = []
    for meta in list_raw_batches(venue, dataset):
        if meta["batch_id"] != batch_id:
            continue
        ingest = meta["ingested_at"][:10]
        d = os.path.join(RAW_DIR, venue, dataset, f"ingest_date={ingest}")
        for f in sorted(os.listdir(d)):
            if f.startswith(meta["batch_id"]) and not f.endswith(".meta.json"):
                out.append(os.path.join(d, f))
    return out


def _hex_addr(s: str) -> str:
    return "0x" + s[-40:].lower()


def _dec(value_hex: str, decimals: int) -> float:
    return int(value_hex, 16) / (10 ** decimals)


def decode_transfers() -> pd.DataFrame:
    """原始日志 -> token_transfer 解码宽表 (ethereum + arbitrum)。"""
    frames = []
    for chain_id, cfg in CHAIN_ERC20.items():
        for tok, meta in cfg["tokens"].items():
            for p in _raw_files(cfg["venue"], "erc20_transfer_logs",
                                f"{tok}_transfer_logs_v1"):
                with open(p, encoding="utf-8") as f:
                    logs = json.load(f)
                if not logs:
                    continue
                dec = meta["decimals"]
                rows = [{
                    "token": tok,
                    "block_number": int(l["blockNumber"], 16),
                    "tx_hash": l["transactionHash"],
                    "log_index": int(l["logIndex"], 16),
                    "from_address": _hex_addr(l["topics"][1]),
                    "to_address": _hex_addr(l["topics"][2]),
                    "value_raw": l["data"],
                    "value_decimal": _dec(l["data"][:66], dec),
                    "is_mint": l["topics"][1] == "0x" + "0" * 64,
                    "is_burn": l["topics"][2] == "0x" + "0" * 64,
                } for l in logs]
                d = pd.DataFrame(rows)
                d["chain_id"] = chain_id
                frames.append(d)
    if not frames:
        return pd.DataFrame()
    out = pd.concat(frames, ignore_index=True)
    # 跨批次重叠去重 (多次运行/窗口重叠)
    out = out.drop_duplicates(["chain_id", "token", "tx_hash", "log_index"],
                              keep="first")
    return out


def block_timestamps(chain_id: str = "ethereum") -> dict:
    """窗口边界块时间戳 (用于线性插值)。"""
    venue = CHAIN_ERC20[chain_id]["venue"]
    for p in _raw_files(venue, "erc20_transfer_logs", "window_blocks_v1"):
        with open(p, encoding="utf-8") as f:
            w = json.load(f)
        return {k: int(str(v), 0) for k, v in w.items()
                if k in ("start_block", "end_block", "start_timestamp",
                         "end_timestamp")}
    return {}


def add_timestamps(df: pd.DataFrame) -> pd.DataFrame:
    """按 (chain_id, 块号) 线性插值时间戳 (边界块精确, 中间 ±10 分钟, manifest 注明)。"""
    if df.empty:
        return df.copy()
    frames = []
    for chain_id, g in df.groupby("chain_id", sort=False):
        w = block_timestamps(chain_id)
        g = g.copy()
        if not w or "start_block" not in w:
            g["block_timestamp_utc"] = pd.Series(
                pd.NaT, index=g.index, dtype="datetime64[ns, UTC]")
        else:
            slope = (w["end_timestamp"] - w["start_timestamp"]) / max(
                w["end_block"] - w["start_block"], 1)
            g["block_timestamp_utc"] = pd.to_datetime(
                w["start_timestamp"] + (g["block_number"] - w["start_block"]) * slope,
                unit="s", utc=True)
        frames.append(g)
    return pd.concat(frames, ignore_index=True) if frames else df.copy()


def aggregate_daily(df: pd.DataFrame) -> pd.DataFrame:
    """token_transfer -> onchain_daily_aggregate。"""
    if df.empty:
        return pd.DataFrame()
    df = df.copy()
    df["date_utc"] = df["block_timestamp_utc"].dt.normalize()
    g = df.groupby(["chain_id", "token", "date_utc"])
    out = g.agg(
        transfer_count=("tx_hash", "count"),
        unique_from=("from_address", "nunique"),
        unique_to=("to_address", "nunique"),
        volume_token=("value_decimal", "sum"),
        mint_count=("is_mint", "sum"),
        burn_count=("is_burn", "sum"),
    ).reset_index()
    # 大额转账: >= 1e6 美元近似 (按 token 面值, USDT/USDC 1e6, DAI 1e6)
    out["large_transfer_count"] = 0
    large = df[df["value_decimal"] >= 1_000_000]
    if not large.empty:
        lc = large.groupby(["chain_id", "token", "date_utc"]).size().rename(
            "large_transfer_count").reset_index()
        out = out.drop(columns="large_transfer_count").merge(
            lc, on=["chain_id", "token", "date_utc"], how="left")
        out["large_transfer_count"] = out["large_transfer_count"].fillna(0)
    return out


def normalize_dex_volume() -> pd.DataFrame:
    frames = []
    for p in _raw_files("defillama", "dex_volume", "dex_v1"):
        with open(p, encoding="utf-8") as f:
            j = json.load(f)
        for dex, chart in j.items():
            if dex == "fetched_at" or not chart:
                continue
            df = pd.DataFrame(chart, columns=["ts", "volume_usd"])
            df["dex_name"] = dex
            frames.append(df)
    if not frames:
        return pd.DataFrame()
    out = pd.concat(frames, ignore_index=True)
    out["date_utc"] = pd.to_datetime(out["ts"], unit="s", utc=True).dt.normalize()
    out["volume_usd"] = pd.to_numeric(out["volume_usd"], errors="coerce")
    out["venue_id"] = "defillama"
    out = out.drop_duplicates(["dex_name", "date_utc"], keep="last")  # 跨批次重叠去重
    return out[["venue_id", "dex_name", "date_utc", "volume_usd"]]


def normalize_mempool_blocks() -> pd.DataFrame:
    for p in _raw_files("mempool", "btc_blocks", "blocks_v1"):
        with open(p, encoding="utf-8") as f:
            blocks = json.load(f)
        out = pd.DataFrame([{
            "block_height": b["height"],
            "block_timestamp_utc": pd.to_datetime(b["timestamp"], unit="s", utc=True),
            "tx_count": b.get("tx_count"),
            "size": b.get("size"),
            "fees_total": (b.get("fees") or {}).get("total"),
            "fees_base": (b.get("fees") or {}).get("base"),
            "fee_rate_avg": b.get("avg_fee_rate"),
            "fee_rate_median": b.get("median_fee_rate"),
        } for b in blocks])
        out["venue_id"] = "mempool"
        return out
    return pd.DataFrame()


def normalize_mempool_fees() -> pd.DataFrame:
    for p in _raw_files("mempool", "btc_fees", "fees_recommended_v1"):
        with open(p, encoding="utf-8") as f:
            j = json.load(f)
        row = {"venue_id": "mempool", "fetched_at": j.get("fetched_at")}
        for k in ("fastestFee", "halfHourFee", "hourFee", "economyFee", "minimumFee"):
            row[k] = j.get(k)
        return pd.DataFrame([row])
    return pd.DataFrame()


def normalize_oracle_snapshot() -> pd.DataFrame:
    for p in _raw_files("ethereum", "oracle_snapshot", "chainlink_v1"):
        with open(p, encoding="utf-8") as f:
            j = json.load(f)
        rows = []
        for pair in ("BTC-USD", "ETH-USD"):
            if pair not in j:
                continue
            r = j[pair]
            rows.append({
                "venue_id": "chainlink",
                "pair": pair,
                "price": r["answer"] / 1e8,
                "updated_at": pd.to_datetime(r["updatedAt"], unit="s", utc=True),
                "fetched_at": pd.to_datetime(j.get("fetched_at"), utc=True),
                "round_id": str(r["roundId"]),   # uint256 超 int64, 用字符串
            })
        return pd.DataFrame(rows)
    return pd.DataFrame()


def normalize_solana_snapshot() -> pd.DataFrame:
    """Solana 快照原始 JSON -> solana_snapshot 标准表。"""
    for p in _raw_files("solana", "solana_snapshot", "solana_snapshot_v1"):
        with open(p, encoding="utf-8") as f:
            j = json.load(f)
        row = {
            "venue_id": "solana",
            "slot": j.get("slot"),
            "block_height": j.get("block_height"),
            "usdc_supply": j.get("usdc_supply"),
            "tps": j.get("tps"),
            "fetched_at": pd.to_datetime(j.get("fetched_at"), utc=True),
        }
        return pd.DataFrame([row])
    return pd.DataFrame()


def write_onchain_parquet(df: pd.DataFrame, dataset: str, venue: str,
                          time_col: str) -> str:
    import pyarrow as pa
    import pyarrow.parquet as pq
    root = os.path.join(L1_DIR, dataset, venue)
    os.makedirs(root, exist_ok=True)
    df = df.copy()
    for c in df.columns:
        if "time" in c or c == "date_utc":
            df[c] = pd.to_datetime(df[c], utc=True, errors="coerce").astype("datetime64[us, UTC]")
    if time_col in df.columns:
        df["date"] = pd.to_datetime(df[time_col], utc=True).dt.strftime("%Y-%m-%d")
    pq.write_table(pa.Table.from_pandas(df, preserve_index=False),
                   os.path.join(root, "data.parquet"), compression="snappy")
    return root
