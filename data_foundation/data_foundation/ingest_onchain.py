# -*- coding: utf-8 -*-
"""
ingest_onchain.py — 链上数据 L0 摄取 (阶段 4, 免费源)
=====================================================
- ERC-20 Transfer 日志: eth.drpc.org eth_getLogs (USDT/USDC/DAI, 近 24h)
- 区块时间戳: eth_getBlockByNumber 批量 (去重后 ≤4000 块精确, 否则边界插值)
- BTC mempool/fees: mempool.space (推荐费率快照 + 近 24h 区块)
- DEX 量: DefiLlama (Uniswap 等日频)
- Chainlink 快照: latestRoundData (BTC/USD, ETH/USD)
"""
from __future__ import annotations

import json
import os
import time
from datetime import datetime, timezone

import requests

from .config import RAW_DIR
from .l0 import list_raw_batches, write_raw_file

UA = {"User-Agent": "Mozilla/5.0 (data-foundation)"}
# 公共 RPC (getLogs 需支持任意历史区间: mevblocker/drpc 实测可用)
RPCS = ["https://rpc.mevblocker.io", "https://eth.drpc.org"]
MEMPOOL = "https://mempool.space/api"
LLAMA = "https://api.llama.fi"

TOKENS = {
    "USDT": {"address": "0xdAC17F958D2ee523a2206206994597C13D831ec7", "decimals": 6},
    "USDC": {"address": "0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48", "decimals": 6},
    "DAI": {"address": "0x6B175474E89094C44Da98b954EedeAC495271d0F", "decimals": 18},
}
# 日志抓取范围 (USDT 24h 日志量 ~50万条/250MB, MVP 先取 USDC+DAI)
LOG_TOKENS = ["USDC", "DAI"]
TRANSFER = "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"
CHAINLINK = {
    "BTC-USD": "0x1b44F3514812d835EB1BDB0acB33d3fA3351Ee43",
    "ETH-USD": "0x5f4eC3Df9cbd43714FE2740f5E3616155c5b8419",
}


def _rpc(method, params, retries=2, timeout=12):
    errors = []
    for i in range(retries):
        for rpc in RPCS:
            try:
                r = requests.post(rpc, json={"jsonrpc": "2.0", "id": 1,
                                             "method": method, "params": params},
                                  timeout=timeout, headers=UA)
                r.raise_for_status()
                j = r.json()
                if "result" in j:
                    return j["result"]
                errors.append(f"{rpc}: {str(j)[:80]}")
            except Exception as e:  # noqa: BLE001
                errors.append(f"{rpc}: {str(e)[:80]}")
        time.sleep(2 * (i + 1))
    raise RuntimeError("; ".join(errors[-4:]))


def _already(venue: str, dataset: str, batch_id: str) -> bool:
    return any(m.get("batch_id") == batch_id
               for m in list_raw_batches(venue, dataset))


def _save(tmp_name: str, content: str, venue: str, dataset: str, batch_id: str,
          source: dict) -> str:
    tmp = os.path.join(RAW_DIR, "_tmp", tmp_name)
    os.makedirs(os.path.dirname(tmp), exist_ok=True)
    with open(tmp, "w", encoding="utf-8") as f:
        f.write(content)
    return write_raw_file(tmp, venue, dataset, batch_id, source,
                          timestamp_unit="ms", ext="json")


def ingest_erc20_logs(days: int = 1) -> list[str]:
    """近 days 天 ERC-20 Transfer 日志 (自适应块区间分页, 超限自动减半)。"""
    written = []
    latest = int(_rpc("eth_blockNumber", []), 16)
    start_block = latest - days * 7200
    for tok in LOG_TOKENS:
        meta = TOKENS[tok]
        bid = f"{tok}_transfer_logs_v1"
        if _already("ethereum", "erc20_transfer_logs", bid):
            continue
        all_logs, frm = [], start_block
        while frm <= latest:
            span = 200
            while span >= 10:
                to = min(frm + span - 1, latest)
                try:
                    logs = _rpc("eth_getLogs", [{"address": meta["address"],
                                                 "topics": [TRANSFER],
                                                 "fromBlock": hex(frm),
                                                 "toBlock": hex(to)}])
                    break
                except RuntimeError as e:
                    msg = str(e)
                    if "too many logs" in msg or "exceeds max results" in msg \
                            or "more than 10000" in msg:
                        span //= 2
                        continue
                    raise
            if logs:
                all_logs.extend(logs)
            frm = to + 1
            time.sleep(0.4)
        written.append(_save(
            f"eth_{tok}_logs.json", json.dumps(all_logs),
            "ethereum", "erc20_transfer_logs", bid,
            {"api": "eth_getLogs", "token": tok,
             "address": meta["address"], "blocks": [start_block, latest],
             "fetched_at": datetime.now(timezone.utc).isoformat()}))
        print(f"  {tok}: {len(all_logs)} 条日志, {len(set(l['blockNumber'] for l in all_logs))} 块",
              flush=True)
    # 窗口边界块时间戳
    bid = "window_blocks_v1"
    if not _already("ethereum", "erc20_transfer_logs", "window_blocks_v1"):
        content = json.dumps({
            "start_block": start_block,
            "end_block": latest,
            "start_timestamp": _rpc("eth_getBlockByNumber",
                                    [hex(start_block), False])["timestamp"],
            "end_timestamp": _rpc("eth_getBlockByNumber",
                                  [hex(latest), False])["timestamp"],
        })
        written.append(_save("eth_window.json", content, "ethereum",
                             "erc20_transfer_logs", bid,
                             {"api": "eth_getBlockByNumber", "note": "窗口边界时间戳"}))
    return written


def ingest_mempool(hours: int = 24) -> list[str]:
    written = []
    # 推荐费率快照
    if not _already("mempool", "btc_fees", "fees_recommended_v1"):
        r = requests.get(f"{MEMPOOL}/v1/fees/recommended", timeout=25, headers=UA)
        r.raise_for_status()
        j = r.json()
        j["fetched_at"] = datetime.now(timezone.utc).isoformat()
        written.append(_save("mempool_fees.json", json.dumps(j),
                             "mempool", "btc_fees", "fees_recommended_v1",
                             {"api": "mempool.space /v1/fees/recommended"}))
    # 区块 (24h, 15块/页)
    if not _already("mempool", "btc_blocks", "blocks_v1"):
        blocks, height = [], None
        end_ts = time.time()
        for _ in range(30):
            path = f"{MEMPOOL}/v1/blocks" if height is None \
                else f"{MEMPOOL}/v1/blocks/{height}"
            r = requests.get(path, timeout=25, headers=UA)
            r.raise_for_status()
            page = r.json()
            if not page:
                break
            blocks.extend(page)
            height = page[-1]["height"] - 1
            if page[0]["timestamp"] < end_ts - hours * 3600:
                break
            time.sleep(0.3)
        written.append(_save("mempool_blocks.json", json.dumps(blocks),
                             "mempool", "btc_blocks", "blocks_v1",
                             {"api": "mempool.space /api/v1/blocks",
                              "hours": hours}))
        print(f"  mempool blocks: {len(blocks)} 块", flush=True)
    return written


def ingest_dex_volume() -> list[str]:
    if _already("defillama", "dex_volume", "dex_v1"):
        return []
    last = None
    DEX_WHITELIST = ["Uniswap", "PancakeSwap", "Raydium", "Curve",
                     "Orca", "Trader Joe", "Aerodrome"]
    for i in range(8):
        try:
            r = requests.get(
                f"{LLAMA}/overview/dexs?dataType=dailyVolume",
                timeout=90, headers=UA)
            r.raise_for_status()
            j = r.json()
            out = {"all_dexs": j.get("totalDataChart", [])}
            for day in j.get("totalDataChartBreakdown", []):
                if not isinstance(day, dict):
                    continue
                ts = day.get("date")
                dexs = day.get("dexs", {})
                for dex in DEX_WHITELIST:
                    if dex in dexs and isinstance(dexs[dex], dict):
                        out.setdefault(dex, []).append(
                            [ts, dexs[dex].get("volume", 0)])
            written = [_save("dex_volume.json", json.dumps(out), "defillama",
                             "dex_volume", "dex_v1",
                             {"api": "defillama /overview/dexs",
                              "whitelist": DEX_WHITELIST,
                              "fetched_at": datetime.now(timezone.utc).isoformat()})]
            n = sum(len(v) for v in out.values())
            print(f"  dex: {len(out)} 个系列, {n} 行", flush=True)
            return written
        except Exception as e:  # noqa: BLE001
            last = e
            time.sleep(10)
    raise RuntimeError(str(last)[:100])


def ingest_chainlink() -> list[str]:
    if _already("ethereum", "oracle_snapshot", "chainlink_v1"):
        return []
    out = {}
    for pair, addr in CHAINLINK.items():
        res = _rpc("eth_call", [{"to": addr, "data": "0xfeaf968c"}, "latest"])
        # latestRoundData 返回 5 个 32 字节词: roundId, answer, startedAt, updatedAt, answeredInRound
        hex_body = res[2:] if res.startswith("0x") else res
        words = [int(hex_body[i:i + 64], 16) for i in range(0, len(hex_body), 64)]
        if len(words) >= 5:
            out[pair] = {"roundId": words[0], "answer": words[1],
                         "startedAt": words[2], "updatedAt": words[3],
                         "answeredInRound": words[4]}
        time.sleep(0.4)
    out["fetched_at"] = datetime.now(timezone.utc).isoformat()
    return [_save("chainlink.json", json.dumps(out), "ethereum",
                  "oracle_snapshot", "chainlink_v1",
                  {"api": "eth_call latestRoundData", "pairs": list(CHAINLINK)})]


def ingest_onchain_all(days: int = 1, hours: int = 24) -> list[str]:
    written = []
    print("  链上 L0 摄取...")
    written += ingest_erc20_logs(days)
    written += ingest_mempool(hours)
    try:
        written += ingest_dex_volume()
    except Exception as e:  # noqa: BLE001
        print(f"  [warn] dex 摄取失败(可重跑): {str(e)[:60]}")
    written += ingest_chainlink()
    print(f"  共 {len(written)} 批次")
    return written
