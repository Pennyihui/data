# -*- coding: utf-8 -*-
"""
ingest_onchain.py — 链上数据 L0 摄取 (阶段 4, 免费源)
=====================================================
- ERC-20 Transfer 日志: eth_getLogs (近 24h)
  * Ethereum: USDT/USDC/DAI (rpc.mevblocker.io / eth.drpc.org)
  * Arbitrum: USDT/USDC/DAI (arb1.arbitrum.io / arbitrum.drpc.org)
  * 自适应块区间分页, 单查询 >1 万条自动减半 (min span 10 块)
- 区块时间戳: eth_getBlockByNumber 批量 (去重后 ≤4000 块精确, 否则边界插值)
  * Arbitrum 出块 ~0.25s/块 (24h ≈ 34 万块), 同样取窗口边界时间戳 + 线性插值
- Solana 快照: getSlot/getBlockHeight/getTokenSupply(USDC)/getRecentPerformanceSamples
  (快照级; 完整转账解码需索引器, 设计文档允许先聚合)
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
# Arbitrum RPC (故障切换链; arb1 官方为唯一可靠 getLogs 源, 单查询>1万条报
# "exceeds limit of 10000"; drpc 免费计划间歇性 "Temporary internal error";
# 1rpc.io 不支持 eth_getLogs)
ARB_RPCS = ["https://arb1.arbitrum.io/rpc",
            "https://arbitrum.drpc.org"]
# Solana RPC (故障切换链; mainnet-beta 官方 + publicnode 实测可用)
SOLANA_RPCS = ["https://api.mainnet-beta.solana.com",
               "https://solana-rpc.publicnode.com"]
MEMPOOL = "https://mempool.space/api"
LLAMA = "https://api.llama.fi"

TOKENS = {
    "USDT": {"address": "0xdAC17F958D2ee523a2206206994597C13D831ec7", "decimals": 6},
    "USDC": {"address": "0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48", "decimals": 6},
    "DAI": {"address": "0x6B175474E89094C44Da98b954EedeAC495271d0F", "decimals": 18},
}
ARB_TOKENS = {
    "USDT": {"address": "0xFd086bC7CD5C481DCC9C85ebE478A1C0b69FCbb9", "decimals": 6},
    "USDC": {"address": "0xaf88d065e77c8cC2239327C5EDb3A432268e5831", "decimals": 6},
    "DAI": {"address": "0xDA10009cBd5D07dd0CeCc66161FC93D7c9000da1", "decimals": 18},
}
# 链 -> (venue, tokens, rpcs, 每 24h 出块数估计, 起始分窗, timeout, retries)
CHAIN_ERC20 = {
    "ethereum": {"venue": "ethereum", "tokens": TOKENS, "rpcs": RPCS,
                 "blocks_per_day": 7200, "start_span": 200,
                 "timeout": 12, "retries": 2},
    # Arbitrum 0.25s/块, 单查询 1 万条上限; 起始跨度 1000 (实测 ~5.3 条/块,
    # 1000 块 ≈ 5.3k 条安全; 稀疏币种自动放宽跨度见 _ingest_erc20_logs)
    "arbitrum": {"venue": "arbitrum", "tokens": ARB_TOKENS, "rpcs": ARB_RPCS,
                 "blocks_per_day": 345600, "start_span": 1000,
                 "timeout": 25, "retries": 3},
}
# 日志抓取范围 (Ethereum USDT 24h 日志量 ~50 万条/250MB+; 全量抓取, raw 已 gitignore)
LOG_TOKENS = ["USDT", "USDC", "DAI"]
TRANSFER = "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"
CHAINLINK = {
    "BTC-USD": "0x1b44F3514812d835EB1BDB0acB33d3fA3351Ee43",
    "ETH-USD": "0x5f4eC3Df9cbd43714FE2740f5E3616155c5b8419",
}
SOLANA_USDC_MINT = "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v"


def _rpc(method, params, rpcs=None, retries=2, timeout=12):
    """带退避的 JSON-RPC 调用; rpcs 缺省用 Ethereum RPCS。

    错误聚合后 raise; 消息里含 "more than 10000" 等供上层减窗。
    """
    rpcs = rpcs or RPCS
    errors = []
    for i in range(retries):
        for rpc in rpcs:
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


def _ingest_erc20_logs(chain: str, days: int) -> list[str]:
    """近 days 天 ERC-20 Transfer 日志 (自适应块区间分页, 超限自动减半)。"""
    cfg = CHAIN_ERC20[chain]
    venue, tokens, rpcs = cfg["venue"], cfg["tokens"], cfg["rpcs"]
    timeout, retries = cfg["timeout"], cfg["retries"]
    written = []
    latest = int(_rpc("eth_blockNumber", [], rpcs=rpcs,
                      timeout=timeout, retries=retries), 16)
    start_block = latest - days * cfg["blocks_per_day"]
    for tok, meta in tokens.items():
        bid = f"{tok}_transfer_logs_v1"
        if _already(venue, "erc20_transfer_logs", bid):
            continue
        all_logs, frm = [], start_block
        span_hint = cfg["start_span"]   # 上一窗口可用跨度 (稀疏币种自动放宽)
        while frm <= latest:
            span = span_hint
            while span >= 10:
                to = min(frm + span - 1, latest)
                try:
                    logs = _rpc("eth_getLogs", [{"address": meta["address"],
                                                 "topics": [TRANSFER],
                                                 "fromBlock": hex(frm),
                                                 "toBlock": hex(to)}],
                                rpcs=rpcs, timeout=timeout, retries=retries)
                    break
                except RuntimeError as e:
                    msg = str(e)
                    # 各家 provider 超限措辞: mevblocker/drpc "more than 10000",
                    # arbitrum arb1 "exceeds limit of 10000" (code -32000)
                    if "too many logs" in msg or "exceeds max results" in msg \
                            or "more than 10000" in msg \
                            or "limit of 10000" in msg:
                        span //= 2
                        continue
                    raise
            if logs:
                all_logs.extend(logs)
            # 跨度自适应: 本窗口日志稀疏则放宽下一窗口, 密集则保持 (避免反复探测)
            if logs is not None and len(logs) < 5000:
                span_hint = min(span * 2, cfg["start_span"])
            else:
                span_hint = span
            frm = to + 1
            time.sleep(0.4)
        written.append(_save(
            f"{chain}_{tok}_logs.json", json.dumps(all_logs),
            venue, "erc20_transfer_logs", bid,
            {"api": "eth_getLogs", "token": tok, "chain": chain,
             "address": meta["address"], "blocks": [start_block, latest],
             "fetched_at": datetime.now(timezone.utc).isoformat()}))
        print(f"  {chain} {tok}: {len(all_logs)} 条日志, "
              f"{len(set(l['blockNumber'] for l in all_logs))} 块", flush=True)
    # 窗口边界块时间戳
    bid = "window_blocks_v1"
    if not _already(venue, "erc20_transfer_logs", "window_blocks_v1"):
        content = json.dumps({
            "chain": chain,
            "start_block": start_block,
            "end_block": latest,
            "start_timestamp": _rpc("eth_getBlockByNumber",
                                    [hex(start_block), False], rpcs=rpcs,
                                    timeout=timeout, retries=retries)["timestamp"],
            "end_timestamp": _rpc("eth_getBlockByNumber",
                                  [hex(latest), False], rpcs=rpcs,
                                  timeout=timeout, retries=retries)["timestamp"],
        })
        written.append(_save(f"{chain}_window.json", content, venue,
                             "erc20_transfer_logs", bid,
                             {"api": "eth_getBlockByNumber", "chain": chain,
                              "note": "窗口边界时间戳"}))
    return written


def ingest_erc20_logs(days: int = 1) -> list[str]:
    """Ethereum 近 days 天 ERC-20 Transfer 日志 (USDT/USDC/DAI, 全量)。"""
    return _ingest_erc20_logs("ethereum", days)


def ingest_arbitrum_logs(days: int = 1) -> list[str]:
    """Arbitrum 近 days 天 ERC-20 Transfer 日志 (USDT/USDC/DAI)。"""
    return _ingest_erc20_logs("arbitrum", days)


def ingest_solana_snapshot() -> list[str]:
    """Solana USDC 供应量/区块高度/槽位快照 (getTokenSupply 等)。"""
    bid = "solana_snapshot_v1"
    if _already("solana", "solana_snapshot", bid):
        return []
    slot = _rpc("getSlot", [], rpcs=SOLANA_RPCS, timeout=20)
    height = _rpc("getBlockHeight", [], rpcs=SOLANA_RPCS, timeout=20)
    sup = _rpc("getTokenSupply", [SOLANA_USDC_MINT], rpcs=SOLANA_RPCS, timeout=20)
    tps = None
    try:
        samples = _rpc("getRecentPerformanceSamples", [3], rpcs=SOLANA_RPCS,
                       timeout=20)
        if samples:
            s = samples[0]
            tps = round((s.get("numTransactions") or 0)
                        / max(s.get("samplePeriodSecs") or 60, 1), 2)
    except Exception:  # noqa: BLE001  # tps 样本可选, 失败不影响快照
        tps = None
    value = (sup or {}).get("value") or {}
    content = {
        "chain": "solana",
        "slot": slot,
        "block_height": height,
        "usdc_mint": SOLANA_USDC_MINT,
        "usdc_supply": value.get("uiAmount"),
        "usdc_supply_raw": value.get("amount"),
        "usdc_decimals": value.get("decimals"),
        "tps": tps,
        "fetched_at": datetime.now(timezone.utc).isoformat(),
    }
    written = [_save("solana_snapshot.json", json.dumps(content),
                     "solana", "solana_snapshot", bid,
                     {"api": "Solana JSON-RPC (getSlot/getBlockHeight/"
                      "getTokenSupply/getRecentPerformanceSamples)",
                      "mint": SOLANA_USDC_MINT,
                      "fetched_at": content["fetched_at"]})]
    print(f"  solana snapshot: slot={slot} height={height} "
          f"usdc_supply={content['usdc_supply']} tps={tps}", flush=True)
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
    # 各链独立容错: 单链反复失败仅告警, 不影响其余数据 (可重跑补齐)
    try:
        written += ingest_erc20_logs(days)
    except Exception as e:  # noqa: BLE001
        print(f"  [warn] Ethereum ERC-20 日志摄取失败(可重跑 --stage onchain): "
              f"{str(e)[:100]}")
    try:
        written += ingest_arbitrum_logs(days)
    except Exception as e:  # noqa: BLE001
        print(f"  [warn] Arbitrum ERC-20 日志摄取失败(可重跑 --stage onchain): "
              f"{str(e)[:100]}")
    try:
        written += ingest_solana_snapshot()
    except Exception as e:  # noqa: BLE001
        print(f"  [warn] Solana 快照摄取失败(可重跑 --stage onchain): "
              f"{str(e)[:100]}")
    written += ingest_mempool(hours)
    try:
        written += ingest_dex_volume()
    except Exception as e:  # noqa: BLE001
        print(f"  [warn] dex 摄取失败(可重跑): {str(e)[:60]}")
    written += ingest_chainlink()
    print(f"  共 {len(written)} 批次")
    return written
