# -*- coding: utf-8 -*-
"""临时调试: Deribit/Arbitrum/Solana 可达性。用后删除。"""
import time

import requests

UA = {"User-Agent": "Mozilla/5.0 (data-foundation)"}


def get(url, params=None, retries=4, timeout=25):
    last = None
    for i in range(retries):
        try:
            r = requests.get(url, params=params, timeout=timeout, headers=UA)
            if r.status_code == 200:
                return r.json()
            last = f"HTTP{r.status_code}: {r.text[:80]}"
        except Exception as e:  # noqa: BLE001
            last = str(e)[:80]
        time.sleep(1.5)
    return {"ERR": last}


print("== Deribit ==")
j = get("https://www.deribit.com/api/v2/public/get_index_price",
        {"index_name": "btc_usd"})
print("index:", j.get("result") if isinstance(j, dict) else j)
j2 = get("https://www.deribit.com/api/v2/public/get_volatility_index_data",
         {"currency": "BTC", "start_timestamp": int(time.time() * 1000) - 3 * 86400000,
          "end_timestamp": int(time.time() * 1000)})
print("dvol 3d:", len(j2.get("result", {}).get("data", [])) if isinstance(j2, dict) else j2)
j3 = get("https://www.deribit.com/api/v2/public/get_book_summary_by_currency",
         {"currency": "BTC", "kind": "option"})
if isinstance(j3, dict) and j3.get("result"):
    print("book_summary BTC options:", len(j3["result"]), "条; 首条字段:",
          list(j3["result"][0].keys())[:12])
else:
    print("book FAIL:", str(j3)[:100])

print("\n== Arbitrum RPC ==")
p = {"jsonrpc": "2.0", "id": 1, "method": "eth_blockNumber", "params": []}
try:
    r = requests.post("https://arb1.arbitrum.io/rpc", json=p, timeout=20, headers=UA)
    print("arb blockNumber:", r.json().get("result"))
except Exception as e:  # noqa: BLE001
    print("arb EXC:", str(e)[:60])

print("\n== Solana RPC ==")
p2 = {"jsonrpc": "2.0", "id": 1, "method": "getSlot", "params": []}
try:
    r = requests.post("https://api.mainnet-beta.solana.com", json=p2, timeout=20, headers=UA)
    print("sol slot:", r.json().get("result"))
except Exception as e:  # noqa: BLE001
    print("sol EXC:", str(e)[:60])
# USDC 供应量
p3 = {"jsonrpc": "2.0", "id": 1, "method": "getTokenSupply",
      "params": ["EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v"]}
try:
    r = requests.post("https://api.mainnet-beta.solana.com", json=p3, timeout=20, headers=UA)
    j = r.json()
    v = j.get("result", {}).get("value", {})
    print("sol USDC supply:", v.get("amount"), "decimals:", v.get("decimals"))
except Exception as e:  # noqa: BLE001
    print("sol supply EXC:", str(e)[:60])
