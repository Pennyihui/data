# -*- coding: utf-8 -*-
"""
netpath.py — 统一网络链路助手: 四级兜底 + 通道观测
====================================================
背景 (2026-08 实测):
  * 本机 DNS 对 binance 等域被 GFW 污染 (解析到 Meta IP), 直连必失败;
  * 本机 mihomo (ProxyPoolService 管理的单核心):
      - 7897 mixed 口 + 域名规则分流: binance 域名 -> binance-failover 合并组
        (url-test 锁定单节点, 出口 IP 固定, 抗单源波动最强);
      - 7900-7907 专用口各绑一个按订阅源拆分的严格组 (auto-failover-*,
        同款健康检查 testnet.binancefuture.com/fapi/v1/time, tolerance=100,
        成员均为实测可访问币安的 binance_ok 节点), 绕过规则引擎, 故障域=单订阅源;
      - 7898 SOCKS5 口。
链路优先级 (已确认: 7897 域名优先):
  T0 vision    api.binance.com 的 URL 改写 data-api.binance.vision 直连 (官方公共数据镜像)
  T1 p7897     HTTP 代理 CONNECT 带域名 -> 命中 binance-failover 合并组
               (注意: 必须带域名请求, 不要钉 IP, 否则域名规则匹配不上)
  T2 专用端口   7900(au1rxx-1) -> 7906(v2rayfreeclash) -> 7907(topfreeclash)
               -> 7904(free18-1) -> 7902(diplole, 当前仅 3 健康节点故排最后)
  T3 socks5h   127.0.0.1:7898 远程解析 (需 PySocks, 域名仍可见、仍可命中规则);
               无 PySocks 时退化为 DoH 解析真实 IP + getaddrinfo 补丁 (Agent A 验证方案)
行为约定:
  * 失败驱动降级: 当前通道连续 2 次失败自动切下一级; 成功即缓存该通道;
  * 429 -> sleep 5 后同通道重试; 403(WAF) -> 指数退避并加速换通道;
  * 全局最小请求间隔限速 (WAF 友好); 所有尝试走 trust_env=False 的会话,
    不受环境变量代理干扰, 通道语义完全由本模块控制;
  * stats_snapshot() 输出观测汇总 (当前通道/成功失败计数/切换次数),
    供 run_daily 写进 daily_manifest —— 解决"到底走的哪条道"无法回溯的问题。
"""
from __future__ import annotations

import socket
import threading
import time
from contextlib import contextmanager
from urllib.parse import urlparse

import requests

UA = {"User-Agent": "Mozilla/5.0 (data-foundation)"}

_P7897 = "http://127.0.0.1:7897"
_SOCKS5H = "socks5h://127.0.0.1:7898"
_DED_PORTS = [7900, 7906, 7907, 7904, 7902]
_MIN_INTERVAL = 0.12          # 全局最小请求间隔 (秒)

try:                            # socks5h 需要 PySocks
    import socks  # noqa: F401
    _HAS_SOCKS = True
except Exception:               # noqa: BLE001
    _HAS_SOCKS = False

_SESSION = requests.Session()   # 不吃环境变量代理, 通道由本模块显式控制
_SESSION.trust_env = False

_LOGGER = None                  # 由调用方 set_logger(log) 注入


def set_logger(fn) -> None:
    global _LOGGER
    _LOGGER = fn


def _log(msg: str) -> None:
    if _LOGGER:
        try:
            _LOGGER(msg)
        except Exception:       # noqa: BLE001
            pass


# ---------------------------------------------------------------------------
# 传输通道定义
# ---------------------------------------------------------------------------
def _build_transports() -> list[dict]:
    ts: list[dict] = [
        {"name": "T0_vision", "kind": "direct", "only_host": "api.binance.com"},
        {"name": "T1_p7897", "kind": "proxy",
         "proxies": {"http": _P7897, "https": _P7897}},
    ]
    for p in _DED_PORTS:
        ts.append({"name": f"T2_p{p}", "kind": "proxy",
                   "proxies": {"http": f"http://127.0.0.1:{p}",
                               "https": f"http://127.0.0.1:{p}"}})
    if _HAS_SOCKS:
        ts.append({"name": "T3_socks5h", "kind": "proxy",
                   "proxies": {"http": _SOCKS5H, "https": _SOCKS5H}})
    ts.append({"name": "T3_doh_pin", "kind": "pin"})
    return ts


_TRANSPORTS = _build_transports()

_LOCK = threading.Lock()
_STATE = {
    "idx": 1,          # 默认从 T1 开始 (T0 仅 api.binance.com 且直连常不可用)
    "switches": 0,
    "last_channel": None,
}
_PER = {t["name"]: {"ok": 0, "fail": 0} for t in _TRANSPORTS}

_THROTTLE_LOCK = threading.Lock()
_LAST_CALL = [0.0]


def _throttle() -> None:
    with _THROTTLE_LOCK:
        wait = _LAST_CALL[0] + _MIN_INTERVAL - time.time()
        if wait > 0:
            time.sleep(wait)
        _LAST_CALL[0] = time.time()


# ---------------------------------------------------------------------------
# DoH 解析 + getaddrinfo 补丁 (最后兜底: 绕过本地 DNS 污染)
# ---------------------------------------------------------------------------
_DOH_ENDPOINTS = [
    "https://1.1.1.1/dns-query?name={h}&type=A",     # Cloudflare, IP 字面量无需 DNS
    "https://8.8.8.8/resolve?name={h}&type=A",       # Google
    "https://223.5.5.5/resolve?name={h}&type=A",     # AliDNS
]
_DOH_CACHE: dict[str, tuple[float, list[str]]] = {}


def doh_resolve(host: str, timeout: int = 8) -> list[str]:
    """经 DoH (IP 字面量端点, 加密查询不受 GFW 注入影响) 解析真实 A 记录。"""
    now = time.time()
    hit = _DOH_CACHE.get(host)
    if hit and now - hit[0] < 3600:
        return hit[1]
    errs = []
    for tpl in _DOH_ENDPOINTS:
        try:
            r = _SESSION.get(tpl.format(h=host), timeout=timeout,
                             headers={"accept": "application/dns-json"})
            r.raise_for_status()
            answers = [a["data"] for a in r.json().get("Answer", [])
                       if a.get("type") == 1]
            if answers:
                _DOH_CACHE[host] = (now, answers)
                return answers
            errs.append(f"{tpl.split('/')[2]}:empty")
        except Exception as e:  # noqa: BLE001
            errs.append(f"{tpl.split('/')[2]}:{str(e)[:40]}")
    raise RuntimeError(f"DoH 解析 {host} 失败: {'; '.join(errs)}")


@contextmanager
def _pinned(host: str, ips: list[str]):
    """临时把 host 的 DNS 解析钉到真实 IP (TLS/SNI 与校验仍用域名)。"""
    orig = socket.getaddrinfo

    def fake(h, port, *a, **k):  # noqa: ANN001, ANN002, ANN003
        if h == host:
            fam = socket.AF_INET
            out = []
            for ip in ips:
                try:
                    out.append((fam, socket.SOCK_STREAM, 6, "",
                                (ip, int(port) if port else 443)))
                except Exception:  # noqa: BLE001
                    pass
            if out:
                return out
        return orig(h, port, *a, **k)

    socket.getaddrinfo = fake
    try:
        yield
    finally:
        socket.getaddrinfo = orig


# ---------------------------------------------------------------------------
# 单次尝试与主循环
# ---------------------------------------------------------------------------
def _attempt(tr: dict, method: str, url: str, params, headers, timeout,
             json_body) -> requests.Response:
    _throttle()
    if tr["kind"] == "direct":
        # api.binance.com -> data-api.binance.vision (官方公共镜像, 可直连)
        eff = url.replace("://api.binance.com", "://data-api.binance.vision")
        return _SESSION.request(method, eff, params=params,
                                headers=headers or UA, timeout=timeout,
                                proxies={"http": None, "https": None},
                                json=json_body)
    if tr["kind"] == "pin":
        host = urlparse(url).hostname or ""
        ips = doh_resolve(host)
        with _pinned(host, ips):
            return _SESSION.request(method, url, params=params,
                                    headers=headers or UA, timeout=timeout,
                                    proxies={"http": None, "https": None},
                                    json=json_body)
    return _SESSION.request(method, url, params=params, headers=headers or UA,
                            timeout=timeout, proxies=tr["proxies"],
                            json=json_body)


def request(method: str, url: str, *, params=None, headers=None, timeout=25,
            json_body=None, retries: int = 8) -> requests.Response:
    """带四级兜底的请求。返回最终 Response (调用方自行 .json()/.text)。"""
    host = urlparse(url).hostname or ""
    order = [i for i, tr in enumerate(_TRANSPORTS)
             if tr.get("only_host") in (None, host)]
    if not order:
        order = [1]
    with _LOCK:
        start = _STATE["idx"]
    pos = order.index(start) if start in order else 0

    errors: list[str] = []
    bad = 0
    attempt = 0
    while attempt < retries:
        idx = order[pos % len(order)]
        tr = _TRANSPORTS[idx]
        try:
            r = _attempt(tr, method, url, params, headers, timeout, json_body)
            if r.status_code == 429:
                _log(f"netpath [{tr['name']}] 429, sleep 5")
                time.sleep(5)
                attempt += 1
                continue
            if r.status_code == 403:
                bad += 1
                time.sleep(min(2 ** bad, 20))
                if bad >= 2:
                    bad = 0
                    pos += 1
                    _note_switch(order, pos)
                attempt += 1
                continue
            r.raise_for_status()
            with _LOCK:
                _STATE["idx"] = idx
                _STATE["last_channel"] = tr["name"]
                _PER[tr["name"]]["ok"] += 1
            return r
        except Exception as e:  # noqa: BLE001
            errors.append(f"{tr['name']}:{str(e)[:70]}")
            with _LOCK:
                _PER[tr["name"]]["fail"] += 1
            bad += 1
            time.sleep(min(1.5 * (attempt + 1), 12))
            if bad >= 2:
                bad = 0
                pos += 1
                _note_switch(order, pos)
            attempt += 1
    raise RuntimeError(
        f"netpath 全通道失败 ({retries} 次尝试) {url}: " + " | ".join(errors[-4:]))


def _note_switch(order: list[int], pos: int) -> None:
    nxt = _TRANSPORTS[order[pos % len(order)]]["name"]
    with _LOCK:
        _STATE["switches"] += 1
    _log(f"netpath 切换通道 -> {nxt}")


# ---------------------------------------------------------------------------
# 公共 API
# ---------------------------------------------------------------------------
def fetch_json(url, params=None, *, headers=None, timeout=25, retries=8,
               json_body=None):
    r = request("GET", url, params=params, headers=headers, timeout=timeout,
                json_body=json_body, retries=retries)
    return r.json()


def post_json(url, json_body=None, *, headers=None, timeout=25, retries=4):
    r = request("POST", url, headers=headers, timeout=timeout,
                json_body=json_body, retries=retries)
    return r.json()


def fetch_text(url, params=None, *, headers=None, timeout=30, retries=8):
    r = request("GET", url, params=params, headers=headers, timeout=timeout,
                retries=retries)
    return r.text


def probe(timeout: int = 8,
          ref_urls: dict[str, str] | None = None) -> dict[str, dict]:
    """逐通道探测 (默认打 fapi /fapi/v1/time), 返回各通道结果并把缓存通道
    设为第一个可用项。ref_urls 可为不同域名分别指定参考 URL。"""
    ref_urls = ref_urls or {"*": "https://fapi.binance.com/fapi/v1/time"}
    out: dict[str, dict] = {}
    first_ok = None
    for i, tr in enumerate(_TRANSPORTS):
        ref = ref_urls.get(tr["name"], ref_urls.get("*"))
        if tr.get("only_host"):
            host_ok = any(urlparse(u).hostname == tr["only_host"]
                          for u in ref_urls.values())
            if not host_ok and tr["name"] == "T0_vision":
                out[tr["name"]] = {"ok": False, "skip": "非 api.binance.com 调用"}
                continue
        t0 = time.time()
        try:
            r = _attempt(tr, "GET", ref, None, UA, timeout, None)
            r.raise_for_status()
            ms = int((time.time() - t0) * 1000)
            out[tr["name"]] = {"ok": True, "ms": ms}
            if first_ok is None:
                first_ok = i
        except Exception as e:  # noqa: BLE001
            out[tr["name"]] = {"ok": False, "err": str(e)[:60]}
    with _LOCK:
        if first_ok is not None:
            _STATE["idx"] = first_ok
            _STATE["last_channel"] = _TRANSPORTS[first_ok]["name"]
    return out


def stats_snapshot() -> dict:
    with _LOCK:
        return {
            "current_channel": _STATE.get("last_channel"),
            "switches": _STATE["switches"],
            "per_transport": {k: dict(v) for k, v in _PER.items()},
            "has_pysocks": _HAS_SOCKS,
        }
