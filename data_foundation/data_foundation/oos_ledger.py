# -*- coding: utf-8 -*-
"""oos_ledger.py — OOS 池治理账本 (write-once + 角色分级 + DSR 试验计数)

设计文档: docs/research-pools-timewall-design.md 第 7 节 (v1.0 定稿)

核心机制
--------
OOS 是进交易前的最后闸门。治理方式不是"没人能看", 而是**角色分级**:

    Agent      提交模型版本 -> 拿到回执 (evaluation_id + 状态)
               永远拿不到分数 (API 层物理隔离, 无参数可绕过)

    研究人员   走人审通道读结果, 做 go/no-go 决策
               每次读取写账本 (谁/何时/看了哪个版本)

    账本       append-only, 每条记录不可修改
               eval_count 累计试验次数 N -> DSR 折现夏普的输入

为什么这样能防泄露
----------------
泄漏的本质是"看到分数 -> 自动回去改 -> 再测"的闭环。Agent 的迭代是自动的、
高频的, 一旦能读 OOS 分数必然把 OOS 当反馈池用且不留痕迹。**让 Agent 物理
读不到, 是保证 DSR 里的 N 真实的唯一办法。**

研究人员的查看是低频、有意识的决策行为, 且每次都留痕。"看了再改"在统计上
等价于多一次试验 (N+1), 无法隐身。
"""
from __future__ import annotations

import json
import os
import threading
import time
from datetime import datetime, timezone

from .config import DATA_ROOT
from .pool_registry import POOLS

# 账本目录 (与数据平级, 属于研究治理而非数据本体)
LEDGER_DIR = os.path.join(DATA_ROOT, "_oos_ledger")
SUBMISSIONS = os.path.join(LEDGER_DIR, "submissions.jsonl")   # Agent 提交
REVIEWS = os.path.join(LEDGER_DIR, "reviews.jsonl")           # 人员读取/决策
_LOCK = threading.Lock()

VALID_STATUS = ("pending", "evaluated", "rejected", "voided")

# ---------------------------------------------------------------------------
# 角色鉴权 (代码级, 不是靠约定)
# ---------------------------------------------------------------------------
# Agent 凭证前缀: 这些身份禁止调用任何返回分数的接口。
# 部署时人审通道还应走独立端口/凭证 —— 但即便调用方伪造 reader 名字, 本模块
# 也会因前缀匹配而拒绝。设计文档第 7.1 节"Agent 盲"在代码里真正生效。
AGENT_ID_PREFIXES = ("agent", "bot", "ai_", "llm", "gpt", "claude", "model_")


def assert_human(reader: str) -> None:
    """人审通道的代码级门禁: Agent 身份直接拒绝。

    比"约定 Agent 不调用"可靠 —— 越权发生在调用瞬间就被挡下, 且不留可利用的
    静默路径 (要么明确抛错, 要么悄悄返回, 不能是"看运气")。
    """
    r = (reader or "").strip().lower()
    if not r:
        raise PermissionError("人审通道需要身份标识 (reader)")
    for p in AGENT_ID_PREFIXES:
        if r.startswith(p):
            raise PermissionError(
                f"身份 {reader!r} 被识别为 Agent 身份, 无权读取 OOS 结果。"
                f"OOS 分数只对研究人员开放 (Agent 盲, 见设计文档 7.1)。"
            )


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _append(path: str, rec: dict) -> None:
    """append-only 写入 (每行一个 JSON)。写失败即抛 —— 账本不可静默丢记录。"""
    os.makedirs(os.path.dirname(path), exist_ok=True)
    line = json.dumps(rec, ensure_ascii=False, default=str)
    with open(path, "a", encoding="utf-8") as f:
        f.write(line + "\n")
        f.flush()
        os.fsync(f.fileno())   # 落盘, 防崩溃丢账


def _read_all(path: str) -> list[dict]:
    if not os.path.exists(path):
        return []
    out = []
    with open(path, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line:
                out.append(json.loads(line))
    return out


# ---------------------------------------------------------------------------
# Agent 侧: 提交 + 查状态 (永远拿不到分数)
# ---------------------------------------------------------------------------
def submit_oos_candidate(run_id: str, code_hash: str,
                         artifact_uri: str = "", notes: str = "") -> dict:
    """Agent 把模型版本送进 OOS 评估。

    返回回执: {evaluation_id, status, pool, submitted_at}
    **不含任何分数** —— 这是 API 层强制的"Agent 盲", 不是靠调用方自觉。
    """
    if not run_id:
        raise ValueError("run_id 不能为空")
    if not code_hash:
        raise ValueError("code_hash 不能为空 (锁定被测版本, 防止事后改代码重跑)")
    with _LOCK:
        ev_id = f"oos_{int(time.time() * 1000)}"
        rec = {
            "evaluation_id": ev_id,
            "run_id": run_id,
            "code_hash": code_hash,
            "artifact_uri": artifact_uri,
            "notes": notes,
            "pool": "oos",
            "pool_start": str(POOLS["oos"].start_ts.date()),
            "pool_end": str(POOLS["oos"].end_ts.date()),
            "status": "pending",
            "submitted_at": _now(),
        }
        _append(SUBMISSIONS, rec)
    return {"evaluation_id": ev_id, "status": "pending", "pool": "oos",
            "submitted_at": rec["submitted_at"]}


def oos_status(evaluation_id: str) -> dict:
    """Agent 查提交状态。只返回 pending/evaluated/voided, **不返回分数**。"""
    recs = _read_all(SUBMISSIONS)
    for r in reversed(recs):
        if r.get("evaluation_id") == evaluation_id:
            return {"evaluation_id": evaluation_id,
                    "status": r.get("status"),
                    "pool": "oos",
                    "submitted_at": r.get("submitted_at")}
    raise KeyError(f"未知 evaluation_id: {evaluation_id}")


def eval_count(run_id: str | None = None) -> dict:
    """累计试验次数 N (供 DSR 折减夏普)。

    - run_id 给了 -> 该 run 被送进 OOS 的次数 (一个版本反复送 = 多次试验)
    - run_id=None -> 全局总试验次数
    """
    recs = _read_all(SUBMISSIONS)
    if run_id is None:
        return {"total_trials": len(recs),
                "unique_runs": len({r.get("run_id") for r in recs})}
    hit = [r for r in recs if r.get("run_id") == run_id]
    return {"run_id": run_id, "total_trials": len(hit),
            "evaluation_ids": [r.get("evaluation_id") for r in hit]}


# ---------------------------------------------------------------------------
# 人审侧: 读取结果 (留痕) + go/no-go 决策
# ---------------------------------------------------------------------------
def record_evaluation(evaluation_id: str, metrics: dict,
                      reviewer: str = "") -> dict:
    """登记某次 OOS 评估的**实际结果** (由评估执行方在评估完成后写入)。

    注意: 写入方是评估执行方 (系统), 不是 Agent。写入后结果对 Agent 不可见
    (Agent 侧无读取接口)。研究人员在 oos_result_read 里读取时会被留痕。
    """
    with _LOCK:
        _append(REVIEWS, {"type": "evaluation_result",
                          "evaluation_id": evaluation_id,
                          "metrics": metrics,
                          "reviewer": reviewer,
                          "recorded_at": _now()})
    return {"evaluation_id": evaluation_id, "recorded": True}


def oos_result_read(evaluation_id: str, reader: str) -> dict:
    """研究人员读取某次 OOS 评价结果。**每次读取写账本** (reader/时间/版本)。

    角色门禁: reader 为 Agent 身份 (agent*/bot*/ai_*...) 时直接 PermissionError。
    这是人审通道 —— Agent 既没有调用入口, 伪造身份也会被本函数拒绝。
    """
    assert_human(reader)
    results = [r for r in _read_all(REVIEWS)
               if r.get("evaluation_id") == evaluation_id
               and r.get("type") == "evaluation_result"]
    if not results:
        raise KeyError(f"evaluation_id {evaluation_id} 尚无评估结果")
    result = results[-1]
    with _LOCK:
        _append(REVIEWS, {"type": "read_log",
                          "evaluation_id": evaluation_id,
                          "reader": reader,
                          "read_at": _now()})
    return {"evaluation_id": evaluation_id, "metrics": result.get("metrics"),
            "recorded_at": result.get("recorded_at")}


def oos_verdict(evaluation_id: str, accept: bool, reviewer: str,
                note: str = "") -> dict:
    """记录 go/no-go 决策。reject -> 下一个提交的版本计为新试验 (N+1)。"""
    assert_human(reviewer)
    verdict = "accept" if accept else "reject"
    with _LOCK:
        _append(REVIEWS, {"type": "verdict",
                          "evaluation_id": evaluation_id,
                          "verdict": verdict,
                          "reviewer": reviewer,
                          "note": note,
                          "decided_at": _now()})
    return {"evaluation_id": evaluation_id, "verdict": verdict}


# ---------------------------------------------------------------------------
# 审计视图 (研究人员用; Agent 也不该调用)
# ---------------------------------------------------------------------------
def audit_log(limit: int = 50) -> dict:
    """账本审计视图: 最近提交 + 读取 + 决策, 用于追溯"谁在什么时候看了什么"。"""
    subs = _read_all(SUBMISSIONS)
    reviews = _read_all(REVIEWS)
    return {
        "n_submissions": len(subs),
        "n_reads": sum(1 for r in reviews if r.get("type") == "read_log"),
        "n_verdicts": sum(1 for r in reviews if r.get("type") == "verdict"),
        "recent_submissions": subs[-limit:],
        "recent_reviews": reviews[-limit:],
    }


if __name__ == "__main__":
    import sys
    print("=" * 70)
    print("OOS 账本 — 演示 (Agent 侧 vs 人审侧 的信息隔离)")
    print("=" * 70)

    receipt = submit_oos_candidate(
        run_id="run_demo_v1", code_hash="sha256:deadbeef",
        artifact_uri="file:///models/v1.pkl", notes="demo")
    ev = receipt["evaluation_id"]
    print(f"\n[Agent] 提交回执: {json.dumps(receipt, ensure_ascii=False)}")

    st = oos_status(ev)
    print(f"[Agent] 查状态: {json.dumps(st, ensure_ascii=False)}")
    print("        ^ 注意: 没有 metrics/分数字段 —— Agent 拿不到结果")

    record_evaluation(ev, {"sharpe": 1.8, "max_dd": -0.22, "win_rate": 0.54},
                      reviewer="system")
    print("\n[系统] 评估完成, 结果已写入账本 (Agent 不可见)")

    try:
        r = oos_result_read(ev, reader="agent_bot")
        print(f"[越权] Agent 读取结果 -> 拿到: {r}")
        print("        ^ 危险: 角色门禁失效!")
    except PermissionError as e:
        print(f"[越权] Agent 读取被拒 -> PermissionError: {e}")
    except Exception as e:
        print(f"[越权] Agent 读取被拒: {type(e).__name__}: {e}")

    res = oos_result_read(ev, reader="研究员_张三")
    print(f"\n[研究员] 读取结果 (已留痕): sharpe={res['metrics'].get('sharpe')} "
          f"max_dd={res['metrics'].get('max_dd')}")

    v = oos_verdict(ev, accept=False, reviewer="研究员_张三", note="夏普不达标")
    print(f"[研究员] 决策: {v}")

    # 再提交一次同版本 (模拟"被拒后改一版再送")
    submit_oos_candidate(run_id="run_demo_v1", code_hash="sha256:cafebabe",
                         notes="被拒后修改再送")
    c = eval_count()
    print(f"\n[DSR] 试验次数 N (同 run 送 2 次): {c}")
    print("        ^ N 忠实反映真实试验次数 -> 夏普须按 N 折现")

    a = audit_log()
    print(f"\n[审计] 提交={a['n_submissions']} 读取={a['n_reads']} "
          f"决策={a['n_verdicts']}")
    print("\n演示完成 —— 账本目录:", LEDGER_DIR)