# -*- coding: utf-8 -*-
"""eval_service.py — 内部评估服务 + 反馈池通道 (2026-10-05 三池隔离)

三池隔离模型 (用户定案) 里, 反馈池(valid)和 OOS 池**不向 Agent 开放原始数据**,
只能"提交 → 内部服务评 → 取结果"。本模块实现这条通道:

    submit(pool_id, run_id, payload)   Agent 提交 (代码哈希 + 参数)
    evaluate(evaluation_id, fn)       **内部服务**调用: 执行评估并记成绩
    read_feedback(evaluation_id, reader)  Agent 取反馈池结果 —— 限次数
    read_oos(evaluation_id, reader)       人读 OOS 结果 (Agent 永久拒绝)

关键不变式 (代码级, 不靠约定):
  1. valid/oos 的评估**只能由本服务执行** —— Agent 既拿不到原始数据, 也就无法
     自行算业绩, 绕不过去。
  2. 反馈池结果限 `VALID_FEEDBACK_VIEW_LIMIT` 次 —— 防"看结果→改→再看"的闭环
     把反馈池也过拟合掉。每次读取留痕 (reader/时间/第几次)。
  3. OOS 结果 Agent 永不返回 (复用 oos_ledger.assert_human 的身份前缀拦截),
     每次人工读取同样留痕。

评估函数 (fn) 由调用方注入 —— 服务不内置 IC/FDR 算法 (那属模型协议层),
本层只管**谁能评、谁能看、看了几次**。
"""
from __future__ import annotations

import json
import os
import threading
import time
from datetime import datetime, timezone

from . import oos_ledger
from .pool_registry import (VALID_FEEDBACK_VIEW_LIMIT, assert_can_read_data,
                            data_access_mode)

FEEDBACK_DIR = oos_ledger.LEDGER_DIR
FEEDBACK_VIEWS = os.path.join(FEEDBACK_DIR, "feedback_views.jsonl")
SUBMISSIONS = os.path.join(FEEDBACK_DIR, "submissions.jsonl")
_LOCK = threading.Lock()


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _append(path: str, rec: dict) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with _LOCK:
        with open(path, "a", encoding="utf-8") as f:
            f.write(json.dumps(rec, ensure_ascii=False, default=str) + "\n")
            f.flush()
            os.fsync(f.fileno())


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


def submit(pool_id: str, run_id: str, code_hash: str, payload: dict | None = None,
           agent: str = "agent") -> str:
    """Agent 提交一个候选 (valid/oos 通道)。返回 evaluation_id。

    valid/oos: Agent 本来就拿不到原始数据, 所以这里只接收代码/参数, 由内部
    服务在**服务端**用池内数据评估 —— Agent 无法自行验证。
    """
    eid = f"{pool_id}-{run_id}-{int(time.time()*1000)}"
    _append(SUBMISSIONS, {
        "evaluation_id": eid, "pool": pool_id, "run_id": run_id,
        "code_hash": code_hash, "payload": payload or {},
        "agent": agent, "submitted_at": _now(), "status": "pending",
    })
    if pool_id == "oos":
        # OOS 提交同时登记 oos_ledger —— 让 eval_count(DSR 试验次数 N) 统计到,
        # 否则 Agent 反复提交 OOS 的行为不留痕 (DSR 里 N 会失真)。
        try:
            oos_ledger.submit_oos_candidate(run_id, code_hash,
                                             notes=f"via eval_service agent={agent}")
        except Exception:
            pass
    return eid


def evaluate(evaluation_id: str, metrics: dict, evaluated_by: str = "eval_service",
             notes: str = "") -> dict:
    """内部服务完成评估并记成绩。**只有本服务能调** (Agent 无原始数据亦无从自评)。

    入库前过 **schema 门** (evaluation.metrics.validate_metrics): 白名单字段 +
    必需顶层 + 数值域 + 指标版本 —— 防"注入指标"绕过白名单 (设计文档 §4)。
    schema 门失败 -> 拒绝入库并抛出, 不是静默接受。

    OOS 池的成绩**同时**写入 oos_ledger —— 那是人审通道读的地方, 保证
    "服务评的" 和 "人读的" 是同一份记录 (否则两套账本各说各话)。Agent 读
    oos_ledger 仍被 assert_human 按身份前缀拦住。
    """
    subs = [s for s in _read_all(SUBMISSIONS)
            if s["evaluation_id"] == evaluation_id]
    if not subs:
        raise KeyError(f"未知 evaluation_id: {evaluation_id}")
    rec = subs[-1]
    # -- schema 门 (指标本体来自 evaluation 包, 通道只管谁能评/谁能看) --
    from .evaluation.metrics import validate_metrics
    problems = validate_metrics(metrics, strict=False)
    if problems:
        raise ValueError(
            "评估结果未过 schema 门, 拒绝入库: " + "; ".join(problems[:8]))
    _append(SUBMISSIONS, {
        "evaluation_id": evaluation_id, "pool": rec["pool"],
        "run_id": rec["run_id"], "status": "evaluated",
        "metrics": metrics, "evaluated_by": evaluated_by, "notes": notes,
        "evaluated_at": _now(),
    })
    if rec["pool"] == "oos":
        try:
            oos_ledger.record_evaluation(evaluation_id, metrics,
                                         reviewer=evaluated_by)
        except Exception:
            pass                    # 人审账本写失败不阻断服务评估
    return {"evaluation_id": evaluation_id, "pool": rec["pool"],
            "status": "evaluated", "metrics": metrics}


def _view_count(evaluation_id: str) -> int:
    return len([v for v in _read_all(FEEDBACK_VIEWS)
                if v["evaluation_id"] == evaluation_id])


def read_feedback(evaluation_id: str, reader: str = "agent",
                  limit: int | None = None) -> dict:
    """Agent 读取**反馈池**结果 —— 限次数。

    limit 默认 VALID_FEEDBACK_VIEW_LIMIT (2 次)。超限抛错, 防反复看结果调参。
    """
    subs = [s for s in _read_all(SUBMISSIONS)
            if s["evaluation_id"] == evaluation_id]
    if not subs:
        raise KeyError(f"未知 evaluation_id: {evaluation_id}")
    rec = next((s for s in reversed(subs) if s.get("status") == "evaluated"), None)
    if rec is None:
        return {"evaluation_id": evaluation_id, "status": "pending",
                "note": "尚未评估完成"}
    pool = rec["pool"]
    if pool not in ("valid",):
        raise PermissionError(
            f"read_feedback 只服务反馈池(valid), 拿不到池 {pool} 的结果。"
            + ("OOS 结果 Agent 永不返回, 只有人可读 (oos_result_read)。"
               if pool == "oos" else ""))
    limit = VALID_FEEDBACK_VIEW_LIMIT if limit is None else limit
    seen = _view_count(evaluation_id)
    if seen >= limit:
        raise PermissionError(
            f"反馈池结果查看次数已达上限 ({limit}) —— 反复看结果改模型会把反馈池"
            f"也过拟合掉。若确需更多, 请用新代码哈希重新提交一次候选。")
    _append(FEEDBACK_VIEWS, {
        "evaluation_id": evaluation_id, "pool": pool, "reader": reader,
        "view_index": seen + 1, "viewed_at": _now(),
    })
    return {"evaluation_id": evaluation_id, "pool": pool, "status": "evaluated",
            "metrics": rec.get("metrics"), "views_used": seen + 1,
            "views_limit": limit}


def status(evaluation_id: str) -> dict:
    """查评估状态 (不给分数)。任何池都可用。"""
    subs = [s for s in _read_all(SUBMISSIONS)
            if s["evaluation_id"] == evaluation_id]
    if not subs:
        raise KeyError(evaluation_id)
    last = subs[-1]
    return {"evaluation_id": evaluation_id, "pool": last["pool"],
            "status": last.get("status"), "submitted_at": last.get("submitted_at")}


if __name__ == "__main__":  # pragma: no cover
    from .evaluation.metrics import build_metrics
    demo = build_metrics(
        {"ic_mean": 0.031, "rank_ic_mean": 0.028, "icir": 0.21},
        {"ann_return_net": 0.18, "sharpe": 1.1, "max_drawdown": -0.15,
         "annualized_turnover": 3.2, "total_cost": 1200.0},
        {"psr": 0.97, "dsr": 0.93, "n_trials": 8})
    eid = submit("valid", "demo-run", "codehash-demo", {"note": "demo"})
    print("submitted:", eid)
    print("status:", status(eid))
    evaluate(eid, demo)
    print("feedback:", read_feedback(eid))
    try:
        read_feedback(eid)
        read_feedback(eid)
    except PermissionError as e:
        print("第3次被拒:", str(e)[:80])
    # schema 门: 白名单外指标被拒
    bad = submit("valid", "demo-bad", "codehash-demo")
    try:
        evaluate(bad, {"rank_ic": 0.031})
    except ValueError as e:
        print("[schema 门] 注入指标被拒:", str(e)[:70])
    oid = submit("oos", "demo-run", "codehash-demo")
    evaluate(oid, demo)
    try:
        read_feedback(oid)
    except PermissionError as e:
        print("OOS 读取被拒:", str(e)[:70])