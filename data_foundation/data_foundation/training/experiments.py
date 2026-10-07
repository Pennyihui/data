# -*- coding: utf-8 -*-
"""experiments.py — 实验追踪 (试错账本 + 全局试验计数 N)

设计: docs/supervised-learning-protocol-design.md §9

两种账本, 同一份追加式存储 (与 oos_ledger 风格一致):
  - ExperimentRecord: 每次正式训练/评估留痕 (超参 + 指纹 + 三层指标)
  - trial_count(N)   : **全局试验计数**, 供给 DSR (评价协议 E3)

关键 (决策 E3): **开发池的试验也计入 N**。否则 Agent 可以在 oof 试 1000 次挑个
好看的再去 valid 碰运气, 而 DSR 的 N 仍是 1 —— N 失真则 DSR 失真, 整个多重
检验修正形同虚设。
"""
from __future__ import annotations

import json
import os
import threading
from dataclasses import dataclass, asdict
from datetime import datetime, timezone

from ..config import DATA_ROOT

__all__ = ["ExperimentRecord", "EXPERIMENTS_DIR", "record_experiment",
           "list_experiments", "trial_count"]

EXPERIMENTS_DIR = os.path.join(DATA_ROOT, "experiments")
LEDGER = os.path.join(EXPERIMENTS_DIR, "experiments.jsonl")
_LOCK = threading.Lock()


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


@dataclass(frozen=True)
class ExperimentRecord:
    run_id: str
    code_hash: str = ""
    model_family: str = ""
    model_params: dict = None
    feature_fingerprint: str = ""
    label_fingerprint: str = ""
    label_name: str = ""
    fold_plan: str = ""
    pool_id: str = "oof"
    metrics: dict = None
    metrics_version: str = "v1"
    evaluation_id: str = ""          # valid/oos 服务评时关联 eval_service
    created_at: str = ""
    note: str = ""

    def to_dict(self) -> dict:
        d = asdict(self)
        if d.get("model_params") is None:
            d["model_params"] = {}
        if d.get("metrics") is None:
            d["metrics"] = {}
        return d


def _append(rec: dict, path: str = LEDGER) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with _LOCK:
        with open(path, "a", encoding="utf-8") as f:
            f.write(json.dumps(rec, ensure_ascii=False, default=str) + "\n")
            f.flush()
            os.fsync(f.fileno())


def record_experiment(rec: ExperimentRecord, path: str = LEDGER) -> dict:
    """写入一条实验记录 (追加)。同 run_id 视为同一次试验。"""
    if not rec.run_id:
        raise ValueError("run_id 不能为空 (它是试验的唯一身份)")
    d = rec.to_dict()
    if not d.get("created_at"):
        d["created_at"] = _now()
    _append(d, path)
    return d


def _read_all(path: str = LEDGER) -> list[dict]:
    if not os.path.exists(path):
        return []
    out = []
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line:
                try:
                    out.append(json.loads(line))
                except json.JSONDecodeError:
                    continue
    return out


def list_experiments(pool_id: str | None = None,
                     path: str = LEDGER) -> list[dict]:
    rows = [r for r in _read_all(path)
            if pool_id is None or r.get("pool_id") == pool_id]
    return rows


def trial_count(pool_ids=("oof", "valid", "oos", "rolling_oos"),
                path: str = LEDGER) -> int:
    """全局试验计数 N = 去重 run_id 数 (含开发池 —— 决策 E3)。"""
    seen = set()
    for r in _read_all(path):
        if pool_ids and r.get("pool_id") not in tuple(pool_ids):
            continue
        rid = r.get("run_id")
        if rid:
            seen.add(rid)
    return len(seen)


if __name__ == "__main__":  # pragma: no cover
    import tempfile
    with tempfile.TemporaryDirectory() as td:
        p = os.path.join(td, "e.jsonl")
        record_experiment(ExperimentRecord(run_id="r1", pool_id="oof",
                                          model_family="linear"), path=p)
        record_experiment(ExperimentRecord(run_id="r1", pool_id="valid"), path=p)
        record_experiment(ExperimentRecord(run_id="r2", pool_id="oos"), path=p)
        assert trial_count(path=p) == 2, "同 run_id 只算一次"
        assert trial_count(pool_ids=("oof",), path=p) == 1, "按池过滤"
        print("experiments OK: N =", trial_count(path=p))