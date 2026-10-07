# -*- coding: utf-8 -*-
"""registry_versioning.py — 特征注册表版本化 (快照 + 变更历史)

问题: 注册表当前是**进程内字典** (`registry._FEATURES`)。它的
FeatureSpec.version 只在**同一进程内**改表达式时自增 (1.0 -> 1.1), 重启就丢;
更没有"库在什么时间、由谁、改成了什么样"的记录。研究里"我上周跑的那个版本
是什么"这种问题无法回答, 而特征库一改, 历史结论就无法复现。

设计 (对应设计文档 4.0 "表达式变更即升版, 旧版保留, 保证历史研究可复现"):
  * **内容哈希**: 每个特征按 (name, expr) 算 content_hash —— 与进程无关的
    稳定标识, 用来发现"代码改了但没注意到"。
  * **快照**: 一份注册表的全量 JSON (特征名/表达式/版本/内容哈希/类别),
    带 id (时间戳 + 内容哈希) 与创建原因。落在 features/snapshots/ (入库,
    于是变更历史同时进 git, 可 diff 可回溯)。
  * **差异**: 两个快照之间 added / removed / changed 三类。
  * **漂移检测**: 当前内存注册表 vs 某个快照 —— 改了却没存快照时能发现。
"""
from __future__ import annotations

import hashlib
import json
import os
import time
from dataclasses import asdict, dataclass, field as dc_field

import pandas as pd

__all__ = ["Snapshot", "content_hash", "take_snapshot", "list_snapshots",
           "load_snapshot", "diff_snapshots", "drift_from", "latest_snapshot"]


def _snapshot_dir() -> str:
    return os.path.join(os.path.dirname(os.path.abspath(__file__)), "snapshots")


def content_hash(name: str, expr: str) -> str:
    """特征内容哈希 (name + expr)。与进程/版本号无关。"""
    h = hashlib.sha1()
    h.update(name.encode("utf-8"))
    h.update(b"\x1f")
    h.update(expr.encode("utf-8"))
    return h.hexdigest()[:12]


@dataclass
class Snapshot:
    """注册表在某一时刻的全量状态。"""

    snapshot_id: str
    created_at: str
    reason: str
    n_features: int
    features: dict          # name -> {expr, version, content_hash, category}
    registry_hash: str

    def to_dict(self) -> dict:
        return asdict(self)

    @staticmethod
    def from_dict(d: dict) -> "Snapshot":
        return Snapshot(**d)

    def summary(self) -> dict:
        return {"snapshot_id": self.snapshot_id, "created_at": self.created_at,
                "reason": self.reason, "n_features": self.n_features,
                "registry_hash": self.registry_hash}


def take_snapshot(reason: str = "manual", spec_map: dict | None = None) -> Snapshot:
    """给当前注册表拍一张快照并落盘 (返回 Snapshot)。"""
    from . import registry
    if spec_map is None:
        registry.load_library()
        spec_map = {s.name: s for s in registry.list_features(status="active")}
    feats = {}
    for name, spec in sorted(spec_map.items()):
        feats[name] = {"expr": spec.expr, "version": spec.version,
                        "content_hash": content_hash(name, spec.expr),
                        "category": spec.category,
                        "operators": list(spec.operators)}
    reg_hash = hashlib.sha1(
        "|".join(f"{n}={f['content_hash']}" for n, f in sorted(feats.items()))
        .encode("utf-8")).hexdigest()[:16]
    now = pd.Timestamp.utcnow()
    snap = Snapshot(
        snapshot_id=f"{now.strftime('%Y%m%dT%H%M%SZ')}-{reg_hash}",
        created_at=now.isoformat(), reason=reason,
        n_features=len(feats), features=feats, registry_hash=reg_hash)
    d = _snapshot_dir()
    os.makedirs(d, exist_ok=True)
    path = os.path.join(d, f"{snap.snapshot_id}.json")
    with open(path, "w", encoding="utf-8") as f:
        json.dump(snap.to_dict(), f, ensure_ascii=False, indent=2)
    return snap


def list_snapshots() -> list[dict]:
    """按时间列出所有快照的摘要 (最新在前)。"""
    d = _snapshot_dir()
    if not os.path.isdir(d):
        return []
    out = []
    for fn in sorted(os.listdir(d)):
        if not fn.endswith(".json"):
            continue
        try:
            with open(os.path.join(d, fn), encoding="utf-8") as f:
                out.append(Snapshot.from_dict(json.load(f)).summary())
        except Exception:
            continue
    out.sort(key=lambda s: s["created_at"], reverse=True)
    return out


def load_snapshot(snapshot_id: str) -> Snapshot:
    """按 id (或文件名前缀) 取快照。"""
    d = _snapshot_dir()
    path = os.path.join(d, f"{snapshot_id}.json")
    if not os.path.exists(path):
        hits = [fn for fn in os.listdir(d) if fn.startswith(snapshot_id)]
        if not hits:
            raise KeyError(f"找不到快照 {snapshot_id!r}")
        path = os.path.join(d, hits[0])
    with open(path, encoding="utf-8") as f:
        return Snapshot.from_dict(json.load(f))


def latest_snapshot() -> Snapshot | None:
    lst = list_snapshots()
    if not lst:
        return None
    return load_snapshot(lst[0]["snapshot_id"])


def diff_snapshots(old_id: str, new_id: str) -> dict:
    """两个快照之间的变更: 新增 / 删除 / 表达式变化 (按内容哈希判定)。"""
    a, b = load_snapshot(old_id), load_snapshot(new_id)
    na, nb = set(a.features), set(b.features)
    added = sorted(nb - na)
    removed = sorted(na - nb)
    changed = sorted(n for n in (na & nb)
                     if a.features[n]["content_hash"] != b.features[n]["content_hash"])
    return {
        "old": a.snapshot_id, "new": b.snapshot_id,
        "n_added": len(added), "n_removed": len(removed),
        "n_changed": len(changed),
        "added": added, "removed": removed, "changed": changed,
        "changed_detail": {n: {"old_expr": a.features[n]["expr"],
                               "new_expr": b.features[n]["expr"],
                               "old_version": a.features[n]["version"],
                               "new_version": b.features[n]["version"]}
                           for n in changed[:50]},
    }


def drift_from(snapshot_id: str, spec_map: dict | None = None) -> dict:
    """当前内存注册表 vs 某快照的漂移 —— 改了却没存快照时能发现。"""
    from . import registry
    if spec_map is None:
        registry.load_library()
        spec_map = {s.name: s for s in registry.list_features(status="active")}
    snap = load_snapshot(snapshot_id)
    now = {n: content_hash(n, s.expr) for n, s in spec_map.items()}
    then = {n: f["content_hash"] for n, f in snap.features.items()}
    added = sorted(set(now) - set(then))
    removed = sorted(set(then) - set(now))
    changed = sorted(n for n in set(now) & set(then) if now[n] != then[n])
    return {"snapshot": snap.snapshot_id, "n_added": len(added),
            "n_removed": len(removed), "n_changed": len(changed),
            "added": added, "removed": removed, "changed": changed,
            "drifted": bool(added or removed or changed)}


if __name__ == "__main__":  # pragma: no cover
    import argparse
    ap = argparse.ArgumentParser()
    ap.add_argument("--take", metavar="REASON", help="拍快照")
    ap.add_argument("--list", action="store_true")
    ap.add_argument("--diff", nargs=2, metavar=("OLD", "NEW"))
    ap.add_argument("--drift", metavar="SNAPSHOT_ID")
    args = ap.parse_args()
    if args.take:
        s = take_snapshot(args.take)
        print("已存快照:", json.dumps(s.summary(), ensure_ascii=False))
    elif args.diff:
        print(json.dumps(diff_snapshots(*args.diff), ensure_ascii=False, indent=2))
    elif args.drift:
        print(json.dumps(drift_from(args.drift), ensure_ascii=False, indent=2))
    else:
        for s in list_snapshots():
            print(json.dumps(s, ensure_ascii=False))