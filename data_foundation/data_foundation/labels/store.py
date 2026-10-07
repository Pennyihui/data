# -*- coding: utf-8 -*-
"""store.py — 标签落盘 + 指纹 + 版本 + 池访问控制

设计: docs/label-system-design.md §8

三条不变式 (代码级):
  1. **标签与特征物理隔离**: 标签写在 data/labels/ (与 data/l2/certified 平级),
     特征引擎/缓存的路径里根本没有它 —— 标签不可能混进特征面板。
  2. **标签视同原始数据**: 标签 = 价格的未来函数, 拿到标签 ~= 拿到未来收益。
     valid/oos 由内部评估服务计算并保管, Agent 读标签 -> PermissionError
     (扩展 pool_registry.assert_can_read_data)。
  3. **不可变版本 + 输入指纹**: 规格或输入数据一变 -> 新指纹, 旧文件不可变。
"""
from __future__ import annotations

import hashlib
import json
import os
from datetime import datetime, timezone

import pandas as pd

from ..config import DATA_ROOT
from ..pool_registry import assert_can_read_data, data_access_mode
from .definitions import LabelResult, LabelSpec, get_label, list_labels
from .objectives import get_objective

__all__ = [
    "LABELS_DIR", "label_dir", "label_fingerprint", "save_label", "load_label",
    "load_meta", "list_stored", "meta_path",
]


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


LABELS_DIR = os.path.join(DATA_ROOT, "labels")


def label_dir(objective: str) -> str:
    return os.path.join(LABELS_DIR, objective)


# ---------------------------------------------------------------------------
# 指纹 (与特征缓存键同构: 规格 + 输入数据 + 池)
# ---------------------------------------------------------------------------
def _sha1(*parts: str) -> str:
    h = hashlib.sha1()
    for p in parts:
        h.update(str(p).encode("utf-8"))
        h.update(b"|")
    return h.hexdigest()


def label_fingerprint(spec: LabelSpec | str, *, pool_id: str = "oof",
                      data_fingerprint: str = "", assets: str = "") -> str:
    """标签缓存键。改任一成分 (口径/成本/horizon/数据/池/资产集) -> 新键。"""
    if isinstance(spec, str):
        spec = get_label(spec)
    parts = [spec.fingerprint(), f"pool={pool_id}",
             f"data={data_fingerprint}", f"assets={assets}"]
    return _sha1(*parts)


def _filename(spec: LabelSpec, fp: str) -> str:
    return f"{spec.name}__{spec.version}__{fp[:8]}.parquet"


def meta_path(objective: str, spec: LabelSpec, fp: str) -> str:
    return os.path.join(label_dir(objective), _filename(spec, fp).replace(
        ".parquet", ".meta.json"))


# ---------------------------------------------------------------------------
# 存
# ---------------------------------------------------------------------------
def save_label(result: LabelResult, *, pool_id: str = "oof",
               data_fingerprint: str = "", assets: str = "",
               pool_dir: str | None = None) -> dict:
    """落盘一个标签。返回 meta (含指纹)。旧版本不可变: 指纹不同 = 新文件。"""
    spec = result.spec
    fp = label_fingerprint(spec, pool_id=pool_id,
                           data_fingerprint=data_fingerprint, assets=assets)
    d = pool_dir or label_dir(spec.objective)
    os.makedirs(d, exist_ok=True)
    frame = pd.DataFrame({"label": result.values,
                          "label_available_at": result.available_at})
    path = os.path.join(d, _filename(spec, fp))
    frame.to_parquet(path, engine="pyarrow")          # index 保留 (MultiIndex)
    meta = {
        "label": spec.name, "objective": spec.objective, "kind": spec.kind,
        "version": spec.version, "fingerprint": fp,
        "spec_fingerprint": spec.fingerprint(),
        "horizon_bars": result.horizon_bars,
        "entry_offset": result.entry_offset, "exit_offset": result.exit_offset,
        "cost": {"taker_fee": spec.cost.taker_fee,
                 "slippage_bps": spec.cost.slippage_bps,
                 "round_trip": spec.cost.round_trip},
        "pool": pool_id, "data_fingerprint": data_fingerprint,
        "assets": assets, "n_rows": int(len(frame)),
        "n_labeled": int(frame["label"].notna().sum()),
        "created_at": _now(), "path": path,
    }
    with open(os.path.join(d, _filename(spec, fp).replace(
            ".parquet", ".meta.json")), "w", encoding="utf-8") as f:
        json.dump(meta, f, ensure_ascii=False, indent=2)
    return meta


# ---------------------------------------------------------------------------
# 读 (池访问控制在这里, 不是靠约定)
# ---------------------------------------------------------------------------
def load_meta(label: str, *, pool_id: str = "oof",
              version: str | None = None) -> dict:
    spec = get_label(label)
    d = label_dir(spec.objective)
    if not os.path.isdir(d):
        raise FileNotFoundError(f"标签 {label!r} 尚无落盘记录 ({d})")
    rows = []
    for fn in sorted(os.listdir(d)):
        if not fn.endswith(".meta.json"):
            continue
        with open(os.path.join(d, fn), encoding="utf-8") as f:
            m = json.load(f)
        if m.get("label") == label and m.get("pool") == pool_id and \
                (version is None or m.get("version") == version):
            rows.append(m)
    if not rows:
        raise FileNotFoundError(
            f"池 {pool_id} 下没有标签 {label!r} 的落盘记录"
            + (f" (版本 {version})" if version else ""))
    return rows[-1]


def load_label(label: str, *, pool_id: str = "oof", version: str | None = None,
               as_of=None, allow_protected: bool = False) -> pd.DataFrame:
    """读标签面板 (value + label_available_at)。

    valid/oos -> PermissionError (**标签视同原始数据**, 设计文档 §2 原则5)。
    内部评估服务用 allow_protected=True 绕过 (它是唯一有权读的服务端路径)。
    """
    if not allow_protected:
        assert_can_read_data(pool_id, "标签")
    spec = get_label(label)
    meta = load_meta(label, pool_id=pool_id, version=version)
    frame = pd.read_parquet(meta["path"], engine="pyarrow")
    if as_of is not None:
        cutoff = pd.Timestamp(as_of)
        cutoff = cutoff.tz_localize("UTC") if cutoff.tzinfo is None \
            else cutoff.tz_convert("UTC")
        frame = frame[frame.index.get_level_values("time") <= cutoff]
    frame.attrs["label_meta"] = meta
    return frame


def list_stored(pool_id: str | None = None) -> list[dict]:
    out = []
    if not os.path.isdir(LABELS_DIR):
        return out
    for obj in sorted(os.listdir(LABELS_DIR)):
        d = os.path.join(LABELS_DIR, obj)
        if not os.path.isdir(d):
            continue
        for fn in sorted(os.listdir(d)):
            if not fn.endswith(".meta.json"):
                continue
            with open(os.path.join(d, fn), encoding="utf-8") as f:
                m = json.load(f)
            if pool_id and m.get("pool") != pool_id:
                continue
            out.append({k: m.get(k) for k in
                        ("label", "objective", "pool", "version", "fingerprint",
                         "n_rows", "n_labeled", "created_at")})
    return out


if __name__ == "__main__":  # pragma: no cover
    import json as _json
    print("[注册标签]")
    for row in list_labels():
        print(" ", _json.dumps(row, ensure_ascii=False))
    print("[已落盘]", len(list_stored()), "个")
    try:
        load_label("ret_10d", pool_id="valid")
    except PermissionError as e:
        print("[自检] valid 池读标签被拒:", str(e)[:60])