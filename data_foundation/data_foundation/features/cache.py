# -*- coding: utf-8 -*-
"""cache.py — 特征落盘缓存 (设计文档决策 3)

决策 3 原文: "先不落盘 (F1-F3 阶段); 过早缓存引入 PIT 一致性风险 (上游修正后
缓存过期); 等算力成瓶颈再引入, 且**缓存 key 须含 特征版本 + 输入批次ID + 池**"。

触发条件已到: 292 个特征 / 10 资产 / 4 万行 = 约 5 分钟, 一次全量算完就扔掉
不合理 (尤其是调参时反复重算同一批特征)。

**缓存过期是这套设计要防的头号事故**: 上游修订历史数据后, 旧缓存仍在磁盘上,
拿它当"当前"数据用 = 用过期信息做研究, 而且**不会报错**。所以:

    key = 特征名 + 特征版本 + 表达式内容哈希
        + 输入数据指纹 (各数据集 manifest 的 row_count/coverage/certified_at)
        + 池 + 层 + 窗口 + 资产集合

输入数据指纹来自**认证 manifest** 而不是 mtime: 重建数据必然更新
certified_at/row_count, 指纹随之变化, 旧缓存自动失效。

存储布局 (每个特征一个文件, 便于**部分命中**):
    <root>/<pool>/<data_fp>/<window_fp>/<feature>/v<version>-<expr_hash>.parquet
    <root>/<pool>/<data_fp>/<window_fp>/<feature>/v<version>-<expr_hash>.meta.json
文件里存两列: value + data_available_at —— 可用时间必须跟着值一起落盘,
否则读回来的特征就失去了 PIT 契约。
"""
from __future__ import annotations

import hashlib
import json
import os
import time
from dataclasses import dataclass, field as dc_field

import pandas as pd

from ..config import CERTIFIED_DIR

__all__ = ["CacheKey", "FeatureCache", "data_fingerprint", "default_cache_root"]

_MANIFEST_CACHE: dict = {}
_META_SUFFIX = ".meta.json"


def default_cache_root() -> str:
    """缓存根目录 (默认在 data/derived/feature_cache, 已 gitignore)。"""
    return os.path.join(os.path.dirname(CERTIFIED_DIR), "derived", "feature_cache")


def _sha1(*parts: str) -> str:
    h = hashlib.sha1()
    for p in parts:
        h.update(str(p).encode("utf-8"))
        h.update(b"\x1f")
    return h.hexdigest()[:16]


def _load_manifest(dataset: str) -> dict:
    if dataset not in _MANIFEST_CACHE:
        path = os.path.join(CERTIFIED_DIR, dataset, "manifest.json")
        if os.path.exists(path):
            with open(path, encoding="utf-8") as f:
                _MANIFEST_CACHE[dataset] = json.load(f)
        else:
            _MANIFEST_CACHE[dataset] = {}
    return _MANIFEST_CACHE[dataset]


def data_fingerprint(datasets) -> str:
    """输入数据指纹: 相关数据集 manifest 的 (row_count, coverage, certified_at)
    之哈希。上游数据一重建, certified_at/row_count 必变, 指纹即变 -> 缓存自动失效。
    """
    parts = []
    for ds in sorted(set(datasets)):
        m = _load_manifest(ds)
        parts.append(_sha1(ds, m.get("row_count"), m.get("coverage_start"),
                           m.get("coverage_end"), m.get("certified_at")))
    return _sha1(*parts) if parts else "nodata"


@dataclass(frozen=True)
class CacheKey:
    """一个特征的缓存身份。缺一不可 —— 任何一项变了都必须重算。"""

    feature: str
    version: str
    expr_hash: str
    data_fp: str
    pool_id: str
    layer: str
    window_fp: str              # start/end/warmup/assets 的哈希
    window_desc: str = ""       # 人可读描述 (写进 meta, 便于排查)

    def path(self, root: str) -> str:
        return os.path.join(
            root, self.pool_id, self.data_fp, self.window_fp, self.feature,
            f"v{self.version}-{self.expr_hash}.parquet")


@dataclass
class CacheStats:
    hit: int = 0
    miss: int = 0
    store: int = 0
    seconds_saved: float = 0.0
    hit_features: list = dc_field(default_factory=list)

    def as_dict(self) -> dict:
        return {"hit": self.hit, "miss": self.miss, "store": self.store,
                "seconds_saved": round(self.seconds_saved, 1),
                "hit_features": sorted(self.hit_features)[:50]}


class FeatureCache:
    """特征落盘缓存 (可关闭)。engine 以 cache='auto'/'read'/'write'/'off' 使用。"""

    def __init__(self, root: str | None = None, enabled: bool = True):
        self.root = root or default_cache_root()
        self.enabled = enabled
        self.stats = CacheStats()

    # -- key 构造 ---------------------------------------------------------
    def make_key(self, spec, panel, scope: PoolScope,
                 assets=None) -> CacheKey:
        datasets = sorted({f.dataset for f in _field_specs(spec.expr)})
        data_fp = data_fingerprint(datasets)
        start = getattr(panel, "start", None)
        end = getattr(panel, "end", None)
        warmup = getattr(panel, "warmup", pd.Timedelta(0))
        # 资产集合必须进键: 算 [BTC] 与算 [BTC,ETH] 的面板索引不同, 共用缓存
        # 会让后者读到子集 (静默缺值)。
        asset_key = ",".join(sorted(assets or []))
        window_fp = _sha1(str(start), str(end), str(warmup), asset_key)
        desc = f"{str(start)[:10]}~{str(end)[:10]} warmup={warmup} n_assets={len(assets or [])}"
        return CacheKey(feature=spec.name, version=spec.version,
                        expr_hash=_sha1(spec.expr), data_fp=data_fp,
                        pool_id=scope.pool_id, layer=scope.layer,
                        window_fp=window_fp, window_desc=desc)

    # -- 读 ---------------------------------------------------------------
    def lookup(self, key: CacheKey):
        """命中返回 (values, avail, meta); 未命中返回 None。"""
        if not self.enabled:
            return None
        p = key.path(self.root)
        if not os.path.exists(p):
            self.stats.miss += 1
            return None
        try:
            df = pd.read_parquet(p)
        except Exception:
            self.stats.miss += 1
            return None
        if not isinstance(df.index, pd.MultiIndex) or \
                not {"value", "data_available_at"} <= set(df.columns):
            # 老版本缓存格式(索引丢失)或损坏 -> 当未命中 (宁可重算)
            self.stats.miss += 1
            return None
        meta = {}
        mp = p.replace(".parquet", _META_SUFFIX)
        if os.path.exists(mp):
            try:
                with open(mp, encoding="utf-8") as f:
                    meta = json.load(f)
            except Exception:
                meta = {}
        self.stats.hit += 1
        self.stats.hit_features.append(key.feature)
        if meta.get("compute_seconds"):
            self.stats.seconds_saved += float(meta["compute_seconds"])
        return df["value"], df["data_available_at"], meta

    # -- 写 ---------------------------------------------------------------
    def store(self, key: CacheKey, values: pd.Series, avail: pd.Series,
               meta_extra: dict | None = None) -> str:
        if not self.enabled:
            return ""
        p = key.path(self.root)
        os.makedirs(os.path.dirname(p), exist_ok=True)
        out = pd.DataFrame({"value": values.to_numpy(),
                            "data_available_at": avail.to_numpy()},
                           index=values.index)
        out.index.names = values.index.names
        # 注意: **不能用 atomic_write_parquet** —— 它 preserve_index=False 会把
        # 面板的 MultiIndex (base_asset,time) 丢掉, 读回来只剩 RangeIndex,
        # reindex 后全变 NaN。缓存必须自己写、保留索引 (并用临时文件+rename
        # 保证原子性)。
        import pyarrow as pa
        import pyarrow.parquet as pq
        tmp = p + ".tmp"
        pq.write_table(pa.Table.from_pandas(out, preserve_index=True),
                       tmp, compression="snappy")
        os.replace(tmp, p)
        meta = {
            "feature": key.feature, "version": key.version,
            "expr_hash": key.expr_hash, "data_fp": key.data_fp,
            "pool": key.pool_id, "layer": key.layer, "window": key.window_desc,
            "rows": int(len(out)),
            "non_null": int(out["value"].notna().sum()),
            "cached_at": pd.Timestamp.utcnow().isoformat(),
        }
        meta.update(meta_extra or {})
        mp = p.replace(".parquet", _META_SUFFIX)
        with open(mp, "w", encoding="utf-8") as f:
            json.dump(meta, f, ensure_ascii=False, indent=2)
        self.stats.store += 1
        return p

    # -- 运维 -------------------------------------------------------------
    def size_mb(self) -> float:
        if not os.path.isdir(self.root):
            return 0.0
        total = 0
        for dirpath, _, files in os.walk(self.root):
            for fn in files:
                if fn.endswith(".parquet"):
                    try:
                        total += os.path.getsize(os.path.join(dirpath, fn))
                    except OSError:
                        pass
        return round(total / 1e6, 2)

    def clear(self, pool_id: str | None = None) -> int:
        """清缓存。返回删除的文件数。pool_id=None 清全部。"""
        base = os.path.join(self.root, pool_id) if pool_id else self.root
        n = 0
        if os.path.isdir(base):
            for dirpath, _, files in os.walk(base, topdown=False):
                for fn in files:
                    os.remove(os.path.join(dirpath, fn))
                    n += 1
                if dirpath != base:
                    try:
                        os.rmdir(dirpath)
                    except OSError:
                        pass
        return n


def _field_specs(expr: str) -> list:
    """表达式用到的字段 -> FieldSpec (给 data_fingerprint 用)。

    这里只取**字段** (不含特征名): 特征依赖的其它特征由引擎递归装载,
    其底层字段也会出现在本批 field_need 里, 指纹因此覆盖整条依赖链的数据。
    """
    from ..fields import FIELD_REGISTRY
    import re
    out = []
    for m in re.finditer(r"[A-Za-z_][A-Za-z_0-9]*", expr):
        spec = FIELD_REGISTRY.get(m.group(0))
        if spec is not None and spec not in out:
            out.append(spec)
    return out


if __name__ == "__main__":  # pragma: no cover
    import argparse
    ap = argparse.ArgumentParser()
    ap.add_argument("--root", default=None)
    ap.add_argument("--clear", action="store_true")
    ap.add_argument("--pool", default=None)
    ap.add_argument("--stats", action="store_true")
    args = ap.parse_args()
    c = FeatureCache(root=args.root)
    if args.clear:
        print(f"已删除 {c.clear(args.pool)} 个文件")
    else:
        print(f"缓存目录: {c.root}")
        print(f"占用: {c.size_mb()} MB")