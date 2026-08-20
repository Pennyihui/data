# -*- coding: utf-8 -*-
"""
l0.py — L0 原始层
=================
- 原始数据不可变: 只写入, 不修改
- 每批次 checksum (sha256)
- 元信息 sidecar: 源/参数/抓取时间/时间戳单位/行数
- 目录: raw/{venue_id}/{dataset}/{ingest_date}/{batch_id}.{ext} + .meta.json
"""
from __future__ import annotations

import hashlib
import json
import os
import shutil
from datetime import datetime, timezone

from .config import RAW_DIR


def sha256_file(path: str) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def write_raw_file(src_path: str, venue_id: str, dataset: str,
                   batch_id: str, source: dict, timestamp_unit: str = "ms",
                   ext: str | None = None) -> str:
    """把外部文件原样复制进 L0 原始层, 并写 .meta.json 元信息。

    src_path: 源文件 (CSV/JSON)
    source:   {url/api, params, fetched_at, ...}
    """
    ext = ext or os.path.splitext(src_path)[1].lstrip(".")
    ingest_date = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    dst_dir = os.path.join(RAW_DIR, venue_id, dataset, f"ingest_date={ingest_date}")
    os.makedirs(dst_dir, exist_ok=True)
    dst = os.path.join(dst_dir, f"{batch_id}.{ext}")
    shutil.copy2(src_path, dst)  # 原样复制, 不修改

    meta = {
        "batch_id": batch_id,
        "source_path": src_path,
        "source": source,
        "ingested_at": datetime.now(timezone.utc).isoformat(),
        "timestamp_unit": timestamp_unit,
        "timezone": "UTC",
        "checksum_sha256": sha256_file(dst),
        "file_size_bytes": os.path.getsize(dst),
        "immutable": True,
    }
    with open(f"{dst}.meta.json", "w", encoding="utf-8") as f:
        json.dump(meta, f, ensure_ascii=False, indent=2, default=str)
    return dst


def list_raw_batches(venue_id: str, dataset: str) -> list[dict]:
    """列出该数据集所有批次(含 meta)。"""
    root = os.path.join(RAW_DIR, venue_id, dataset)
    out = []
    if not os.path.isdir(root):
        return out
    for ingest in sorted(os.listdir(root)):
        d = os.path.join(root, ingest)
        if not os.path.isdir(d):
            continue
        for f in sorted(os.listdir(d)):
            if f.endswith(".meta.json"):
                with open(os.path.join(d, f), encoding="utf-8") as fh:
                    out.append(json.load(fh))
    return out
