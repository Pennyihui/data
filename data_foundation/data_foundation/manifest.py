# -*- coding: utf-8 -*-
"""manifest.py — L2 manifest 生成/校验工具。"""
from __future__ import annotations

import json
import os
from datetime import datetime, timezone

from .schema import MANIFEST_FIELDS


def empty_manifest(dataset: str, schema_version: str) -> dict:
    return {
        "dataset": dataset,
        "schema_version": schema_version,
        "source_batches": [],
        "coverage_start": None,
        "coverage_end": None,
        "row_count": 0,
        "duplicate_count": 0,
        "gap_count": 0,
        "suspect_count": 0,
        "timestamp_unit": "us",
        "timezone": "UTC",
        "certification_status": "pending",
        "certified_at": None,
        "aggregation_rules": None,
        "quality_rule_version": None,
        "notes": [],
    }


def write_manifest(dataset_dir: str, manifest: dict):
    os.makedirs(dataset_dir, exist_ok=True)
    path = os.path.join(dataset_dir, "manifest.json")
    with open(path, "w", encoding="utf-8") as f:
        json.dump(manifest, f, ensure_ascii=False, indent=2, default=str)
    return path


def load_manifest(dataset_dir: str) -> dict:
    path = os.path.join(dataset_dir, "manifest.json")
    if not os.path.exists(path):
        return empty_manifest(os.path.basename(dataset_dir), "unknown")
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def certify_manifest(manifest: dict, coverage_start, coverage_end, row_count,
                     duplicate_count, gap_count, suspect_count,
                     quality_rule_version: str) -> dict:
    """填写认证结果。异常数据不修改, 只记录计数。"""
    manifest["coverage_start"] = coverage_start
    manifest["coverage_end"] = coverage_end
    manifest["row_count"] = row_count
    manifest["duplicate_count"] = duplicate_count
    manifest["gap_count"] = gap_count
    manifest["suspect_count"] = suspect_count
    manifest["quality_rule_version"] = quality_rule_version
    manifest["certification_status"] = "certified"
    manifest["certified_at"] = datetime.now(timezone.utc).isoformat()
    return manifest


def validate_manifest(manifest: dict) -> list:
    """返回缺失字段列表。"""
    return [f for f in MANIFEST_FIELDS if f not in manifest]
