# -*- coding: utf-8 -*-
"""
data_foundation — 加密量化数据底座 (L0 Raw / L1 Normalized / L2 Certified)
==========================================================================
依据 docs/crypto-data-foundation-research.md 实现:
  L0 Raw        原始数据, 不可变, checksum, 源元信息
  L1 Normalized 统一 schema 的 Parquet (UTC, venue/instrument/interval)
  L2 Certified  质量认证 + manifest + 研究快照 (研究默认读取层)
L3 Features 不在本包范围。

用法:
  python -m data_foundation.run_pipeline --stage l0     # 仅 L0
  python -m data_foundation.run_pipeline --stage all    # L0->L1->L2
"""

from .config import (BASE_DIR, CERTIFIED_DIR, L1_DIR, L0_DIR, RAW_DIR,
                     MVP_ASSETS, INTERVALS)
from . import manifest

__all__ = ["BASE_DIR", "RAW_DIR", "L0_DIR", "L1_DIR", "CERTIFIED_DIR",
           "MVP_ASSETS", "INTERVALS", "manifest"]
__version__ = "0.1.0"
