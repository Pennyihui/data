# -*- coding: utf-8 -*-
"""
atomic.py — 原子写工具
======================
问题背景: L1/L2 的 parquet 之前用 ``pq.write_table`` 直接写目标路径。若进程在写盘
过程中被强杀 (OOM / 断电 / 任务被终止), 目标文件会留下一个**截断的** parquet, 下一次
读取时抛 ``ArrowInvalid: Parquet magic bytes not found in footer``。对增量型数据集
(如 universe_membership) 这会形成**自锁**: 每次运行先读旧文件 -> 读失败 -> 永远无法
前进, 即使数据源本身完全正常。

方案: 先写同目录下的 ``*.tmp`` 临时文件, 成功关闭后再 ``os.replace`` 原子改名。
``os.replace`` 在同一文件系统上是原子操作, 因此目标路径要么是旧的完整文件, 要么是
新的完整文件, 绝不会是半成品。
"""
from __future__ import annotations

import os

import pyarrow as pa
import pyarrow.parquet as pq


def atomic_write_table(table: pa.Table, path: str, compression: str = "snappy") -> str:
    """原子地把 Arrow Table 写成 parquet。

    写 ``path + ".tmp"`` 成功后再 os.replace 到 path。任何异常时清理临时文件,
    保证目标路径上的旧文件 (若存在) 不受影响。
    """
    tmp = f"{path}.tmp"
    try:
        pq.write_table(table, tmp, compression=compression)
        os.replace(tmp, path)  # 原子改名 (同目录, 同文件系统)
    except BaseException:
        # 任何失败 (含 KeyboardInterrupt / SystemExit) 都不留下 .tmp 垃圾
        try:
            if os.path.exists(tmp):
                os.remove(tmp)
        except OSError:
            pass
        raise
    return path


def atomic_write_parquet(df, path: str, compression: str = "snappy") -> str:
    """atomic_write_table 的 pandas DataFrame 便捷封装。"""
    return atomic_write_table(pa.Table.from_pandas(df, preserve_index=False),
                              path, compression=compression)


def safe_read_parquet(path: str):
    """读 parquet; 若文件损坏/截断返回 None 而不抛异常 (调用方自行决定重建策略)。"""
    import pandas as pd
    try:
        return pd.read_parquet(path)
    except Exception:  # noqa: BLE001
        return None
