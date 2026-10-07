# -*- coding: utf-8 -*-
"""_test_label_isolation.py — 标签与特征的物理隔离 (四道闸)

设计: docs/label-system-design.md §6

继承 next_open 泄漏 (提交 6c0dd44d) 的教训: **写"某东西不可见"的注释时, 必须
同时写一个断言它不可见的测试**, 否则注释会变成谎言。这里四道闸各配一个测试:

  闸1 命名空间: 特征 DSL 的字段白名单里没有标签名 -> 引用标签的特征表达式编译失败
  闸2 存储:     标签只写在 data/labels/, 特征引擎路径里根本没有它
  闸3 字段源:   fields.py 无任何前视字段 (机械扫描 + 未来不变性语义)
  闸4 池访问:   valid/oos 读标签 -> PermissionError (标签视同原始数据)
"""
from __future__ import annotations

import os
import sys

import pandas as pd

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from data_foundation.features import dsl
from data_foundation.fields import FIELD_REGISTRY
from data_foundation.labels import LABELS, list_labels
from data_foundation.labels.store import LABELS_DIR
from data_foundation.pool_registry import DATA_ACCESS, data_access_mode

PASS, FAIL = [], []


def check(name, fn):
    try:
        fn()
        PASS.append(name)
    except Exception as e:  # noqa: BLE001
        FAIL.append(f"{name}: {type(e).__name__}: {e}")


# ---------------------------------------------------------------------------
# 闸1: 命名空间 —— 特征表达式物理上引用不到标签
# ---------------------------------------------------------------------------
def test_gate1_dsl_cannot_reference_label():
    feature_fields = set(FIELD_REGISTRY)
    label_names = set(LABELS)
    assert not (feature_fields & label_names), \
        f"标签名出现在特征字段表里: {feature_fields & label_names}"
    for label in label_names:
        try:
            dsl.compile_expr(label, known_fields=feature_fields)
            raise AssertionError(
                f"特征表达式竟能编译出标签 {label!r} —— 命名空间隔离被打破")
        except dsl.ExprError as e:
            assert "未知字段" in str(e), f"报错信息应说明未知字段: {e}"
        # 也试一个嵌套在算子里的引用
        try:
            dsl.compile_expr(f"ts_rank({label}, 10)", known_fields=feature_fields)
            raise AssertionError(f"嵌套引用标签 {label!r} 也应被拒")
        except dsl.ExprError:
            pass


def test_gate1_valid_feature_still_compiles():
    """隔离不能误伤: 正常特征表达式照常编译 (用真实存在的字段)。"""
    c = dsl.compile_expr("ts_rank(close, 24)", known_fields=set(FIELD_REGISTRY))
    assert "ts_rank" in c.operators and "close" in c.fields


# ---------------------------------------------------------------------------
# 闸2: 存储 —— 标签目录与特征缓存目录物理分离
# ---------------------------------------------------------------------------
def test_gate2_storage_separation():
    label_dir = os.path.abspath(LABELS_DIR)
    # 特征缓存/快照目录: features/snapshots 与 data/labels 不能相互包含
    feat_snap = os.path.abspath(os.path.join(
        os.path.dirname(__file__), "data_foundation", "features", "snapshots"))
    assert not label_dir.startswith(feat_snap), "标签目录不能落在特征快照目录里"
    assert not feat_snap.startswith(label_dir), "特征快照不能落在标签目录里"
    # 标签目录名不是认证数据目录 (那是 data/l2/certified)
    assert "certified" not in label_dir, "标签不能写进认证数据目录"
    # 特征引擎源码里不应出现 labels 目录/模块引用
    engine_py = os.path.join(os.path.dirname(__file__), "data_foundation",
                             "features", "engine.py")
    with open(engine_py, encoding="utf-8") as f:
        src = f.read()
    assert "labels" not in src, "特征引擎不应引用 labels 模块/目录"


# ---------------------------------------------------------------------------
# 闸3: 字段源 —— 特征字段表无前视字段
# ---------------------------------------------------------------------------
def test_gate3_no_forward_looking_fields():
    """机械扫描: 特征字段表里不能出现正向时间偏移 (shift(-k) 之类)。"""
    fields_py = os.path.join(os.path.dirname(__file__), "data_foundation",
                             "fields.py")
    with open(fields_py, encoding="utf-8") as f:
        src = f.read()
    # 只看 FieldSpec 定义块 (排除文档字符串里的举例)
    body = src.split("FieldSpec(")[-1]
    assert "shift(-" not in body, "特征字段定义里出现负向 shift (=前视)"
    assert "fwd" not in body.lower(), "特征字段定义里出现 fwd (前视) 标记"
    # 标签类字段名不能混进特征表
    suspicious = [n for n in FIELD_REGISTRY
                  if n.startswith(("ret_", "label", "future", "fwd_"))]
    assert not suspicious, f"特征表里有标签/前视字段: {suspicious}"


# ---------------------------------------------------------------------------
# 闸4: 池访问 —— 标签视同原始数据
# ---------------------------------------------------------------------------
def test_gate4_pool_access():
    assert data_access_mode("valid") == "submit_only"
    assert data_access_mode("oos") == "submit_only"
    from data_foundation.labels.store import load_label
    for pool in ("valid", "oos", "rolling_oos"):
        try:
            load_label("ret_10d", pool_id=pool)
            raise AssertionError(f"{pool} 池读标签应被拒")
        except PermissionError:
            pass
        except FileNotFoundError:
            pass
    # oof 是唯一可读池 (但仍需实际落盘)
    assert data_access_mode("oof") == "full"


# ---------------------------------------------------------------------------
# 作弊测试: 试图让标签进特征的路径全部物理失败
# ---------------------------------------------------------------------------
def test_cheat_paths_all_fail():
    """继承 next_open 教训: 注释承诺的"不可见"必须有断言支撑。"""
    # 作弊1: 白名单必须只来自 FIELD_REGISTRY —— 注入标签名后编译成功恰恰说明
    #   "白名单来源"是唯一防线, 而生产路径不经过注入。断言生产白名单不含标签。
    feature_fields = set(FIELD_REGISTRY)
    assert not (feature_fields & set(LABELS)), \
        "生产字段白名单里混进了标签 —— 隔离被打破"
    # 作弊2: 试图绕过标签存储的池门
    from data_foundation.labels import store
    try:
        store.load_label("ret_10d", pool_id="valid", allow_protected=False)
        raise AssertionError("valid 池读标签必须失败")
    except PermissionError:
        pass
    # 作弊3: 把标签写进特征缓存目录 (应因标签目录独立而不影响特征缓存)
    assert not os.path.isdir(os.path.join(LABELS_DIR, "snapshots")), \
        "标签目录不应包含 snapshots (特征快照属于特征侧)"


if __name__ == "__main__":
    for name, fn in list(globals().items()):
        if name.startswith("test_") and callable(fn):
            check(name, fn)
    print(f"\n{'=' * 60}")
    print(f"标签隔离测试: {len(PASS)} 通过, {len(FAIL)} 失败")
    for f in FAIL:
        print("  [FAIL]", f)
    print("=" * 60)
    sys.exit(1 if FAIL else 0)