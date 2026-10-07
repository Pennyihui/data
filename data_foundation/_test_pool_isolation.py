# -*- coding: utf-8 -*-
"""_test_pool_isolation.py — 三池隔离验证 (2026-10-05 定案)

定案的模型:
    oof  开发池: Agent 可读原始数据 / 算特征 / 看结果
    valid 反馈池: Agent 不可读原始数据; 提交 -> 内部服务评 -> 结果**限次数**返给
                  Agent (防反复看结果调参把反馈池也过拟合掉)
    oos   OOS:    Agent 不可读原始数据; 提交 -> 服务评 -> 结果 Agent 永不返回;
                  人 (assert_human) 可读做 go/no-go

守的不变式 (代码级, 不靠约定):
  1. valid/oos 的原始数据读取与特征计算一律 PermissionError (堵"自己算业绩"
     绕过 Agent 盲的绕门 —— 实测原实现里 Agent 能 bind oos 读 4080 行 K线)
  2. 反馈池结果限次可取, 超限拒绝
  3. OOS 结果 Agent 永不返回 (反馈池接口与 OOS 账本双重拦截)
"""
from __future__ import annotations

import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import data_foundation.mcp_server as mcp  # noqa: E402
from data_foundation import eval_service as E  # noqa: E402
from data_foundation import oos_ledger as L  # noqa: E402
from data_foundation.pool_registry import (VALID_FEEDBACK_VIEW_LIMIT,  # noqa: E402
                                           assert_can_read_data,
                                           data_access_mode)

PASS, FAIL = [], []


def check(name, cond, detail=""):
    ok = bool(cond)
    (PASS if ok else FAIL).append(name)
    print(f"  [{'PASS' if ok else 'FAIL'}] {name}" + (f"  {detail}" if detail else ""))
    return ok


def bind(pool):
    """重置会话绑定 (mcp 单会话只绑一次, 测试里需绕过)。"""
    mcp._SCOPE = None
    return mcp.tool_bind_pool(pool)


def raises_perm(fn, *a, **kw):
    try:
        fn(*a, **kw)
        return False
    except PermissionError:
        return True
    except Exception:
        return False


print("=" * 74)
print("三池隔离验证")
print("=" * 74)

# ---------------------------------------------------------------------------
print("\n1) 访问策略表")
check("oof 策略 = full", data_access_mode("oof") == "full")
check("valid 策略 = submit_only", data_access_mode("valid") == "submit_only")
check("oos 策略 = submit_only", data_access_mode("oos") == "submit_only")
check("反馈池限次 = 2", VALID_FEEDBACK_VIEW_LIMIT == 2, str(VALID_FEEDBACK_VIEW_LIMIT))

# ---------------------------------------------------------------------------
print("\n2) 开发池: 全部放行")
bind("oof")
try:
    r = mcp.tool_query_candles("binance", "BTC-USDT", as_of="2022-06-01")
    check("读 K 线", r["n_rows"] > 0, f"rows={r['n_rows']}")
except Exception as exc:
    check("读 K 线", False, str(exc)[:60])
try:
    r = mcp.tool_compute_features(["vol_24h"], start="2022-01-01",
                                  end="2022-01-15", assets=["BTC"])
    check("算特征", r["n_rows"] > 0, f"rows={r['n_rows']}")
except Exception as exc:
    check("算特征", False, str(exc)[:60])

# ---------------------------------------------------------------------------
print("\n3) OOS 池: 拒绝读数据与算特征 (堵绕门)")
bind("oos")
check("read K 线被拒", raises_perm(mcp.tool_query_candles, "binance", "BTC-USDT"))
check("读衍生品被拒", raises_perm(mcp.tool_query_derivatives,
                                "binance", "BTC-USDT", "derivatives_funding"))
check("读宇宙被拒", raises_perm(mcp.tool_query_universe))
check("算特征被拒", raises_perm(mcp.tool_compute_features, ["vol_24h"],
                              start="2025-08-01", end="2025-08-15",
                              assets=["BTC"]))
check("底层门禁函数直接调用也被拒",
      raises_perm(assert_can_read_data, "oos", "原始数据"))
check("开发池底层门禁放行", (assert_can_read_data("oof", "x") is None))

# ---------------------------------------------------------------------------
print("\n4) 反馈池: 同样拒绝读数据")
bind("valid")
check("read K 线被拒", raises_perm(mcp.tool_query_candles, "binance", "BTC-USDT"))
check("算特征被拒", raises_perm(mcp.tool_compute_features, ["vol_24h"],
                              start="2024-06-01", end="2024-06-15",
                              assets=["BTC"]))

# ---------------------------------------------------------------------------
print("\n5) 反馈池通道: 提交 -> 服务评 -> 限次取结果")
# 评估记录必须过 schema 门 (评价协议 §4): 用 build_metrics 组装合法三层指标
from data_foundation.evaluation.metrics import build_metrics  # noqa: E402


def _demo_metrics(rank_ic: float, sharpe: float = 1.0) -> dict:
    return build_metrics(
        {"ic_mean": rank_ic * 1.1, "rank_ic_mean": rank_ic},
        {"ann_return_net": 0.15, "sharpe": sharpe, "max_drawdown": -0.12},
        {"psr": 0.97, "dsr": 0.93, "n_trials": 5})


eid = E.submit("valid", "run-iso-1", "hash-feedback", {"note": "iso test"})
check("提交返回 evaluation_id", bool(eid), eid)
check("初始状态 pending", E.status(eid)["status"] == "pending")
E.evaluate(eid, _demo_metrics(0.031))
check("服务评估后状态 evaluated", E.status(eid)["status"] == "evaluated")
fb1 = E.read_feedback(eid)
check("第1次取结果成功",
      fb1["metrics"]["prediction"]["rank_ic_mean"] == 0.031,
      str(fb1["metrics"])[:80])
fb2 = E.read_feedback(eid)
check("第2次取结果成功", fb2["views_used"] == 2)
check("第3次被拒 (限次数)", raises_perm(E.read_feedback, eid))
# schema 门: 白名单外指标拒绝入库 (评价协议 §4 不许"注入指标")
bad_eid = E.submit("valid", "run-iso-bad", "hash-bad")
try:
    E.evaluate(bad_eid, {"rank_ic": 0.5})
    check("schema 门拒白名单外指标", False, "竟然入库了")
except ValueError:
    check("schema 门拒白名单外指标", True)

# ---------------------------------------------------------------------------
print("\n6) OOS 通道: Agent 永不拿结果, 人可读")
oid = E.submit("oos", "run-iso-1", "hash-oos")
E.evaluate(oid, _demo_metrics(0.012))
check("OOS 走反馈接口被拒", raises_perm(E.read_feedback, oid))
try:
    L.oos_result_read(oid, reader="agent")
    check("人审通道 Agent 被拒", False, "竟然通过了")
except PermissionError:
    check("人审通道 Agent 被拒", True)
# 人读: 需给一个非 agent 前缀身份
try:
    res = L.oos_result_read(oid, reader="researcher_zhang")
    check("人可读 OOS 结果", isinstance(res, dict) and res.get("metrics"),
          str(res)[:60])
except Exception as exc:
    check("人可读 OOS 结果", False, str(exc)[:60])

# ---------------------------------------------------------------------------
print("\n7) 跨池不串: 反馈池提交不能当 OOS 结果读")
eid2 = E.submit("valid", "run-x", "h2")
E.evaluate(eid2, _demo_metrics(0.05))
check("valid 提交按 valid 读", E.read_feedback(eid2)["pool"] == "valid")

# ---------------------------------------------------------------------------
print("\n8) 策略查询工具")
p = mcp.tool_pool_access_policy("valid")
check("策略工具返回可读标记", p["data_access"] == "submit_only"
      and p["can_read_raw"] is False)

print("\n" + "=" * 74)
print(f"结果: {len(PASS)} 通过 / {len(FAIL)} 失败")
if FAIL:
    for f_ in FAIL:
        print("   -", f_)
print("=" * 74)
sys.exit(1 if FAIL else 0)