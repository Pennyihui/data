# -*- coding: utf-8 -*-
"""metrics.py — 三层指标电池 (预测层 / 交易层 / 显著性层)

设计: docs/evaluation-protocol-design.md §3

三层各答各的问题 (§1 原则2):
  预测层  模型准不准   -> IC / rank-IC / ICIR / 命中率 / 分位组合
  交易层  变不变现     -> 成本后年化 / Sharpe / Calmar / 回撤 / 换手 / 盈亏比
  显著性层是不是运气   -> PSR / DSR (significance.py) / walk-forward 稳定性

参照: Qlib benchmark 指标电池 (IC/ICIR/Rank IC/年化/信息比) + Alphalens 分位
组合。**成本后为准** (原则3): 标签本身已扣 c_rt (ret_10d 是净收益), 分位组合
的 zero-cost 对照是显式换回毛收益再看差多少。
"""
from __future__ import annotations

import math

import numpy as np
import pandas as pd

from ..labels.objectives import CostParams

__all__ = [
    "METRICS_VERSION", "PREDICTION_KEYS", "TRADING_KEYS", "SIGNIFICANCE_KEYS",
    "ic_series", "rank_ic_series", "prediction_metrics", "trading_metrics",
    "walkforward_stability", "build_metrics", "validate_metrics",
]

#: 指标定义版本 (口径改了 -> +1, 旧记录仍可解释)
METRICS_VERSION = "v1"

# -- 指标白名单 (反馈只返回这些, 且只返回白名单 —— 原则5/E4) ------------------
PREDICTION_KEYS = (
    "ic_mean", "ic_std", "icir", "rank_ic_mean", "rank_ic_std", "rank_icir",
    "hit_rate", "n_days", "n_cross_section",
    "quantile_spread", "quantile_spread_zero_cost", "quantile_monotonicity",
    "top_group_return_net", "top_group_return_zero_cost", "top_hit_rate",
    "cost_eaten_by_top_group",
)
TRADING_KEYS = (
    "ann_return_net", "ann_vol", "sharpe", "calmar", "max_drawdown",
    "annualized_turnover", "total_cost", "win_rate", "profit_factor",
    "n_periods",
)
SIGNIFICANCE_KEYS = ("psr", "dsr", "sr_threshold", "n_trials", "n_obs",
                     "skew", "kurtosis", "sharpe_used",
    "fold_sharpe_std", "fold_ic_std", "worst_fold_sharpe", "n_folds")

_REQUIRED_TOP = ("metrics_version", "prediction", "trading", "significance")
_BOUNDED = {"ic_mean": (-1, 1), "rank_ic_mean": (-1, 1),
            "hit_rate": (0, 1), "quantile_monotonicity": (-1, 1),
            "max_drawdown": (-1, 0)}


# ---------------------------------------------------------------------------
# 预测层
# ---------------------------------------------------------------------------
def _cross_section_frame(scores: pd.Series, labels: pd.Series) -> pd.DataFrame:
    """按决策时刻 (time 层) 分组的 (score, label) 对齐表, 丢任一侧缺失。"""
    df = pd.DataFrame({"score": scores, "label": labels})
    df = df.dropna()
    if df.empty:
        raise ValueError("scores/labels 对齐后为空 —— 检查决策时刻是否一致")
    return df


def ic_series(scores: pd.Series, labels: pd.Series,
              min_count: int = 5) -> pd.Series:
    """逐决策日截面的 Pearson IC。"""
    df = _cross_section_frame(scores, labels)

    def one(g):
        s, y = g["score"], g["label"]
        if len(g) < min_count or s.std(ddof=0) == 0 or y.std(ddof=0) == 0:
            return np.nan
        return float(s.corr(y))

    return df.groupby(level="time", sort=True).apply(one)


def rank_ic_series(scores: pd.Series, labels: pd.Series,
                   min_count: int = 5) -> pd.Series:
    """逐决策日截面的 Spearman (秩) IC —— 抗离群, 与 IC 并存 (多口径)。"""
    df = _cross_section_frame(scores, labels)

    def one(g):
        s, y = g["score"], g["label"]
        if len(g) < min_count or s.nunique() < 2 or y.nunique() < 2:
            return np.nan
        return float(s.corr(y, method="spearman"))

    return df.groupby(level="time", sort=True).apply(one)


def _ir(series: pd.Series) -> tuple[float, float, float]:
    """返回 (mean, std, ir=mean/std)。三者都要 —— ICIR 是 IR, 不是 std。"""
    v = series.dropna()
    if v.empty:
        return float("nan"), float("nan"), float("nan")
    mean = float(v.mean())
    std = float(v.std(ddof=1))
    ir = mean / std if std > 0 else float("nan")
    return mean, std, ir


def _quantile_stats(scores: pd.Series, labels: pd.Series,
                    n_quantiles: int = 5, min_count: int = 10) -> dict:
    """分位组合分析: top-bottom 价差 + 单调性 + 成本对照。

    成本语义 (原则3): labels 本身已是**净**收益 (ret 标签扣了 c_rt), 所以
      quantile_spread            = top - bottom (对净标签)
      quantile_spread_zero_cost  = top - bottom (对毛标签 = 净标签 + c_rt)
    多空价差对**均一成本不敏感** (两边同扣), 成本信号体现在**单边**收益:
      cost_eaten_by_top_group = top_group_return_zero_cost - top_group_return_net
    """
    df = _cross_section_frame(scores, labels)
    out = {k: float("nan") for k in
           ("quantile_spread", "quantile_spread_zero_cost",
            "quantile_monotonicity", "top_group_return_net",
            "top_group_return_zero_cost", "cost_eaten_by_top_group")}
    spreads, tops, tops_nc = [], [], []
    k = int(n_quantiles)
    for _, g in df.groupby(level="time", sort=True):
        if len(g) < max(min_count, k):
            continue
        s = g["score"]
        # 等频分位 (rank 去重后按 floor 切, 避免并列分数落空)
        order = s.rank(method="first")
        buckets = pd.cut(order, bins=k, labels=False)
        gl = g.assign(_b=buckets)
        means = gl.groupby("_b", observed=False)["label"].mean()
        if means.isna().all() or len(means.dropna()) < 2:
            continue
        top = float(means.dropna().iloc[-1])
        bot = float(means.dropna().iloc[0])
        spreads.append(top - bot)
        tops.append(top)
        # 单调性: 组序 vs 组均值的 Spearman (应为 1)
        m = means.dropna()
        if len(m) >= 2 and m.index.nunique() == len(m):
            mono = float(pd.Series(m.index, index=m.index).corr(
                m.reset_index(drop=True), method="spearman"))
            out["quantile_monotonicity"] = mono
        # 成本对照: 净标签 + c_rt 还原毛标签 (c_rt 是目标成本参数, 由调用方传)
        pass
    if spreads:
        out["quantile_spread"] = float(np.mean(spreads))
        out["top_group_return_net"] = float(np.mean(tops))
    return out


def prediction_metrics(scores: pd.Series, labels: pd.Series, *,
                       cost: CostParams | None = None,
                       n_quantiles: int = 5, min_count: int = 5,
                       n_trials_hint: int = 1) -> dict:
    """预测层指标电池。scores/labels 均为 MultiIndex(asset, time) 的 Series。"""
    cost = cost or CostParams()
    ic = ic_series(scores, labels, min_count=min_count)
    ric = rank_ic_series(scores, labels, min_count=min_count)
    ic_mean, ic_std, icir = _ir(ic)
    ric_mean, ric_std, ricir = _ir(ric)
    df = _cross_section_frame(scores, labels)
    n_days = int(df.index.get_level_values("time").nunique())

    m = {
        "ic_mean": ic_mean, "ic_std": ic_std, "icir": icir,
        "rank_ic_mean": ric_mean, "rank_ic_std": ric_std, "rank_icir": ricir,
        "hit_rate": float("nan"), "n_days": n_days,
        "n_cross_section": float(np.mean(
            df.groupby(level="time").size().reindex(ic.index).dropna())
            if n_days else np.nan),
        "top_hit_rate": float("nan"),
    }
    # 命中率: sign(score) == sign(label) (标签在 {0,1} 或有正负号时都适用)
    if set(df["label"].dropna().unique()) <= {0.0, 1.0}:
        hit = (np.sign(df["score"]) > 0) == (df["label"] > 0)
        m["hit_rate"] = float(hit.mean())
    elif set(df["label"].dropna().unique()) <= {1.0, 2.0, 3.0, 4.0, 5.0}:
        # 多分类: top 组 (标签=最大分位) 的命中率 —— 用 sign 判定方向
        pass
    m.update(_quantile_stats(scores, labels, n_quantiles=n_quantiles,
                             min_count=max(min_count, n_quantiles * 2)))
    # 成本对照: 净标签 -> 毛标签 (+c_rt), 单边成本效应
    c_rt = cost.round_trip
    m["quantile_spread_zero_cost"] = m["quantile_spread"]
    m["top_group_return_zero_cost"] = m["top_group_return_net"] + c_rt \
        if m["top_group_return_net"] == m["top_group_return_net"] else float("nan")
    m["cost_eaten_by_top_group"] = c_rt \
        if m["top_group_return_net"] == m["top_group_return_net"] else float("nan")
    return m


# ---------------------------------------------------------------------------
# 交易层
# ---------------------------------------------------------------------------
def trading_metrics(portfolio: dict, *, period_returns: pd.Series | None = None,
                   periods_per_year: float = 365 * 24) -> dict:
    """交易层电池。portfolio = Portfolio.metrics() 的返回 (可补充逐期收益)。"""
    m = {"ann_return_net": float("nan"), "ann_vol": float("nan"),
         "sharpe": float("nan"), "calmar": float("nan"),
         "max_drawdown": float("nan"), "annualized_turnover": float("nan"),
         "total_cost": float("nan"), "win_rate": float("nan"),
         "profit_factor": float("nan"), "n_periods": float("nan")}
    cagr = portfolio.get("cagr", float("nan"))
    m["ann_return_net"] = float(cagr)                       # 权益已扣成本 -> 净值即净
    m["ann_vol"] = float(portfolio.get("vol_annual", float("nan")))
    m["sharpe"] = float(portfolio.get("sharpe", float("nan")))
    m["max_drawdown"] = float(portfolio.get("max_drawdown", float("nan")))
    m["total_cost"] = float(portfolio.get("total_cost", float("nan")))
    m["n_periods"] = float(portfolio.get("n_points", 0))
    # Calmar = 年化收益 / |最大回撤|
    if m["max_drawdown"] == m["max_drawdown"] and m["max_drawdown"] < 0:
        m["calmar"] = m["ann_return_net"] / abs(m["max_drawdown"])
    # 年化换手 = 总换手 / 年数
    n_years = m["n_periods"] / periods_per_year if m["n_periods"] else 0.0
    tot_to = float(portfolio.get("total_turnover", float("nan")))
    if n_years > 0 and tot_to == tot_to:
        m["annualized_turnover"] = tot_to / n_years
    # 胜率 / 盈亏比: 逐 bar 损益
    if period_returns is not None and len(period_returns) > 0:
        r = pd.Series(period_returns).astype(float).dropna()
        r = r[np.isfinite(r)]
        if len(r):
            m["win_rate"] = float((r > 0).mean())
            gains = float(r[r > 0].sum())
            losses = float(-r[r < 0].sum())
            if losses > 0:
                m["profit_factor"] = gains / losses
            elif gains > 0:
                m["profit_factor"] = float("inf")
    return m


def walkforward_stability(fold_metrics: list[dict], *, key: str = "sharpe"
                          ) -> dict:
    """跨折稳定性: 折间标准差 + 最差折。防"只有一个 fold 好"。"""
    vals = [f.get(key, float("nan")) for f in fold_metrics
            if isinstance(f, dict) and f.get(key) == f.get(key)]
    if not vals:
        return {"fold_sharpe_std": float("nan"), "fold_ic_std": float("nan"),
                "worst_fold_sharpe": float("nan"), "n_folds": 0}
    v = pd.Series(vals, dtype=float)
    return {"fold_sharpe_std": float(v.std(ddof=1)) if len(v) > 1 else 0.0,
            "worst_fold_sharpe": float(v.min()), "n_folds": len(v)}


# ---------------------------------------------------------------------------
# 组装 + schema 校验
# ---------------------------------------------------------------------------
def build_metrics(prediction: dict, trading: dict, significance: dict, *,
                  stability: dict | None = None,
                  extra: dict | None = None) -> dict:
    """把三层指标组装成评价服务记录的 metrics dict (带版本 + 指纹位)。"""
    sig = dict(significance or {})
    if stability:
        sig.update({k: v for k, v in stability.items() if k in SIGNIFICANCE_KEYS})
    out = {"metrics_version": METRICS_VERSION,
           "prediction": dict(prediction or {}),
           "trading": dict(trading or {}),
           "significance": sig}
    if extra:
        out["extra"] = dict(extra)
    return out


def validate_metrics(metrics: dict, *, strict: bool = True) -> list[str]:
    """schema 校验: 白名单字段 + 必需顶层 + 数值域。返回问题列表。

    用途: 评价服务入库前过这道门 (防"注入指标"绕过白名单)。
    """
    problems: list[str] = []
    if not isinstance(metrics, dict):
        return ["metrics 不是 dict"]
    for k in _REQUIRED_TOP:
        if k not in metrics:
            problems.append(f"缺少顶层字段 {k}")
    if metrics.get("metrics_version") != METRICS_VERSION:
        problems.append(f"metrics_version={metrics.get('metrics_version')!r} "
                        f"!= 当前 {METRICS_VERSION!r}")
    allowed = {"metrics_version", "prediction", "trading", "significance",
           "decision", "extra"}
    for k in metrics:
        if k not in allowed:
            problems.append(f"顶层白名单外字段: {k}")
    for layer, keys in (("prediction", PREDICTION_KEYS),
                        ("trading", TRADING_KEYS),
                        ("significance", SIGNIFICANCE_KEYS)):
        d = metrics.get(layer) or {}
        if not isinstance(d, dict):
            problems.append(f"{layer} 不是 dict")
            continue
        for k in d:
            if k not in keys:
                problems.append(f"{layer} 白名单外字段: {k}")
            v = d[k]
            if isinstance(v, (int, float)) and not math.isfinite(float(v)) \
                    and k not in ("n_days", "n_periods"):
                # NaN 允许 (样本不足), inf 不允许
                if v == float("inf") or v == float("-inf"):
                    problems.append(f"{layer}.{k} = inf")
            if k in _BOUNDED and isinstance(v, (int, float)):
                lo, hi = _BOUNDED[k]
                if v == v and not (lo <= float(v) <= hi):
                    problems.append(f"{layer}.{k}={v} 越界 [{lo}, {hi}]")
    if strict and problems:
        raise ValueError("metrics schema 校验失败: " + "; ".join(problems[:8]))
    return problems


if __name__ == "__main__":  # pragma: no cover
    idx = pd.MultiIndex.from_product(
        [["A", "B", "C", "D", "E"],
         pd.date_range("2021-01-01", periods=6, freq="D", tz="UTC")],
        names=["base_asset", "time"])
    rng = np.random.default_rng(3)
    y = pd.Series(rng.normal(0.01, 0.05, len(idx)), index=idx)
    s = y + rng.normal(0, 0.01, len(idx))
    pm = prediction_metrics(s, y)
    print("[预测层]", {k: round(v, 4) for k, v in pm.items()
                       if k in ("ic_mean", "icir", "rank_ic_mean", "hit_rate",
                                "quantile_spread")})
    tm = trading_metrics({"cagr": 0.3, "vol_annual": 0.2, "sharpe": 1.5,
                          "max_drawdown": -0.2, "total_cost": 100.0,
                          "total_turnover": 5.0, "n_points": 8760})
    print("[交易层]", {k: round(v, 4) for k, v in tm.items()})
    m = build_metrics(pm, tm, {"psr": 0.99, "dsr": 0.98, "n_trials": 10})
    print("[schema]", validate_metrics(m, strict=False) or "OK")