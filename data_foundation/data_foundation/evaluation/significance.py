# -*- coding: utf-8 -*-
"""significance.py — PSR / DSR (多重检验修正)

设计: docs/evaluation-protocol-design.md §3.3

为什么需要它
------------
试 N 个模型, 总有 Sharpe 好看的。裸 Sharpe 阈值在 N 大时几乎必然被"运气"跨过。
López de Prado (Advances in Financial Machine Learning ch.7; Bailey & López de
Prado, JPM 2014) 给出修正:

  PSR(SR*)  单次试验下 Sharpe 超过基准 SR* 的概率 (对非正态做偏度/峰度修正)
  DSR       把基准抬高到 "N 次独立试验下期望出现的最大 Sharpe", 再算 PSR

        E[max SR_N] ≈ √V[SR] · [ (1-γ)·Φ⁻¹(1 - 1/N) + γ·Φ⁻¹(1 - 1/(N·e)) ]

    γ = 0.5772 (Euler-Mascheroni),  V[SR] ≈ 1/(T-1) (正态近似)
    T = 观测数, γ3/γ4 = 收益的偏度/峰度

N 由全局试验计数器供给 (含开发池试验) —— 见 training/experiments.py。
"""
from __future__ import annotations

import math

import numpy as np

__all__ = [
    "probabilistic_sharpe_ratio", "expected_max_sr", "deflated_sharpe_ratio",
    "significance_metrics", "go_no_go", "EULER_MASCHERONI",
]

EULER_MASCHERONI = 0.5772156649015329


def _norm_cdf(x: float) -> float:
    return 0.5 * (1.0 + math.erf(x / math.sqrt(2.0)))


def _norm_ppf(p: float) -> float:
    """标准正态分位数。Acklam 有理逼近, |误差| < 1.15e-9 (足够 DSR 用)。"""
    if not (0.0 < p < 1.0):
        raise ValueError(f"p 必须落在 (0,1), 收到 {p}")
    a = [-3.969683028665376e+01, 2.209460984245205e+02, -2.759285104469687e+02,
         1.383577518672690e+02, -3.066479806614716e+01, 2.506628277459239e+00]
    b = [-5.447609879822406e+01, 1.615858368580409e+02, -1.556989798598866e+02,
         6.680131188771972e+01, -1.328068155288572e+01]
    c = [-7.784894002430293e-03, -3.223964580411365e-01, -2.400758277161838e+00,
         -2.549732539343734e+00, 4.374664141464968e+00, 2.938163982698783e+00]
    d = [7.784695709041462e-03, 3.224671290700398e-01, 2.445134137142996e+00,
         3.754408661907416e+00]
    plow, phigh = 0.02425, 1 - 0.02425
    if p < plow:
        q = math.sqrt(-2 * math.log(p))
        return (((((c[0] * q + c[1]) * q + c[2]) * q + c[3]) * q + c[4]) * q + c[5]) / \
               ((((d[0] * q + d[1]) * q + d[2]) * q + d[3]) * q + 1)
    if p > phigh:
        q = math.sqrt(-2 * math.log(1 - p))
        return -(((((c[0] * q + c[1]) * q + c[2]) * q + c[3]) * q + c[4]) * q + c[5]) / \
                ((((d[0] * q + d[1]) * q + d[2]) * q + d[3]) * q + 1)
    q = p - 0.5
    r = q * q
    return (((((a[0] * r + a[1]) * r + a[2]) * r + a[3]) * r + a[4]) * r + a[5]) * q / \
           (((((b[0] * r + b[1]) * r + b[2]) * r + b[3]) * r + b[4]) * r + 1)


def probabilistic_sharpe_ratio(sharpe: float, *, benchmark: float = 0.0,
                                n_obs: int, skew: float = 0.0,
                                kurtosis: float = 3.0) -> float:
    """PSR(SR*): 观测 Sharpe 超过基准的概率 (偏度/峰度修正)。

    sharpe / benchmark : 同一频率 (都用年化或都用每期, 别混)
    n_obs              : 观测数 (bar 数)
    kurtosis           : 峰度 (**非** excess kurtosis; 正态 = 3)
    """
    if int(n_obs) < 2:
        raise ValueError("n_obs 必须 >= 2")
    denom = 1.0 - float(skew) * float(sharpe) + \
        (float(kurtosis) - 1.0) / 4.0 * float(sharpe) ** 2
    if denom <= 0:
        raise ValueError(f"PSR 分母非正 ({denom}) —— 检查偏度/峰度输入")
    return _norm_cdf((float(sharpe) - float(benchmark)) *
                     math.sqrt(int(n_obs) - 1) / math.sqrt(denom))


def expected_max_sr(n_trials: int, *, n_obs: int,
                    var_sr: float | None = None) -> float:
    """N 次独立试验下期望出现的最大 Sharpe (DSR 的抬高阈值)。

    var_sr: SR 估计量的方差, 默认正态近似 1/(T-1)。
    """
    n = int(n_trials)
    if n < 1:
        raise ValueError("n_trials 必须 >= 1")
    if var_sr is None:
        var_sr = 1.0 / (int(n_obs) - 1)
    if n == 1:
        return 0.0
    g = EULER_MASCHERONI
    e = math.e
    thr = ((1.0 - g) * _norm_ppf(1.0 - 1.0 / n)
           + g * _norm_ppf(1.0 - 1.0 / (n * e)))
    return math.sqrt(var_sr) * thr


def deflated_sharpe_ratio(sharpe: float, *, n_trials: int, n_obs: int,
                          skew: float = 0.0, kurtosis: float = 3.0,
                          benchmark: float = 0.0) -> dict:
    """DSR: 以 E[max SR_N] 为基准的 PSR。返回 dsr + 阈值等诊断量。"""
    sr_star = expected_max_sr(int(n_trials), n_obs=int(n_obs))
    dsr = probabilistic_sharpe_ratio(float(sharpe), benchmark=sr_star,
                                     n_obs=int(n_obs), skew=skew,
                                     kurtosis=kurtosis)
    return {"dsr": float(dsr), "sr_threshold": float(sr_star),
            "n_trials": int(n_trials), "sharpe": float(sharpe),
            "benchmark_sharpe": float(benchmark)}


def significance_metrics(period_returns, *, n_trials: int,
                         sharpe: float | None = None,
                         periods_per_year: float | None = None,
                         benchmark: float = 0.0) -> dict:
    """从逐期收益序列算 PSR/DSR + 偏度/峰度。

    sharpe / periods_per_year 都给 None 时用逐期 Sharpe; 否则把逐期 Sharpe 年化,
    与回测 Portfolio 的 Sharpe 定义保持一致 (cagr/vol 口径另见 metrics.py)。
    """
    r = np.asarray(period_returns, dtype=float)
    r = r[np.isfinite(r)]
    if r.size < 3:
        raise ValueError(f"收益序列太短 ({r.size}) —— 显著性检验至少要 3 期")
    sr_period = float(r.mean() / r.std(ddof=1)) if r.std(ddof=1) > 0 else 0.0
    n = int(r.size)
    skew = float(pd_skew(r))
    kurt = float(pd_kurtosis(r)) + 3.0        # 转成非 excess 峰度
    if sharpe is not None and periods_per_year:
        sr = float(sharpe)
    else:
        sr = sr_period * math.sqrt(periods_per_year) if periods_per_year \
            else sr_period
    psr = probabilistic_sharpe_ratio(sr, benchmark=benchmark, n_obs=n,
                                     skew=skew, kurtosis=kurt)
    d = deflated_sharpe_ratio(sr, n_trials=int(n_trials), n_obs=n,
                              skew=skew, kurtosis=kurt, benchmark=benchmark)
    return {"psr": float(psr), "dsr": d["dsr"],
            "sr_threshold": d["sr_threshold"], "n_trials": int(n_trials),
            "n_obs": n, "skew": skew, "kurtosis": kurt,
            "sharpe_used": sr}


# -- numpy/pandas 无 scipy 时的偏度峰度 (小数组直接算, 保持依赖最小) ----------
def pd_skew(x: np.ndarray) -> float:
    m = x.mean()
    s = x.std()
    return float(np.mean(((x - m) / s) ** 3)) if s > 0 else 0.0


def pd_kurtosis(x: np.ndarray) -> float:
    m = x.mean()
    s = x.std()
    return float(np.mean(((x - m) / s) ** 4)) - 3.0 if s > 0 else 0.0


def go_no_go(metrics: dict, *, min_psr: float = 0.95, min_dsr: float = 0.95,
             require_positive_return: bool = True) -> dict:
    """OOS 人审闸 (决策 E5)。返回 (passed, 明细)。"""
    sig = metrics.get("significance") or {}
    trd = metrics.get("trading") or {}
    checks = {
        "psr": float(sig.get("psr", float("nan"))) >= min_psr,
        "dsr": float(sig.get("dsr", float("nan"))) >= min_dsr,
    }
    if require_positive_return:
        ann = trd.get("ann_return_net", trd.get("cagr", float("nan")))
        checks["positive_net_return"] = bool(np.isfinite(ann) and ann > 0)
    return {"passed": bool(all(checks.values())), "checks": checks,
            "min_psr": min_psr, "min_dsr": min_dsr}


if __name__ == "__main__":  # pragma: no cover
    # 自检1: 标准正态分位数 (对照标准值)
    assert abs(_norm_ppf(0.975) - 1.959964) < 1e-5, "norm_ppf 不准"
    print("[自检] norm_ppf(0.975) =", round(_norm_ppf(0.975), 6))
    # 自检2: 固定每期 Sharpe, N 增大 -> 阈值升 -> DSR 降 (多重检验修正的意义)
    n = 2000
    for sr in (0.06, 0.10):
        row = []
        for nt in (1, 100, 10000):
            thr = expected_max_sr(nt, n_obs=n)
            dsr = probabilistic_sharpe_ratio(sr, benchmark=thr, n_obs=n)
            row.append((nt, thr, dsr))
        print(f"[N扫描] sharpe={sr}: " + "  ".join(
            "N=%d thr=%.4f dsr=%.4f" % v for v in row))
        assert row[0][2] >= row[1][2] >= row[2][2], "DSR 必须随 N 不增"
    print("[自检] DSR 随试验次数 N 收紧 -> 多重检验修正生效")