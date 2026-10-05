# -*- coding: utf-8 -*-
"""operators.py — F1 算子库: 时序 ts_* / 截面 cs_* / 分组 group_* / 预处理 pp_*

设计文档: docs/feature-foundation-design.md 第 3 节 (v0.4)

面板约定
--------
所有算子输入/输出都是 **pandas.Series**, 索引为面板 MultiIndex:
行 = (instrument, time)。算子只做数值变换, 不碰 data_available_at —— 那由
F2 的 PIT 引擎负责 (特征可用时间 = 输入可用时间的最大值)。

* 时序算子 ts_*  : 沿 instrument 内的时间轴 (trailing window, 允许含 t)
* 截面算子 cs_*  : 同一 time 层内跨 instrument
* 分组算子 group_*: 同一 (time, group) 内跨 instrument
* 预处理 pp_*    : 点态变换, 或需要参照分布时必须显式给出 PIT 安全的估计范围

PIT 三条铁律 (违反即为泄漏)
--------------------------
1. **只用当前及过去**: 所有时序窗口 trailing, 值 x[t] 允许参与 t 时刻的特征
   (它自己带 data_available_at, 引擎据此判定何时可用)。禁止居中窗口
   (center=True)、禁止负向 shift (shift(-k))、禁止 bfill/反向填充。
2. **参照分布必须 PIT 安全**: 凡是需要估计分布的算子 (分位/zscore/minmax/
   标准化/detrend), 分布只能来自 [同一时刻的截面] 或 [trailing 窗口]。
   全样本/整段历史估计一律拒绝 —— 那等于用了未来。
3. **不得跨 instrument 串时间**: 时序算子必须按 instrument 分组, 否则 A 的
   收盘价会串进 B 的滚动均值。

未来不变性 (future-invariance)
------------------------------
本模块所有算子满足: 把 t 之后的数据换成任意噪声, t 时刻的输出不变。
该性质由 _test_operators.py 逐算子自动验证 —— 这是比"代码审查看有没有
center=True"强得多的检验: 任何未来信息都会让它失败。

运行: python -m data_foundation.features.operators
"""
from __future__ import annotations

import inspect
import re
from dataclasses import dataclass, field
from typing import Any, Callable

import numpy as np
import pandas as pd

# ---------------------------------------------------------------------------
# 常量
# ---------------------------------------------------------------------------
INSTRUMENT_LEVEL = "instrument"
TIME_LEVEL = "time"

_TIME_ALIASES = (TIME_LEVEL, "date_utc", "open_time_utc", "timestamp", "date")
_INST_ALIASES = (INSTRUMENT_LEVEL, "symbol", "base_asset", "inst")

#: 静态审计的禁止模式 (见 audit_pit_source)
FORBIDDEN_PATTERNS = (
    (r"center\s*=\s*True", "居中窗口"),
    (r"shift\(\s*-", "负向 shift (=用未来)"),
    (r"\.bfill\(", "反向填充 (=用未来)"),
    (r"method\s*=\s*['\"]bfill['\"]", "反向填充 (=用未来)"),
    (r"interpolate\(", "插值 (需显式 limit_direction=forward)"),
)

MIN_WINDOW = 1


# ---------------------------------------------------------------------------
# 内部工具
# ---------------------------------------------------------------------------
def _as_series(x: Any, arg: str = "x") -> pd.Series:
    """严格入参检查: 算子只吃 Series (引擎负责选列, 不给 DataFrame 制造歧义)。"""
    if isinstance(x, pd.DataFrame):
        raise TypeError(f"{arg} 必须是 Series, 收到 DataFrame(形状 {x.shape}); "
                        f"请显式选列, 例如 x['close']")
    if not isinstance(x, pd.Series):
        raise TypeError(f"{arg} 必须是 pandas.Series, 收到 {type(x).__name__}")
    _check_index(x)
    return x


def _pair(x: Any, y: Any) -> tuple[pd.Series, pd.Series]:
    a, b = _as_series(x, "x"), _as_series(y, "y")
    if len(a) != len(b):
        raise ValueError(f"x/y 长度不一致: {len(a)} vs {len(b)}")
    if not a.index.equals(b.index):
        raise ValueError("x/y 索引不一致 (算子要求同一面板)")
    return a, b


def _win(window: Any, name: str = "window") -> int:
    if isinstance(window, bool) or not isinstance(window, (int, np.integer)):
        raise TypeError(f"{name} 必须是正整数, 收到 {window!r}")
    w = int(window)
    if w < MIN_WINDOW:
        raise ValueError(f"{name} 必须 >= {MIN_WINDOW}, 收到 {w}")
    return w


def _mp(window: int, min_periods: Any) -> int:
    if min_periods is None:
        return 1
    mp = int(min_periods)
    if mp < 1:
        raise ValueError(f"min_periods 必须 >= 1, 收到 {mp}")
    if mp > window:
        raise ValueError(f"min_periods({mp}) 不能大于 window({window})")
    return mp


def _lag(lag: Any, name: str = "lag") -> int:
    k = int(lag)
    if k < 0:
        raise ValueError(f"{name} 必须 >= 0 —— 负数是向后取, 那是未来 (PIT 铁律 1)")
    return k


def _level_of(x: pd.Series, aliases: tuple[str, ...]) -> str | None:
    names = list(x.index.names)
    for a in aliases:
        if a in names:
            return a
    return None


def _inst_level(x: pd.Series) -> str | None:
    return _level_of(x, _INST_ALIASES)


def _time_level(x: pd.Series) -> str:
    lv = _level_of(x, _TIME_ALIASES)
    if lv is not None:
        return lv
    if isinstance(x.index, pd.MultiIndex):
        return x.index.names[-1]
    raise ValueError(f"找不到时间层: index.names={list(x.index.names)}, "
                     f"应为 {list(_TIME_ALIASES)} 之一")


def _check_index(x: pd.Series) -> None:
    """面板索引唯一性 —— rolling 结果要按原索引对齐, 重复索引无法对齐。"""
    if isinstance(x.index, pd.MultiIndex) and not x.index.is_unique:
        dup = x.index[x.index.duplicated()].tolist()[:3]
        raise ValueError(f"面板索引必须唯一 (行=(instrument, time)); 重复行例: {dup}")


def _inst_codes(x: pd.Series) -> np.ndarray | None:
    """instrument 层 -> 整数 codes (None = 无 instrument 层, 视为单序列)。"""
    lv = _inst_level(x)
    if lv is None:
        return None
    return pd.factorize(x.index.get_level_values(lv), sort=False)[0]


def _time_codes(x: pd.Series) -> np.ndarray:
    return pd.factorize(x.index.get_level_values(_time_level(x)), sort=False)[0]


def _require_causal_order(x: pd.Series, codes: np.ndarray) -> None:
    """同一 instrument 内的时间必须递增 —— 否则滚动窗口会把未来排到前面。

    注意: 这里用 ``.asi8`` (int64 视图, 零拷贝) 而不是 np.asarray —— tz-aware
    的 DatetimeIndex 转 numpy 会走慢路径 (实测 200 万行 9 秒, 直接吃掉所有
    ts_ 算子的 90% 耗时)。
    """
    lv = _time_level(x)
    t = x.index.get_level_values(lv)
    if len(t) < 2:
        return
    same = codes[1:] == codes[:-1]
    if not same.any():
        return
    # datetime64 直接比较; 其他类型退化为不检查
    try:
        tv = t.asi8
    except AttributeError:                       # 非 datetime 索引
        try:
            tv = np.asarray(t)
        except TypeError:
            return
    if np.any((tv[1:] < tv[:-1]) & same):
        raise ValueError(
            "同一 instrument 内时间必须递增 (面板须按 (instrument, time) 或 "
            "(time, instrument) 排序); 检测到乱序 —— 继续算会产生未来数据")


def _roll(x: pd.Series, window: int, min_periods: int):
    """返回 (rolling 对象, 是否分组)。分组时结果会多一层 level, 由 _finish 剥掉。"""
    codes = _inst_codes(x)
    if codes is None:
        return x.rolling(window=window, min_periods=min_periods), False
    _require_causal_order(x, codes)
    return x.groupby(codes, sort=False).rolling(window=window, min_periods=min_periods), True


def _finish(res, x: pd.Series, grouped: bool) -> pd.Series:
    """把 rolling 结果对齐回原索引 (分组 rolling 会按组重排 + 多一层 level)。"""
    if grouped:
        res = res.droplevel(0)
        if not res.index.equals(x.index):
            res = res.reindex(x.index)
    return res


def _roll_op(x: pd.Series, window: int, min_periods: int, method: str, **kw) -> pd.Series:
    r, g = _roll(x, window, min_periods)
    return _finish(getattr(r, method)(**kw), x, g)


def _roll_apply(x: pd.Series, window: int, min_periods: int, func) -> pd.Series:
    """逐窗 Python 回调 (慢, 只在没有向量化等价写法时用, 如窗口内拟合 λ)。"""
    r, g = _roll(x, window, min_periods)
    return _finish(r.apply(func, raw=True), x, g)


def _shift(x: pd.Series, k: int) -> pd.Series:
    """按 instrument 内的滞后位移 (k>=0, 由 _lag 保证)。"""
    if k == 0:
        return x.copy()
    codes = _inst_codes(x)
    if codes is None:
        return x.shift(k)
    _require_causal_order(x, codes)
    return x.groupby(codes, sort=False).shift(k)


def _weighted_trail_sum(x: pd.Series, window: int, weights: np.ndarray) -> pd.Series:
    """y[t] = Σ_j weights[j] * x[t-j] (weights[0] 配最新值)。固定窗, 缺一即 NaN。

    用 w 次 shift 累加而不是 rolling.apply: 全 C 路径, 无逐窗 Python 回调。
    """
    w = int(window)
    if len(weights) != w:
        raise ValueError(f"weights 长度 {len(weights)} != window {w}")
    out = x * float(weights[0])
    for j in range(1, w):
        out = out + _shift(x, j) * float(weights[j])
    return out.rename(x.name)


def _tg_codes(x: pd.Series, labels: np.ndarray) -> np.ndarray:
    """(time, group) 双键 -> 单个整数 codes (算术混合, 比 MultiIndex 快)。"""
    t = _time_codes(x).astype(np.int64)
    g = pd.factorize(labels, sort=False)[0].astype(np.int64)
    n = int(g.max()) + 1 if len(g) else 1
    key = t * n + g
    return pd.factorize(key, sort=False)[0]


def _resolve_group(x: pd.Series, g: Any) -> np.ndarray:
    """把分组标签解析成与 x.index 对齐的 1 维数组。

    支持:
      * ndarray / list  —— 长度须等于 len(x)
      * Series 与 x.index 完全对齐 —— 时变标签 (板块重分类/市值迁移)
      * Series 以 instrument 为索引 —— 常量标签, 按 instrument 广播
      * str —— x 是 DataFrame 时取列 (算子本身不吃 DataFrame, 故不支持, 直接报错)
    """
    if isinstance(g, str):
        raise TypeError(f"分组标签 g 不能是字符串 {g!r}; 引擎应把 'sector' 解析成 "
                        f"与面板对齐的 Series 后再传入 (见 F4 分组维度)")
    if isinstance(g, pd.Series):
        if g.index.equals(x.index):
            return g.to_numpy()
        lv = _inst_level(x)
        if lv is not None and g.index.is_unique and set(g.index) <= set(x.index.get_level_values(lv)):
            # 按 instrument 广播
            pos = x.index.get_level_values(lv)
            return g.reindex(pos).to_numpy()
        raise ValueError("分组标签 Series 索引既不等于面板索引, 也不是面板 instrument 层的子集")
    arr = np.asarray(g)
    if arr.ndim != 1 or len(arr) != len(x):
        raise ValueError(f"分组标签长度/维度不符: 得到 {arr.shape}, 需要 ({len(x)},)")
    return arr


# ===========================================================================
# ts_* —— 时序算子 (单 instrument 内, 沿时间轴)
# ===========================================================================
def ts_delay(x, lag: int = 1, name: str | None = None) -> pd.Series:
    """滞后 lag 期。lag=0 原样返回。显式取过去值 (PIT 铁律 1)。"""
    x = _as_series(x)
    k = _lag(lag)
    return _shift(x, k).rename(name if name is not None else x.name)


def ts_backfill(x, limit: int | None = None, name: str | None = None) -> pd.Series:
    """按 instrument 内**向前**填充缺失 (ffill): 用最近的历史值补当前 NaN。

    与 bfill 的区别: ffill 只取过去 —— PIT 安全; 这是清洗动作, 缺失模式本身是
    信息 (见 pp_is_missing)。
    """
    x = _as_series(x)
    codes = _inst_codes(x)
    if codes is None:
        out = x.ffill(limit=limit)
    else:
        _require_causal_order(x, codes)
        out = x.groupby(codes, sort=False).ffill(limit=limit)
    return out.rename(name if name is not None else x.name)


def ts_mean(x, window: int = 20, min_periods: int | None = None, name=None) -> pd.Series:
    """窗口均值。默认 min_periods=1 (窗口未满时用已有值平均)。"""
    x = _as_series(x)
    w = _win(window)
    return _roll_op(x, w, _mp(w, min_periods), "mean").rename(name or x.name)


def ts_std(x, window: int = 20, min_periods: int | None = None, ddof: int = 1,
           name=None) -> pd.Series:
    """窗口标准差。ddof=1 样本标准差 (pandas 默认)。"""
    x = _as_series(x)
    w = _win(window)
    return _roll_op(x, w, _mp(w, min_periods), "std", ddof=ddof).rename(name or x.name)


def ts_sum(x, window: int = 20, min_periods: int | None = None, name=None) -> pd.Series:
    """窗口求和。"""
    x = _as_series(x)
    w = _win(window)
    return _roll_op(x, w, _mp(w, min_periods), "sum").rename(name or x.name)


def ts_min(x, window: int = 20, min_periods: int | None = None, name=None) -> pd.Series:
    """窗口最小值。"""
    x = _as_series(x)
    w = _win(window)
    return _roll_op(x, w, _mp(w, min_periods), "min").rename(name or x.name)


def ts_max(x, window: int = 20, min_periods: int | None = None, name=None) -> pd.Series:
    """窗口最大值。"""
    x = _as_series(x)
    w = _win(window)
    return _roll_op(x, w, _mp(w, min_periods), "max").rename(name or x.name)


def ts_median(x, window: int = 20, min_periods: int | None = None, name=None) -> pd.Series:
    """窗口中位数 (抗离群)。"""
    x = _as_series(x)
    w = _win(window)
    return _roll_op(x, w, _mp(w, min_periods), "median").rename(name or x.name)


def ts_quantile(x, window: int = 20, q: float = 0.5, min_periods: int | None = None,
                name=None) -> pd.Series:
    """窗口分位数 (内部算子 + 公开给特征库; q ∈ [0,1])。"""
    x = _as_series(x)
    w = _win(window)
    q = float(q)
    if not 0.0 <= q <= 1.0:
        raise ValueError(f"q 必须 ∈ [0,1], 收到 {q}")
    return _roll_op(x, w, _mp(w, min_periods), "quantile", q=q).rename(name or x.name)


def ts_delta(x, lag: int = 1, name=None) -> pd.Series:
    """差分 x[t] - x[t-lag]。"""
    x = _as_series(x)
    return (x - _shift(x, _lag(lag))).rename(name or x.name)


def ts_pct_change(x, lag: int = 1, name=None) -> pd.Series:
    """变化率 x[t]/x[t-lag] - 1。分母为 0 或非正时结果置 NaN (不产生 inf)。"""
    x = _as_series(x)
    prev = _shift(x, _lag(lag))
    out = x / prev - 1.0
    out = out.where(np.isfinite(out))
    return out.rename(name or x.name)


def ts_rank(x, window: int = 20, min_periods: int | None = None, name=None) -> pd.Series:
    """当前值在过去 window 期内的百分位排名, 与 pandas rank(pct=True) 同口径 (0,1]。

    并列取平均秩; 窗口内 NaN 不参与计数; 当前值为 NaN 则输出 NaN。

    实现: rank = Σ_k [x_t > x_{t-k}] + 0.5·Σ_k [x_t == x_{t-k}] + 1 (自身算并列
    成员), 除以窗口有效值个数。用 w-1 次 shift 比较代替 rolling.apply 的逐窗
    Python 回调 (慢 1-2 个数量级), 且每次比较都是窗口封闭的纯函数 —— 未来
    不变性保持。
    """
    x = _as_series(x)
    w = _win(window)
    mp = _mp(w, min_periods)
    vals = x.to_numpy(dtype=float)
    n_valid = _roll_op(x, w, mp, "count").to_numpy(dtype=float)
    with np.errstate(invalid="ignore"):
        acc = np.zeros(len(x), dtype=float)
        for k in range(1, w):
            prev = _shift(x, k).to_numpy(dtype=float)
            acc += (vals > prev) + 0.5 * (vals == prev)
    pct = pd.Series(np.nan, index=x.index, dtype=float)
    ok = n_valid > 0
    pct[ok] = (acc[ok] + 1.0) / n_valid[ok]
    return pct.where(~np.isnan(vals)).rename(name or x.name)


def ts_zscore(x, window: int = 20, min_periods: int | None = None, ddof: int = 0,
              clip: float | None = None, name=None) -> pd.Series:
    """滚动 z = (x - ts_mean) / ts_std。默认 ddof=0 (小窗口友好); std=0 -> NaN。"""
    x = _as_series(x)
    w = _win(window)
    mp = _mp(w, min_periods)
    mu = _roll_op(x, w, mp, "mean")
    sd = _roll_op(x, w, mp, "std", ddof=ddof)
    out = (x - mu) / sd.where(sd > 0)
    if clip is not None:
        out = out.clip(-abs(clip), abs(clip))
    return out.rename(name or x.name)


def ts_skew(x, window: int = 20, min_periods: int | None = None, name=None) -> pd.Series:
    """滚动偏度 (调整 Fisher-Pearson G1, 与 pandas/scipy(bias=False) 同口径;
    至少 3 个有效值)。

    **不用 pandas 自带的 rolling.skew**: 那是跨窗累积算法, 窗口外的值会通过
    累积状态影响历史输出 (实测把未来数据换成噪声, 历史偏度漂移 ~1e-7),
    不满足未来不变性。这里用窗口和展开 (见 _roll_central_sums),
    每行输出严格只是其窗口内值的函数。
    """
    x = _as_series(x)
    w = _win(window)
    mp = max(_mp(w, min_periods), 3)
    n, s2, s3, _ = _roll_central_sums(x, w, mp)
    m2 = s2 / n
    m3 = s3 / n
    with np.errstate(invalid="ignore", divide="ignore"):
        g1 = m3 / m2 ** 1.5
        out = g1 * np.sqrt(n * (n - 1.0)) / (n - 2.0)
    return out.where((n >= 3) & (m2 > 0)).rename(name or x.name)


def ts_kurt(x, window: int = 20, min_periods: int | None = None, name=None) -> pd.Series:
    """滚动超额峰度 (无偏 G2, Fisher 口径与 pandas/scipy(bias=False) 一致;
    至少 4 个有效值)。实现 同 ts_skew (窗口和展开, 窗口封闭),
    不用 pandas 自带的跨窗累积版本。
    """
    x = _as_series(x)
    w = _win(window)
    mp = max(_mp(w, min_periods), 4)
    n, s2, s3, s4 = _roll_central_sums(x, w, mp)
    m2 = s2 / n
    m4 = s4 / n
    with np.errstate(invalid="ignore", divide="ignore"):
        g2 = m4 / m2 ** 2 - 3.0
        out = ((n + 1.0) * g2 + 6.0) * (n - 1.0) / ((n - 2.0) * (n - 3.0))
    return out.where((n >= 4) & (m2 > 0)).rename(name or x.name)


def _roll_central_sums(x: pd.Series, w: int, mp: int):
    """返回 (n, C2, C3, C4): 窗口内中心矩所需的和 Σ(x-μ)^k (k=2,3,4) 与有效值个数。

    用窗口和展开 (Σx, Σx², Σx³, Σx⁴ 与 μ=Σx/n 全部是同一窗口内值的函数,
    每行输出因此严格窗口封闭 —— 未来不变性成立)。不用 pandas 自带的
    rolling.skew/kurt: 那是跨窗累积算法, 窗口外的值会通过累积状态影响历史
    输出 (实测把未来数据换成噪声, 历史输出漂移 ~1e-5)。

    数值注意: 展开式对 |μ|/σ >> 1 的序列有消减误差 (rel ~ eps*(μ/σ)³),
    量纲极端的序列请先用 pp_sqrt/pp_log/pp_zscore 压缩量纲再算高阶矩。
    """
    n = _roll_op(x, w, mp, "count").astype(float)
    with np.errstate(over="ignore", invalid="ignore"):
        s1 = _roll_op(x, w, mp, "sum")
        s2 = _roll_op(x * x, w, mp, "sum")
        s3 = _roll_op(x * x * x, w, mp, "sum")
        s4 = _roll_op(x * x * x * x, w, mp, "sum")
    mu = s1 / n
    mu2 = mu * mu
    c2 = s2 - n * mu2
    c3 = s3 - 3.0 * mu * s2 + 3.0 * mu2 * s1 - n * mu * mu2
    c4 = s4 - 4.0 * mu * s3 + 6.0 * mu2 * s2 - 4.0 * mu * mu2 * s1 + n * mu2 * mu2
    return n, c2, c3, c4


def ts_ewma(x, span: int | None = 20, halflife: float | None = None,
            alpha: float | None = None, adjust: bool = False, min_periods: int = 1,
            name=None) -> pd.Series:
    """指数加权移动平均 (按 instrument 内)。

    span / halflife / alpha 三者只能给一个: 用 halflife 或 alpha 时必须显式
    传 ``span=None``。默认 adjust=False (递归式, 首值即种子, 不回看窗口外
    历史 —— 与 adjust=True 一样 PIT 安全, 但递归式快很多)。
    """
    x = _as_series(x)
    given = [span is not None, halflife is not None, alpha is not None]
    if sum(given) > 1:
        raise ValueError("span / halflife / alpha 只能给一个 (用 halflife/alpha 时"
                         "须显式传 span=None)")
    if not any(given):
        raise ValueError("span / halflife / alpha 至少给一个")
    if span is not None:
        span = _win(span, "span")
        alpha = 2.0 / (span + 1.0)
        span = None
    # pandas 不允许 span/halflife/alpha 同时出现 —— 显式置空其余项
    ewm_kw = dict(span=span, halflife=halflife, alpha=alpha, adjust=adjust,
                  min_periods=min_periods)
    codes = _inst_codes(x)
    if codes is None:
        out = x.ewm(**ewm_kw).mean()
    else:
        _require_causal_order(x, codes)
        out = x.groupby(codes, sort=False).ewm(**ewm_kw).mean()
        out = out.droplevel(0)
        if not out.index.equals(x.index):
            out = out.reindex(x.index)
    return out.rename(name or x.name)


def ts_decay_linear(x, window: int = 20, reverse: bool = False, normalize: bool = True,
                    name=None) -> pd.Series:
    """线性衰减加权平均 (FIR)。

    权重: 默认 weights = d..1, **最新值权重最大** (线性"衰减"向当前);
          reverse=True 时 weights = 1..d, **最旧值权重最大** —— 这是
          WorldQuant / Qlib 的 ts_decay_linear 口径, 两种都保留, 不替使用者做主。
    normalize=True 权重归一化成均值; False 直接加权求和。
    要求完整窗口 (min_periods=window): 部分窗口下权重结构会被截断, 不可比。
    """
    x = _as_series(x)
    w = _win(window)
    # weights[j] 配 x[t-j]: 默认最新值 (j=0) 权重最大; reverse=True 反过来
    weights = (np.arange(1, w + 1, dtype=float) if reverse
               else np.arange(w, 0, -1, dtype=float))
    if normalize:
        weights = weights / weights.sum()
    return _weighted_trail_sum(x, w, weights).rename(name or x.name)


def ts_corr(x, y, window: int = 20, min_periods: int | None = None, name=None) -> pd.Series:
    """滚动相关系数 (pandas 内部用 Welford 式更新, 数值稳定, 不用 E[xy]-E[x]E[y] 那种
    对价格量级会灾难性消减的写法)。"""
    x, y = _pair(x, y)
    w = _win(window)
    mp = _mp(w, min_periods)
    if mp < 2:
        mp = 2
    codes = _inst_codes(x)
    df = pd.DataFrame({"x": x.to_numpy(), "y": y.to_numpy()})
    if codes is None:
        res = df.rolling(window=w, min_periods=mp).corr()
    else:
        _require_causal_order(x, codes)
        res = df.groupby(codes, sort=False).rolling(window=w, min_periods=mp).corr()
    # res 可能是: 平面行索引 (行=行号, 列=MultiIndex(x,y)) 或 MultiIndex 行
    # (分组键, 行号, 列名)。取 "y" 那一行 (与 y 的相关系数向量) 的 "x" 列
    # -> 每行一个 corr(x, y)。
    if isinstance(res.index, pd.MultiIndex):
        sel = res.xs("y", axis=0, level=-1)
        ser = sel["x"]
        if ser.index.nlevels > 1:
            ser = ser.droplevel(0)
    else:
        ser = res["y"] if res.columns[0] == "y" else res["x"]
    vals = ser.reindex(np.arange(len(x))).to_numpy()
    out = pd.Series(np.asarray(vals, dtype=float), index=x.index, name=name or x.name)
    return out.where(np.isfinite(out))


# ===========================================================================
# cs_* —— 截面算子 (同一 time 内跨 instrument)
# ===========================================================================
def _cs_groupby(x: pd.Series):
    return x.groupby(_time_codes(x), sort=False)


def cs_rel(x, ref, name=None) -> pd.Series:
    """相对基准的收益: x[t] / ref[t] - 1, 按 **time 对齐** (加密最常用的截面特征:
    相对 BTC / 相对板块基准的强弱)。

    ref 需是同一面板里的另一列 (通常是基准资产的 close); 若某时刻 ref 有多个值,
    取该时刻的首个 (基准侧本就只有一条)。ref 缺失 -> NaN (不做任何填充)。

    PIT: 只用同一时刻的值 (不含未来); 可用时间 = 两个输入可用时间的同行最大值。
    """
    x = _as_series(x, "x")
    ref = _pair(x, ref)[1]
    ref_by_time = ref.groupby(_time_codes(ref), sort=False).first()
    vals = ref_by_time.reindex(_time_codes(x))
    out = x / pd.Series(vals.to_numpy(), index=x.index) - 1.0
    out = pd.Series(np.asarray(out, dtype=float), index=x.index)
    return out.where(np.isfinite(out)).rename(name or x.name)


# ===========================================================================
# 无状态预处理算子 (2026-10-05 补)
# 设计原则: 参数只来自**当期截面**或**trailing 窗口** —— 不需要从训练集里
# "记住"任何数字, 因此服务端拿新数据能原样重算 (无状态)。需要拟合参数的
# 变换 (全局标准化器/PCA) 不属于这里, 属模型研究协议, 必须随模型携带。
# ===========================================================================
def cs_residual(x, ref, name=None) -> pd.Series:
    """截面中性化: 对暴露量做逐期 OLS, 返回**残差**。

        x_i = a_t + b_t * ref_i + e_i   ->   e_i

    这是量化里最标准的预处理之一 (把信号里能被规模/波动解释的部分剔掉)。
    逐期独立拟合 => 无状态: 参数 (a_t, b_t) 只依赖当期截面, 不跨期记忆。

    退化保护: 当期 ref 方差为 0 (全同值) 时退化为**去均值** (等价于只拟合截距),
    而不是产出 inf/NaN —— 这种情况在资产很少或标签集中时真的会出现。
    """
    x = _as_series(x, "x")
    ref = _pair(x, ref)[1]
    g = _cs_groupby(x)
    mx = g.transform("mean")
    my = _cs_groupby(ref).transform("mean")
    xc = x - mx
    yc = ref - my
    sxx = _cs_groupby(yc).transform(lambda s: (s * s).sum())
    sxy = _cs_groupby(xc * yc).transform("sum")
    beta = (sxy / sxx.where(sxx > 0))
    resid = xc - beta.fillna(0.0) * yc
    return resid.where(np.isfinite(np.asarray(resid, dtype=float))).rename(
        name or x.name)


def cs_rank_normal(x, name=None) -> pd.Series:
    """截面排名的**高斯化** (inverse-normal transform / Gaussianize)。

    先把截面排名归一化到 (0,1), 再过标准正态分位函数:
        z_i = Phi^{-1}( rank_i / (n+1) )

    为什么量化常用它: 排名天然抗离群且单调, 但均匀分布对线性模型不友好;
    高斯化后既保留排名的稳健性, 又给出近似正态的输入 —— 这是"rank 口径"
    与"z-score 口径"之间的第三种选择, 由检验裁决谁有效 (决策 8)。
    逐期计算 => 无状态。
    """
    x = _as_series(x)
    g = _cs_groupby(x)
    r = g.rank(pct=False)                     # 1..n 的秩
    n = g.transform("size").astype(float)
    p = (r / (n + 1.0)).clip(1e-6, 1 - 1e-6)  # 开区间, 避免 Phi^{-1}(0/1) = ±inf
    try:
        from scipy.special import ndtri     # 标准正态分位函数
    except ImportError as exc:               # pragma: no cover
        raise ImportError("cs_rank_normal 需要 scipy (pip install scipy)") from exc
    arr = ndtri(np.asarray(p, dtype=float))
    out = pd.Series(arr, index=x.index)
    return out.where(np.isfinite(arr)).rename(name or x.name)


def cs_winsorize_mad(x, n_mad: float = 5.0, name=None) -> pd.Series:
    """截面 MAD 去极值: 截断到 中位数 ± n_mad * (1.4826 * MAD)。

    与 cs_winsorize (均值 ± n_std) 的区别: 均值与标准差本身会被离群值带偏,
    MAD 不会 —— 加密数据插针多、分布肥尾, 稳健口径往往更合用。
    MAD = median(|x - median(x)|) 是"正态下的标准差一致性估计" (×1.4826)。
    逐期计算 => 无状态。
    """
    x = _as_series(x)
    n_mad = abs(float(n_mad))
    g = _cs_groupby(x)
    med = g.transform("median")
    mad = g.transform(lambda s: (s - s.median()).abs().median()) * 1.4826
    scale = mad.where(mad > 0)
    lo = med - n_mad * scale
    hi = med + n_mad * scale
    return x.clip(lo, hi).rename(name or x.name)


def pp_soft_threshold(x, k: float = 1.0, name=None) -> pd.Series:
    """软阈值 (去噪/稀疏化): sign(x) * max(|x| - k, 0)。

    小于 k 的信号被压到 0, 大于 k 的被整体收缩 —— 小信号多为噪声, 直接置零
    比留着更稳 (稀疏编码/小波去噪里的标准算子)。k 是**显式给定**的阈值,
    不自动拟合 (拟合阈值 = 引入自由参数)。
    """
    x = _as_series(x)
    k = abs(float(k))
    out = np.sign(x) * (np.abs(x) - k).clip(lower=0.0)
    return pd.Series(np.asarray(out, dtype=float), index=x.index).rename(
        name or x.name)


def pp_savgol(x, window: int = 7, polyorder: int = 2, name=None) -> pd.Series:
    """Savitzky-Golay 平滑的**trailing 版** (只用过去 window 根 bar)。

    对每根 bar, 用其往前 window 个点拟合 polyorder 阶多项式, 取**最新点**的
    拟合值。设计文档 3.3 列了 pp_savgol, 但标准实现是**居中窗口** (用未来数据);
    这里改成只回看, 保住引擎的核心不变量 (未来不变性)。

    实现: SG 在端点处的权重是固定的 (A 的伪逆最后一行), 所以本质是一个 FIR
    滤波器 —— 用 w 次 shift 累加, 不走 rolling.apply 的逐窗回调。要求完整窗口。
    """
    x = _as_series(x)
    w = _win(window, "window")
    p = int(polyorder)
    if p < 0 or p >= w:
        raise ValueError(f"polyorder 必须 0 <= p < window, 收到 p={p}, w={w}")
    A = np.vander(np.arange(w, dtype=float), p + 1, increasing=True)
    coef = A[-1] @ np.linalg.pinv(A)          # 端点处的等权 FIR 系数
    weights = coef[::-1]                      # weights[k] 配滞后 k 根
    return _weighted_trail_sum(x, w, weights).rename(name or x.name)


def pp_boxcox(x, lmbda: float | None = None, by: str = "time",
              window: int | None = None, name=None) -> pd.Series:
    """Box-Cox 幂变换, **参数按 PIT 安全的范围确定** (无状态)。

        y = (x^l - 1) / l   (l != 0);  y = ln(x)  (l = 0)

    用途: 把右偏(肥尾)分布拉近正态, 是 pp_log 的推广 (对数只是 l→0 的特例)。
    lmbda 的三种给法:
      * 显式给 lmbda        -> 纯点态变换
      * by="time" (默认)    -> **逐期截面**用剖面似然拟合 l (当期可得, 无状态)
      * by="ts", window=W   -> trailing 窗口拟合 (更贵, 按 instrument 回看)
    **不允许**用整段历史拟合 (那是前视) —— 传其它 by 直接报错。

    要求 x > 0 (Box-Cox 的定义域); 非正值 -> NaN, 不静默平移。
    """
    x = _as_series(x)
    xpos = x.where(x > 0)
    if lmbda is not None:
        lam = float(lmbda)
        out = _boxcox_apply(xpos, lam)
        return out.rename(name or x.name)
    if by == "time":
        g = _cs_groupby(xpos)
        lam = g.transform(lambda s: _fit_boxcox_lambda(s))
        out = _boxcox_mixed(xpos, lam)
    elif by in ("ts", "trailing", "window"):
        w = _win(window, "window") if window is not None else None
        if w is None:
            raise ValueError("by='ts' 必须给 window")
        lam = _roll_apply(xpos, w, w, _fit_boxcox_lambda)
        out = _boxcox_mixed(xpos, lam)
    else:
        raise ValueError(
            f"by={by!r} 不被允许 —— 只接受 'time'(同刻截面) 或 'ts'(trailing "
            f"窗口); 用整段历史拟合 lambda 会引入未来信息 (PIT 铁律 2)")
    return out.where(np.isfinite(np.asarray(out, dtype=float))).rename(
        name or x.name)


def _boxcox_apply(x: pd.Series, lam: float) -> pd.Series:
    if abs(lam) < 1e-12:
        v = np.log(np.asarray(x, dtype=float))
    else:
        v = (np.power(np.asarray(x, dtype=float), lam) - 1.0) / lam
    return pd.Series(v, index=x.index)


def _boxcox_mixed(x: pd.Series, lam: pd.Series) -> pd.Series:
    """按行使用不同的 lambda (截面/窗口拟合出来的)。"""
    arr = np.asarray(x, dtype=float)
    l = np.asarray(lam, dtype=float)
    with np.errstate(divide="ignore", invalid="ignore", over="ignore"):
        v = np.where(np.abs(l) < 1e-12, np.log(arr),
                     (np.power(np.maximum(arr, 1e-300), l) - 1.0)
                     / np.where(np.abs(l) < 1e-12, 1.0, l))
    return pd.Series(v, index=x.index)


def _fit_boxcox_lambda(s: pd.Series) -> float:
    """剖面似然在固定网格上选 lambda (numpy 实现, 不依赖 scipy)。"""
    v = np.asarray(s, dtype=float)
    v = v[~np.isnan(v)]
    v = v[v > 0]
    n = v.size
    if n < 3:
        return 1.0
    grid = np.arange(-1.0, 2.01, 0.1)
    logv = np.log(v)
    best, best_ll = 1.0, -np.inf
    for lam in grid:
        if abs(lam) < 1e-12:
            z = logv
        else:
            z = (np.power(v, lam) - 1.0) / lam
        ll = (lam - 1.0) * logv.sum() - 0.5 * n * np.log(np.var(z) + 1e-300)
        if ll > best_ll:
            best_ll, best = ll, float(lam)
    return best


def cs_rank(x, name=None) -> pd.Series:
    """截面百分位排名 (同一时刻跨 instrument), 口径同 pandas rank(pct=True) (0,1]。"""
    x = _as_series(x)
    return _cs_groupby(x).rank(pct=True).rename(name or x.name)


def cs_zscore(x, ddof: int = 1, name=None) -> pd.Series:
    """截面 z = (x - 截面均值)/截面标准差。默认 ddof=1 (截面视作样本)。"""
    x = _as_series(x)
    g = _cs_groupby(x)
    mu = g.transform("mean")
    sd = g.transform("std") if ddof else g.transform(lambda s: s.std(ddof=0))
    sd = sd.where(sd > 0)
    return ((x - mu) / sd).rename(name or x.name)


def cs_winsorize(x, n_std: float = 4.0, ddof: int = 1, name=None) -> pd.Series:
    """截面去极值: 截断到 [截面均值 ± n_std*截面标准差]。"""
    x = _as_series(x)
    n_std = abs(float(n_std))
    g = _cs_groupby(x)
    mu = g.transform("mean")
    sd = (g.transform("std") if ddof else g.transform(lambda s: s.std(ddof=0)))
    sd = sd.where(sd > 0)
    return x.clip((mu - n_std * sd), (mu + n_std * sd)).rename(name or x.name)


def cs_normalize(x, method: str = "sum_abs", name=None) -> pd.Series:
    """截面归一化。method:
      * ``sum_abs``  : x / Σ|x| (WorldQuant normalize 口径, 权重和为 ±1)
      * ``minmax``   : (x - min)/(max - min) ∈ [0,1]
      * ``demean``   : x - 截面均值 (权重和为 0, 多空对称)
    """
    x = _as_series(x)
    g = _cs_groupby(x)
    if method == "sum_abs":
        den = g.transform(lambda s: s.abs().sum())
    elif method == "minmax":
        lo = g.transform("min")
        hi = g.transform("max")
        den = (hi - lo).where(hi > lo)
        return ((x - lo) / den).rename(name or x.name)
    elif method == "demean":
        return (x - g.transform("mean")).rename(name or x.name)
    else:
        raise ValueError(f"未知 method {method!r}; 可用: sum_abs / minmax / demean")
    return (x / den.where(den != 0)).rename(name or x.name)


def cs_scale(x, method: str = "mean", name=None) -> pd.Series:
    """截面缩放到可比量纲。method:
      * ``mean``  : x / 截面均值
      * ``std``   : x / 截面标准差
      * ``maxabs``: x / 截面最大绝对值 (权重落在 [-1,1])
    """
    x = _as_series(x)
    g = _cs_groupby(x)
    if method == "mean":
        den = g.transform("mean")
    elif method == "std":
        den = g.transform("std")
    elif method == "maxabs":
        den = g.transform(lambda s: s.abs().max())
    else:
        raise ValueError(f"未知 method {method!r}; 可用: mean / std / maxabs")
    return (x / den.where(den != 0)).rename(name or x.name)


# ===========================================================================
# group_* —— 分组算子 (同一 (time, group) 内跨 instrument)
# ===========================================================================
def _grouped(x: pd.Series, g: Any):
    codes = _tg_codes(x, _resolve_group(x, g))
    return x.groupby(codes, sort=False)


def group_rank(x, g, pct: bool = True, name=None) -> pd.Series:
    """组内百分位排名 (同一时刻、同一分组内跨 instrument)。"""
    x = _as_series(x)
    return _grouped(x, g).rank(pct=pct).rename(name or x.name)


def group_zscore(x, g, ddof: int = 1, name=None) -> pd.Series:
    """组内 z 分数。"""
    x = _as_series(x)
    gb = _grouped(x, g)
    mu = gb.transform("mean")
    sd = gb.transform("std") if ddof else gb.transform(lambda s: s.std(ddof=0))
    sd = sd.where(sd > 0)
    return ((x - mu) / sd).rename(name or x.name)


def group_neutralize(x, g, full: bool = True, name=None) -> pd.Series:
    """组内中性化: x - 组均值 (full=True 再减去残差的截面均值, 使各组均值之和为 0,
    即标准行业中性化的口径)。"""
    x = _as_series(x)
    gb = _grouped(x, g)
    resid = x - gb.transform("mean")
    if full:
        resid = resid - _cs_groupby(resid).transform("mean")
    return resid.rename(name or x.name)


def group_mean(x, g, name=None) -> pd.Series:
    """组均值 (广播回组内每行)。"""
    x = _as_series(x)
    return _grouped(x, g).transform("mean").rename(name or x.name)


def group_std(x, g, ddof: int = 1, name=None) -> pd.Series:
    """组内标准差 (广播回组内每行)。"""
    x = _as_series(x)
    gb = _grouped(x, g)
    return (gb.transform("std") if ddof else gb.transform(lambda s: s.std(ddof=0))).rename(name or x.name)


def group_size(x, g, name=None) -> pd.Series:
    """组内成员数 (广播回组内每行) —— 稀疏组是数据质量的信号。"""
    x = _as_series(x)
    return _grouped(x, g).transform("size").astype(float).rename(name or x.name)


# ===========================================================================
# pp_* —— 预处理算子 (点态, 或需要显式 PIT 安全的参照范围)
# ===========================================================================
def pp_log(x, base: float | None = None, signed: bool = False, eps: float = 0.0,
           name=None) -> pd.Series:
    """对数。

    * 非正数 -> NaN (收益率/成交量等只能取正值时用 pp_log 天然完成清洗)
    * signed=True -> sign(x)*ln(1+|x|), 负值也可用的对称变换
    * eps>0    -> ln(|x| + eps) 再乘原符号, 软化 0
    """
    x = _as_series(x)
    if base is not None:
        b = float(base)
        if not (b > 0 and b != 1):
            raise ValueError(f"base 必须是正数且 != 1, 收到 {base!r}")
    if signed:
        out = np.sign(x) * np.log1p(np.abs(x))
    elif eps > 0:
        out = np.sign(x) * np.log(np.abs(x) + eps)
    else:
        out = pd.Series(np.log(x.where(x > 0)), index=x.index)
    out = out.astype(float)
    out = out.where(np.isfinite(out))
    if base is not None:
        out = out / np.log(float(base))
    return out.rename(name or x.name)


def pp_sqrt(x, name=None) -> pd.Series:
    """平方根 (负值 -> NaN)。常用于压缩成交量/市值的量级。"""
    x = _as_series(x)
    return pd.Series(np.sqrt(x.where(x >= 0)), index=x.index).rename(name or x.name)


def pp_abs(x, name=None) -> pd.Series:
    """绝对值。"""
    return _as_series(x).abs().rename(name or x.name)


def pp_sign(x, name=None) -> pd.Series:
    """符号 (-1/0/+1)。"""
    return _as_series(x).apply(np.sign).astype(float).rename(name or x.name)


def pp_power(x, p: float, name=None) -> pd.Series:
    """幂变换 x^p (负底数配非整数指数结果为 NaN, 不产生 nan 警告)。"""
    x = _as_series(x)
    p = float(p)
    with np.errstate(invalid="ignore", over="ignore"):
        vals = np.power(x.to_numpy(), p)
    out = pd.Series(vals, index=x.index, dtype=float)
    return out.where(np.isfinite(out)).rename(name or x.name)


def pp_clip(x, lower: float | None = None, upper: float | None = None, name=None) -> pd.Series:
    """点态截断到 [lower, upper] (两边可只给一边)。"""
    x = _as_series(x)
    if lower is None and upper is None:
        return x.copy().rename(name or x.name)
    return x.clip(lower, upper).rename(name or x.name)


def pp_diff(x, lag: int = 1, name=None) -> pd.Series:
    """差分 (按 instrument 内滞后)。lag<0 直接报错 —— 那等于取未来。"""
    x = _as_series(x)
    return (x - _shift(x, _lag(lag))).rename(name or x.name)


def pp_pct_change(x, lag: int = 1, name=None) -> pd.Series:
    """变化率 x[t]/x[t-lag]-1 (按 instrument 内滞后)。"""
    x = _as_series(x)
    prev = _shift(x, _lag(lag))
    out = (x / prev - 1.0).where(np.isfinite(x / prev - 1.0))
    return out.rename(name or x.name)


def pp_frac_diff(x, d: float = 0.5, window: int | None = None, log: bool = False,
                 name=None) -> pd.Series:
    """分数阶差分 (Lopez de Prado)。

    系数来自 (1-B)^(-d) = Σ k_j B^j,  k_0 = 1, k_j = k_{j-1}*(d + j - 1)/j;
    y[t] = Σ_{j<w} k_j x[t-j]。对长记忆序列 (价格/成交额) 比整数差分更稳健,
    且不像二阶差分那样放大噪声。

    **window 必填, 不给默认值**: 截断阶数是真正的建模选择 —— w 越大越接近无限阶
    展开, 但噪声/延迟也越大。文献里的最低信息阶 (1+d)/(2-d) 在小 d 时会退化成 1
    (d=0.5 时恰好 =1, 等于"不差分"), 直接拿它当默认值会静默产出退化特征, 所以
    强制显式指定。常用起点: d=0.5 取 w ∈ [5, 10], w=10 ≈ 常见实践值。
    log=True 先取对数 (对价格是标准做法)。
    """
    x = _as_series(x)
    d = float(d)
    if not 0 < d < 2:
        raise ValueError(f"d 必须 ∈ (0,2), 收到 {d}")
    if window is None:
        raise ValueError(
            "window 必填 —— 分数阶差分的截断阶数是建模选择, 没有可靠默认值 "
            "(最低信息阶 (1+d)/(2-d) 在 d<=0.5 时退化成 1 = 不差分)。"
            "常用起点: d=0.5 取 w ∈ [5,10]。")
    w = _win(window)
    base = pp_log(x) if log else x
    coefs = np.empty(w, dtype=float)
    coefs[0] = 1.0
    for j in range(1, w):
        coefs[j] = coefs[j - 1] * (d + j - 1.0) / j
    return _weighted_trail_sum(base, w, coefs).rename(name or x.name)


def pp_ema(x, span: int = 20, adjust: bool = False, name=None) -> pd.Series:
    """指数移动平均 (= ts_ewma 的点态别名, 按 instrument 内计算)。"""
    return ts_ewma(x, span=span, adjust=adjust, name=name)


def _require_pit_reference(by: str) -> str:
    """PIT 铁律 2 的闸门: 参照分布只允许"同刻截面"或"trailing 窗口"。"""
    if by == "time":
        return "time"
    if by in ("ts", "trailing", "window"):
        return "ts"
    raise ValueError(
        f"by={by!r} 不被允许 —— 只接受 'time'(同刻截面) 或 'ts'(trailing 窗口); "
        f"整段样本/全历史估计分布等于用未来信息 (PIT 铁律 2)")


def _ref_mean_std(x: pd.Series, by: str, window: int | None, ddof: int):
    """按参照范围取 (μ, σ)。by='time' 用截面 transform, by='ts' 用 trailing 窗口。"""
    kind = _require_pit_reference(by)
    if kind == "time":
        g = _cs_groupby(x)
        mu = g.transform("mean")
        sd = g.transform("std") if ddof else g.transform(lambda s: s.std(ddof=0))
        return mu, sd
    w = _win(window) if window is not None else None
    if w is None:
        raise ValueError("by='ts' 必须给 window (trailing 窗口长度)")
    return ts_mean(x, w), ts_std(x, w, ddof=ddof)


def pp_zscore(x, by: str = "time", window: int | None = None, ddof: int = 1,
              name=None) -> pd.Series:
    """标准化 z = (x - μ)/σ, 参照分布由 by 决定 (默认同刻截面, PIT 安全)。"""
    x = _as_series(x)
    mu, sd = _ref_mean_std(x, by, window, ddof)
    sd = sd.where(sd > 0)
    return ((x - mu) / sd).rename(name or x.name)


def pp_minmax(x, by: str = "time", window: int | None = None, name=None) -> pd.Series:
    """缩放 (x-min)/(max-min), 参照范围同 pp_zscore。"""
    x = _as_series(x)
    if _require_pit_reference(by) == "time":
        g = _cs_groupby(x)
        lo, hi = g.transform("min"), g.transform("max")
    else:
        w = _win(window) if window is not None else None
        if w is None:
            raise ValueError("by='ts' 必须给 window")
        lo, hi = ts_min(x, w), ts_max(x, w)
    den = (hi - lo).where(hi > lo)
    return ((x - lo) / den).rename(name or x.name)


def pp_robust(x, by: str = "time", window: int | None = None, name=None) -> pd.Series:
    """稳健标准化 (x - median)/IQR, 对离群不敏感。参照范围同 pp_zscore。"""
    x = _as_series(x)
    if _require_pit_reference(by) == "time":
        g = _cs_groupby(x)
        med = g.transform("median")
        iqr = g.transform(lambda s: s.quantile(0.75) - s.quantile(0.25))
    else:
        w = _win(window) if window is not None else None
        if w is None:
            raise ValueError("by='ts' 必须给 window")
        med = ts_median(x, w)
        iqr = ts_quantile(x, w, 0.75) - ts_quantile(x, w, 0.25)
    iqr = iqr.where(iqr > 0)
    return ((x - med) / iqr).rename(name or x.name)


def pp_quantile_bucket(x, n: int = 5, by: str = "time", window: int | None = None,
                       name=None) -> pd.Series:
    """分位分桶 (0..n-1), 参照分布同 pp_zscore (PIT 安全)。

    * by="time": 同一时刻截面内分桶 (截面分位天然同期可比)
    * by="ts"  : trailing 窗口内分桶, 用 (n-1) 个 trailing 分位点切桶;
                 窗口未满 (min_periods=window) 输出 NaN
    """
    x = _as_series(x)
    n = int(n)
    if n < 2:
        raise ValueError(f"n 必须 >= 2, 收到 {n}")
    kind = _require_pit_reference(by)
    if kind == "time":
        p = cs_rank(x).to_numpy()
        bucket = np.minimum(np.floor(p * n), n - 1.0)
    else:
        if window is None:
            raise ValueError("by='ts' 必须给 window")
        w = _win(window)
        xa = x.to_numpy()
        bucket = np.zeros(len(x), dtype=float)
        for q in np.arange(1, n) / n:
            edge = ts_quantile(x, w, float(q), min_periods=w).to_numpy()
            bucket += (xa <= edge)
        bucket = bucket / (n - 1.0)
    out = pd.Series(bucket, index=x.index)
    return out.rename(name or x.name)


def pp_detrend(x, window: int = 20, name=None) -> pd.Series:
    """去线性趋势 (trailing 窗口 OLS 残差)。

    只支持线性 (量化里高阶多项式拟合基本等价于过拟合)。要求完整窗口。
    斜率用闭式解 (b = Σ(t-t̄)x / Σ(t-t̄)², 等价 t=0..w-1 的正交多项式基),
    避免 rolling.apply(polyfit) 的逐窗 Python 回调开销。
    """
    x = _as_series(x)
    w = _win(window)
    t = np.arange(w, dtype=float)
    tm = t.mean()
    denom = float(((t - tm) ** 2).sum())
    if denom <= 0:
        raise ValueError("window 过小, 无法拟合趋势")
    xs = x.to_numpy()
    xt = pd.Series(xs, index=x.index) * 1.0
    # Σ (t - t̄) * x  =  Σ t*x  -  t̄ * Σ x
    sum_x = ts_sum(xt, w, min_periods=w)
    sum_tx = _weighted_trail_sum(xt, w, t)
    slope = (sum_tx - tm * sum_x) / denom
    fitted = slope * xt + (sum_x / w) - slope * tm
    resid = x - fitted
    return resid.rename(name or x.name)


def pp_is_missing(x, name=None) -> pd.Series:
    """缺失指示 1.0/0.0。

    "数据缺失了" 本身是信息 (停牌/退市/接口故障), 别把它当噪声删掉。
    """
    x = _as_series(x)
    return x.isna().astype(float).rename(name or x.name)


def pp_is_outlier(x, threshold: float | None = None, ref: Any = 0.0, name=None) -> pd.Series:
    """离群指示 1.0/0.0。两种用法:

    1. **质量位直通**: x 是 L2 认证的 is_suspect / is_gap 这类布尔列 -> 原样转
       1.0/0.0 (清洗动作用于何处, 就是最强的特征)。
    2. **阈值判定**: |x - ref| > threshold -> 1.0。

    对数值序列未给 threshold 时抛错: "离群"必须相对于某个参照定义, 没有参照的
    绝对阈值判断没有意义 (也容易被当成自由调参旋钮)。需要时序/截面离群标记
    请在表达式里写: ``(ts_zscore(x, 24) > 3).astype(float)`` 或
    ``cs_zscore(x).abs() > 3``。
    """
    x = _as_series(x)
    if x.dtype == bool:
        return x.astype(float).rename(name or x.name)
    if threshold is None:
        raise ValueError(
            "数值序列调用 pp_is_outlier 必须给 threshold; 或直接传入布尔质量列 "
            "(is_suspect / is_gap) —— 无参照的绝对阈值判断没有意义")
    t = float(threshold)
    r = ref if isinstance(ref, pd.Series) else float(ref)
    return ((x - r).abs() > t).astype(float).rename(name or x.name)


# ===========================================================================
# 注册表
# ===========================================================================
@dataclass(frozen=True)
class OperatorSpec:
    """一个算子的元数据 (供引擎/MCP 展示与静态检查)。"""

    name: str
    family: str                       # ts / cs / group / pp
    func: Callable
    min_args: int                     # 除第一个数据参数外的必填位置参数个数
    max_args: int | None              # None = 可变参
    time_axis: str                    # instrument / time / (time,group) / point
    estimator: str                    # none / cross_section / trailing_window
    doc: str

    @property
    def signature(self) -> str:
        return f"{self.name}{inspect.signature(self.func)}"

    def summary(self) -> dict:
        return {
            "name": self.name, "family": self.family,
            "signature": self.signature, "time_axis": self.time_axis,
            "estimator": self.estimator, "doc": self.doc,
            "min_args": self.min_args, "max_args": self.max_args,
        }


def _spec(name, time_axis="instrument", estimator="none") -> OperatorSpec:
    fn = globals()[name]
    sig = inspect.signature(fn)
    params = [p for p in sig.parameters.values()
              if p.default is inspect.Parameter.empty and p.kind in
              (p.POSITIONAL_ONLY, p.POSITIONAL_OR_KEYWORD)]
    has_var = any(p.kind == p.VAR_POSITIONAL for p in sig.parameters.values())
    doc = (fn.__doc__ or "").strip().splitlines()[0] if fn.__doc__ else ""
    return OperatorSpec(
        name=name, family=name.split("_", 1)[0], func=fn,
        min_args=max(len(params) - 1, 0), max_args=None if has_var else len(sig.parameters) - 1,
        time_axis=time_axis, estimator=estimator, doc=doc,
    )


_TS_NAMES = [
    "ts_delay", "ts_backfill", "ts_mean", "ts_std", "ts_sum", "ts_min", "ts_max",
    "ts_median", "ts_quantile", "ts_delta", "ts_pct_change", "ts_rank", "ts_zscore",
    "ts_skew", "ts_kurt", "ts_ewma", "ts_decay_linear", "ts_corr",
]
_CS_NAMES = ["cs_rank", "cs_zscore", "cs_winsorize", "cs_normalize", "cs_scale",
             "cs_rel", "cs_residual", "cs_rank_normal", "cs_winsorize_mad"]
_GROUP_NAMES = ["group_rank", "group_zscore", "group_neutralize", "group_mean",
                "group_std", "group_size"]
_PP_NAMES = [
    "pp_log", "pp_sqrt", "pp_abs", "pp_sign", "pp_power", "pp_clip",
    "pp_diff", "pp_pct_change", "pp_frac_diff", "pp_ema",
    "pp_zscore", "pp_minmax", "pp_robust", "pp_quantile_bucket", "pp_detrend",
    "pp_is_missing", "pp_is_outlier",
    # 2026-10-05 补: 无状态预处理 (参数只来自当期截面或 trailing 窗口)
    "pp_soft_threshold", "pp_savgol", "pp_boxcox",
]

TS_OPERATORS: dict[str, OperatorSpec] = {
    n: _spec(n, "instrument") for n in _TS_NAMES}
CS_OPERATORS: dict[str, OperatorSpec] = {
    n: _spec(n, "time", "cross_section") for n in _CS_NAMES}
GROUP_OPERATORS: dict[str, OperatorSpec] = {
    n: _spec(n, "(time,group)", "cross_section") for n in _GROUP_NAMES}
PP_OPERATORS: dict[str, OperatorSpec] = {
    n: _spec(n, "point" if n not in ("pp_diff", "pp_pct_change", "pp_frac_diff", "pp_ema",
                                    "pp_zscore", "pp_minmax", "pp_robust",
                                    "pp_quantile_bucket", "pp_detrend")
     else ("instrument" if n in ("pp_diff", "pp_pct_change", "pp_frac_diff", "pp_ema",
                                "pp_detrend") else "time|ts"),
     "none" if n in ("pp_log", "pp_sqrt", "pp_abs", "pp_sign", "pp_power", "pp_clip",
                     "pp_diff", "pp_pct_change", "pp_is_missing", "pp_is_outlier")
     else ("trailing_window" if n in ("pp_frac_diff", "pp_ema", "pp_detrend")
           else "cross_section|trailing_window"))
    for n in _PP_NAMES}

ALL_OPERATORS: dict[str, OperatorSpec] = {
    **TS_OPERATORS, **CS_OPERATORS, **GROUP_OPERATORS, **PP_OPERATORS}

FAMILY_REGISTRIES = {
    "ts": TS_OPERATORS, "cs": CS_OPERATORS,
    "group": GROUP_OPERATORS, "pp": PP_OPERATORS,
}

# --- 公开别名: fields.py / F2 PIT 引擎复用同一套分组实现 (单一事实来源) ----
inst_codes = _inst_codes
cs_groupby = _cs_groupby
group_codes = _tg_codes
require_causal_order = _require_causal_order
resolve_group_labels = _resolve_group
shift_within = _shift
time_level = _time_level

__all__ = (
    [*_TS_NAMES, *_CS_NAMES, *_GROUP_NAMES, *_PP_NAMES]
    + ["ALL_OPERATORS", "TS_OPERATORS", "CS_OPERATORS", "GROUP_OPERATORS",
       "PP_OPERATORS", "FAMILY_REGISTRIES", "OperatorSpec",
       "describe_operator", "operator_names", "referenced_operators",
       "unknown_operators", "audit_pit_source",
       "INSTRUMENT_LEVEL", "TIME_LEVEL",
       "inst_codes", "cs_groupby", "group_codes", "require_causal_order",
       "resolve_group_labels", "shift_within", "time_level"]
)


def describe_operator(name: str) -> dict:
    """算子元数据 (MCP describe 用)。"""
    if name not in ALL_OPERATORS:
        raise KeyError(f"未知算子 {name!r}")
    return ALL_OPERATORS[name].summary()


def operator_names(family: str | None = None) -> list[str]:
    if family is None:
        return list(ALL_OPERATORS)
    if family not in FAMILY_REGISTRIES:
        raise KeyError(f"未知算子族 {family!r}, 可用: {list(FAMILY_REGISTRIES)}")
    return list(FAMILY_REGISTRIES[family])


_IDENT_RE = re.compile(r"[A-Za-z_][A-Za-z_0-9]*")


def referenced_operators(expr: str) -> set[str]:
    """从表达式字符串里扫出用到的算子名 (**lint 级**, 不是解析器)。

    用途: F3/F4 拿到 expr 后快速提示"这个算子不存在/拼错了"。真正的解析
    交给 expr_codegen (决策 1), 这里不做真语法分析, 故不认识字符串字面量。
    """
    if not isinstance(expr, str):
        raise TypeError(f"expr 必须是字符串, 收到 {type(expr).__name__}")
    return {m for m in _IDENT_RE.findall(expr) if m in ALL_OPERATORS}


def unknown_operators(expr: str) -> set[str]:
    """扫出疑似算子但库里没有的标识符 (排除已知常量/参数名)。"""
    known = set(ALL_OPERATORS) | {
        "True", "False", "None", "and", "or", "not", "if", "else"}
    return {m for m in _IDENT_RE.findall(expr)
            if m not in known and (m.startswith(("ts_", "cs_", "group_", "pp_")))}


def _strip_comments_and_strings(source: str) -> str:
    """把注释与字符串字面量所在行整行挖空 (保留行号), 只留可执行代码。

    不这么做的话, 本模块的文档里"禁止 center=True"这句话自己就会被审计报警。
    """
    import io
    import tokenize
    lines = source.splitlines()
    try:
        toks = list(tokenize.generate_tokens(io.StringIO(source).readline))
    except (tokenize.TokenError, IndentationError):
        return source
    for tok in toks:
        if tok.type in (tokenize.COMMENT, tokenize.STRING):
            for ln in range(tok.start[0], tok.end[0] + 1):
                if 1 <= ln <= len(lines):
                    lines[ln - 1] = ""
    return "\n".join(lines)


def audit_pit_source(source: str | None = None) -> list[tuple[str, str, int]]:
    """静态 PIT 审计: 在本模块源码里找"用了未来"的可疑写法 (跳过注释与字符串)。

    返回 [(命中, 说明, 行号), ...]; 空列表 = 通过。
    这是自检, 不是证明 —— 真正的保证是 _test_operators.py 的未来不变性测试。
    """
    if source is None:
        import pathlib
        source = pathlib.Path(__file__).read_text(encoding="utf-8")
    code = _strip_comments_and_strings(source)
    out: list[tuple[str, str, int]] = []
    for pat, why in FORBIDDEN_PATTERNS:
        for m in re.finditer(pat, code):
            out.append((m.group(0), why, code[: m.start()].count("\n") + 1))
    return out


# ===========================================================================
# 自检
# ===========================================================================
def _demo_panel(n_inst: int = 4, n_time: int = 40, seed: int = 7) -> pd.DataFrame:
    """冒烟面板: 4 个 instrument × 2 个板块 (每板块 2 个成员) × n_time 小时。"""
    rs = np.random.RandomState(seed)
    insts = [f"C{i}-USDT" for i in range(n_inst)]
    sectors = [f"sector_{i % 2}" for i in range(n_inst)]
    times = pd.date_range("2023-01-01", periods=n_time, freq="h", tz="UTC")
    idx = pd.MultiIndex.from_product([insts, times], names=["instrument", "time"])
    x = pd.Series(rs.randn(len(idx)).cumsum() + 100.0, index=idx, name="x")
    g = pd.Series(np.repeat(sectors, n_time), index=idx, name="sector")
    return pd.DataFrame({"x": x, "g": g})


if __name__ == "__main__":
    import json

    print("=" * 72)
    print(f"算子库: {len(ALL_OPERATORS)} 个 "
          f"(ts={len(TS_OPERATORS)} cs={len(CS_OPERATORS)} "
          f"group={len(GROUP_OPERATORS)} pp={len(PP_OPERATORS)})")
    print("=" * 72)
    for fam in ("ts", "cs", "group", "pp"):
        print(f"\n[{fam}]")
        for n in operator_names(fam):
            s = ALL_OPERATORS[n]
            print(f"  {n:18s} {s.doc}")

    df = _demo_panel()
    x, g = df["x"], df["g"]
    y = x * 0.5 + df["x"].shift(1).fillna(0)          # 造一条相关序列测 ts_corr
    print("\n[冒烟] 代表性算子")
    checks = [
        ("ts_mean(5)", ts_mean(x, 5)),
        ("ts_rank(20)", ts_rank(x, 20)),
        ("ts_zscore(20)", ts_zscore(x, 20)),
        ("ts_decay_linear(10)", ts_decay_linear(x, 10)),
        ("ts_corr(x, y, 10)", ts_corr(x, y, 10)),
        ("ts_frac/q80(20)", ts_quantile(x, 20, 0.8)),
        ("cs_rank", cs_rank(x)),
        ("cs_zscore", cs_zscore(x)),
        ("cs_winsorize(2)", cs_winsorize(x, 2)),
        ("cs_normalize(sum_abs)", cs_normalize(x)),
        ("group_rank(sector)", group_rank(x, g)),
        ("group_neutralize(sector)", group_neutralize(x, g)),
        ("group_size(sector)", group_size(x, g)),
        ("pp_frac_diff(0.5, w=8)", pp_frac_diff(x, 0.5, 8)),
        ("pp_quantile_bucket(5)", pp_quantile_bucket(x, 5)),
        ("pp_quantile_bucket(ts)", pp_quantile_bucket(x, 5, by="ts", window=20)),
        ("pp_detrend(20)", pp_detrend(x, 20)),
        ("pp_zscore(ts,20)", pp_zscore(x, by="ts", window=20)),
        ("pp_is_missing", pp_is_missing(x)),
    ]
    bad = 0
    for label, s in checks:
        v = s.dropna().to_numpy()
        ok = len(v) > 0 and np.isfinite(v).all()
        bad += (not ok)
        print(f"  {'OK ' if ok else 'BAD'} {label:24s} 非空={int(s.notna().sum()):4d} "
              f"min={v.min() if len(v) else float('nan'):+.4f} "
              f"max={v.max() if len(v) else float('nan'):+.4f}")
    print(f"  -> {len(checks) - bad}/{len(checks)} 通过")

    viol = audit_pit_source()
    print(f"\n[PIT 静态审计] {'通过 (无违规写法)' if not viol else viol}")
    print(json.dumps(describe_operator("ts_decay_linear"), ensure_ascii=False, indent=2))
