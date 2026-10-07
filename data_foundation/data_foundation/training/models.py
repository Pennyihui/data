# -*- coding: utf-8 -*-
"""models.py — 模型家族统一接口 + 有状态预处理随模型携带

设计: docs/supervised-learning-protocol-design.md §5

统一接口 (sklearn 风格薄包装, 三个家族可插拔)
-----------------------------------------------
    fit(X, y) -> None          在**训练 fold** 上拟合 (含预处理参数)
    predict(X) -> 分数         越大越看好 (决策 M7)
    state()/load_state()       权重 + 预处理参数一起序列化

原则2 (有状态预处理随模型携带)
-----------------------------
标准化/填充的参数在 fit 内拟合, 序列化进 state(); predict **只用保存参数**。
**禁止在 predict 时重拟合** —— 那是训练/服务静默漂移的源头 (记忆里踩过的坑:
两个参数来自训练集的算子算出同样的特征看着正常但错误且不报错)。
"""
from __future__ import annotations

import io
from dataclasses import dataclass, field
from typing import Protocol

import numpy as np
import pandas as pd

__all__ = [
    "Standardizer", "BaseModel", "LinearModel", "GBDTModel", "BaselineModel",
    "make_model", "MODEL_FAMILIES", "ModelNotFitted",
]


class ModelNotFitted(RuntimeError):
    """predict 前未 fit / 未 load_state。"""


# ---------------------------------------------------------------------------
# 有状态预处理 (参数随模型走)
# ---------------------------------------------------------------------------
class Standardizer:
    """中位数填充 + 标准化。参数**只在 fit 拟合**, 之后只读。"""

    def __init__(self, standardize: bool = True):
        self.standardize = standardize
        self.median_: pd.Series | None = None
        self.mean_: pd.Series | None = None
        self.std_: pd.Series | None = None
        self.columns_: list[str] = []

    def fit(self, X: pd.DataFrame) -> "Standardizer":
        self.columns_ = list(X.columns)
        self.median_ = X.median(numeric_only=False)
        Xf = X.fillna(self.median_)
        if self.standardize:
            self.mean_ = Xf.mean()
            self.std_ = Xf.std(ddof=1).replace(0.0, 1.0)
        else:
            self.mean_ = pd.Series(0.0, index=X.columns)
            self.std_ = pd.Series(1.0, index=X.columns)
        return self

    def transform(self, X: pd.DataFrame) -> np.ndarray:
        if self.median_ is None:
            raise ModelNotFitted("Standardizer 未 fit")
        cols = [c for c in self.columns_ if c in X.columns]
        Xf = X[cols].fillna(self.median_[cols])
        Z = (Xf - self.mean_[cols]) / self.std_[cols]
        return np.nan_to_num(Z.to_numpy(dtype=float), nan=0.0,
                             posinf=0.0, neginf=0.0)

    # -- 序列化 --
    def state(self) -> dict:
        return {"standardize": self.standardize,
                "median": self.median_.to_dict() if self.median_ is not None else None,
                "mean": self.mean_.to_dict() if self.mean_ is not None else None,
                "std": self.std_.to_dict() if self.std_ is not None else None,
                "columns": list(self.columns_)}

    def load_state(self, st: dict) -> "Standardizer":
        self.standardize = bool(st.get("standardize", True))
        self.columns_ = list(st.get("columns") or [])
        self.median_ = pd.Series(st["median"]) if st.get("median") else None
        self.mean_ = pd.Series(st["mean"]) if st.get("mean") else None
        self.std_ = pd.Series(st["std"]) if st.get("std") else None
        return self


# ---------------------------------------------------------------------------
# 基类
# ---------------------------------------------------------------------------
class BaseModel(Protocol):
    family: str

    def fit(self, X: pd.DataFrame, y: pd.Series) -> "BaseModel": ...
    def predict(self, X: pd.DataFrame) -> np.ndarray: ...
    def state(self) -> dict: ...
    def load_state(self, st: dict) -> None: ...


class _Base:
    family = "base"

    def __init__(self, *, standardize: bool = True, seed: int = 0):
        self.seed = int(seed)
        self.pre = Standardizer(standardize=standardize)
        self._fitted = False

    # -- 子类实现 --
    def _fit_core(self, Z: np.ndarray, y: np.ndarray) -> None:  # pragma: no cover
        raise NotImplementedError

    def _predict_core(self, Z: np.ndarray) -> np.ndarray:  # pragma: no cover
        raise NotImplementedError

    def _state_core(self) -> dict:  # pragma: no cover
        return {}

    def _load_core(self, st: dict) -> None:  # pragma: no cover
        return None

    # -- 统一入口 --
    def fit(self, X: pd.DataFrame, y: pd.Series) -> "_Base":
        self.pre.fit(X)                      # 预处理只在训练 fold 拟合
        Z = self.pre.transform(X)
        yv = np.asarray(y, dtype=float)
        ok = np.isfinite(yv)
        self._fit_core(Z[ok], yv[ok])
        self._fitted = True
        return self                          # 可链式: make_model(..).fit(X, y)

    def predict(self, X: pd.DataFrame) -> np.ndarray:
        if not self._fitted:
            raise ModelNotFitted(f"{self.family} 未 fit")
        Z = self.pre.transform(X)            # 只用保存参数 (禁重拟合)
        return self._predict_core(Z)

    def state(self) -> dict:
        if not self._fitted:
            raise ModelNotFitted("未 fit 无从序列化")
        return {"family": self.family, "seed": self.seed,
                "pre": self.pre.state(), "core": self._state_core()}

    def load_state(self, st: dict) -> None:
        self.pre.load_state(st["pre"])
        self.seed = int(st.get("seed", 0))
        self._load_core(st.get("core") or {})
        self._fitted = True

    def __repr__(self):
        return f"<{type(self).__name__} fitted={self._fitted} seed={self.seed}>"


# ---------------------------------------------------------------------------
# 家族: linear (Ridge / Logistic)
# ---------------------------------------------------------------------------
class LinearModel(_Base):
    family = "linear"

    def __init__(self, *, task: str = "regression", alpha: float = 1.0,
                 standardize: bool = True, seed: int = 0):
        super().__init__(standardize=standardize, seed=seed)
        if task not in ("regression", "classification"):
            raise ValueError("task ∈ {regression, classification}")
        self.task = task
        self.alpha = float(alpha)
        self.model_ = None

    def _fit_core(self, Z, y):
        from sklearn.linear_model import LogisticRegression, Ridge
        if self.task == "classification":
            # 标签 {0,1} -> LogisticRegression 需要两类; 单类 -> 常数模型
            uniq = np.unique(y)
            if len(uniq) < 2:
                self.model_ = ("const", float(uniq[0]))
                return
            self.model_ = ("logreg", LogisticRegression(
                C=1.0 / max(self.alpha, 1e-9), max_iter=1000,
                random_state=self.seed).fit(Z, y))
        else:
            self.model_ = ("ridge", Ridge(alpha=self.alpha).fit(Z, y))

    def _predict_core(self, Z):
        kind, m = self.model_
        if kind == "const":
            return np.full(len(Z), m)
        if kind == "logreg":
            return m.predict_proba(Z)[:, 1]      # 正类概率 (决策 M7)
        return m.predict(Z)

    def _state_core(self):
        import pickle
        kind = self.model_[0] if self.model_ else "const"
        # 只序列化**估计器本体** (外层的 (kind, obj) 元组由 load 侧重建)
        return {"task": self.task, "alpha": self.alpha, "kind": kind,
                "model": pickle.dumps(self.model_[1]) if kind != "const" else None,
                "const": self.model_[1] if kind == "const" else None}

    def _load_core(self, st):
        import pickle
        self.task = st.get("task", "regression")
        self.alpha = float(st.get("alpha", 1.0))
        if st.get("kind") == "const":
            self.model_ = ("const", st.get("const"))
        else:
            self.model_ = (st["kind"], pickle.loads(st["model"]))


# ---------------------------------------------------------------------------
# 家族: gbdt (LightGBM 优先, 回落 XGBoost)
# ---------------------------------------------------------------------------
class GBDTModel(_Base):
    family = "gbdt"

    def __init__(self, *, task: str = "regression", n_estimators: int = 100,
                 max_depth: int = 4, learning_rate: float = 0.05,
                 standardize: bool = True, seed: int = 0):
        super().__init__(standardize=standardize, seed=seed)
        self.task = task
        self.n_estimators = int(n_estimators)
        self.max_depth = int(max_depth)
        self.learning_rate = float(learning_rate)
        self.model_ = None
        self.backend_ = None

    def _fit_core(self, Z, y):
        if self.task == "classification":
            uniq = np.unique(y)
            if len(uniq) < 2:
                self.model_ = ("const", float(uniq[0]))
                self.backend_ = "const"
                return
            try:
                import lightgbm as lgb
                self.backend_ = "lightgbm"
                self.model_ = ("clf", lgb.LGBMClassifier(
                    n_estimators=self.n_estimators, max_depth=self.max_depth,
                    learning_rate=self.learning_rate, random_state=self.seed,
                    verbose=-1).fit(Z, y))
            except ImportError:
                from xgboost import XGBClassifier
                self.backend_ = "xgboost"
                self.model_ = ("clf", XGBClassifier(
                    n_estimators=self.n_estimators, max_depth=self.max_depth,
                    learning_rate=self.learning_rate, random_state=self.seed,
                    verbosity=0).fit(Z, y))
        else:
            try:
                import lightgbm as lgb
                self.backend_ = "lightgbm"
                self.model_ = ("reg", lgb.LGBMRegressor(
                    n_estimators=self.n_estimators, max_depth=self.max_depth,
                    learning_rate=self.learning_rate, random_state=self.seed,
                    verbose=-1).fit(Z, y))
            except ImportError:
                from xgboost import XGBRegressor
                self.backend_ = "xgboost"
                self.model_ = ("reg", XGBRegressor(
                    n_estimators=self.n_estimators, max_depth=self.max_depth,
                    learning_rate=self.learning_rate, random_state=self.seed,
                    verbosity=0).fit(Z, y))

    def _predict_core(self, Z):
        kind, m = self.model_
        if kind == "const":
            return np.full(len(Z), m)
        if kind == "clf":
            return m.predict_proba(Z)[:, 1]
        return m.predict(Z)

    def _state_core(self):
        import pickle
        kind = self.model_[0] if self.model_ else "const"
        return {"task": self.task, "n_estimators": self.n_estimators,
                "max_depth": self.max_depth,
                "learning_rate": self.learning_rate, "backend": self.backend_,
                "kind": kind,
                "model": pickle.dumps(self.model_[1]) if kind != "const" else None,
                "const": self.model_[1] if kind == "const" else None}

    def _load_core(self, st):
        import pickle
        self.task = st.get("task", "regression")
        self.backend_ = st.get("backend")
        self.n_estimators = int(st.get("n_estimators", 100))
        self.max_depth = int(st.get("max_depth", 4))
        self.learning_rate = float(st.get("learning_rate", 0.05))
        if st.get("kind") == "const":
            self.model_ = ("const", st.get("const"))
        else:
            self.model_ = (st["kind"], pickle.loads(st["model"]))


# ---------------------------------------------------------------------------
# 家族: baseline (对照: 常数/零模型)
# ---------------------------------------------------------------------------
class BaselineModel(_Base):
    family = "baseline"

    def __init__(self, *, mode: str = "mean", standardize: bool = False,
                 seed: int = 0):
        super().__init__(standardize=standardize, seed=seed)
        self.mode = mode
        self.value_ = 0.0

    def _fit_core(self, Z, y):
        self.value_ = float(np.mean(y)) if self.mode == "mean" else 0.0

    def _predict_core(self, Z):
        return np.full(len(Z), self.value_)

    def _state_core(self):
        return {"mode": self.mode, "value": self.value_}

    def _load_core(self, st):
        self.mode = st.get("mode", "mean")
        self.value_ = float(st.get("value", 0.0))


# ---------------------------------------------------------------------------
# 工厂
# ---------------------------------------------------------------------------
MODEL_FAMILIES = {
    "linear": LinearModel,
    "gbdt": GBDTModel,
    "baseline": BaselineModel,
}


def make_model(family: str, **params) -> _Base:
    if family not in MODEL_FAMILIES:
        raise KeyError(f"未知模型家族 {family!r}; 可用: {sorted(MODEL_FAMILIES)}")
    return MODEL_FAMILIES[family](**params)


if __name__ == "__main__":  # pragma: no cover
    rng = np.random.default_rng(0)
    X = pd.DataFrame(rng.normal(size=(200, 3)), columns=list("abc"))
    y = pd.Series(X["a"] * 2 + rng.normal(0, 0.1, 200))
    m = make_model("linear", alpha=1.0)
    m.fit(X, y)
    pred = m.predict(X)
    print("linear R^2-ish corr:", round(float(np.corrcoef(pred, y)[0, 1]), 4))
    st = m.state()
    m2 = make_model("linear")
    m2.load_state(st)
    assert np.allclose(m.predict(X), m2.predict(X)), "序列化后预测应一致"
    print("state roundtrip OK")