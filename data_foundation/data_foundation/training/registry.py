# -*- coding: utf-8 -*-
"""registry.py — 模型注册表 (模型成为第一类对象)

设计: docs/supervised-learning-protocol-design.md §6

ModelArtifact = 权重 + 有状态预处理参数 + 全套指纹 + 训练窗口。model_hash 是
唯一身份 (同 seed + 同指纹 -> 同 hash -> 可复现)。

存储: data/models/<name>/<hash8>.pkl + <hash8>.meta.json (不可变版本,
与特征 registry_versioning、标签存储同构)。

服务端应用 (valid/oos): load_model -> 校验 feature_fingerprint 与池内特征缓存
一致 -> predict。**指纹不符 = 拒绝评估** (防"训练一套、评估另一套"的静默错配)。
"""
from __future__ import annotations

import hashlib
import json
import os
import pickle
from dataclasses import dataclass, asdict
from datetime import datetime, timezone

from .models import make_model
from ..config import DATA_ROOT
from ..evaluation.metrics import METRICS_VERSION

__all__ = ["ModelArtifact", "MODELS_DIR", "register_model", "load_model",
           "list_models", "describe_model"]

MODELS_DIR = os.path.join(DATA_ROOT, "models")


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _sha1(*parts) -> str:
    h = hashlib.sha1()
    for p in parts:
        h.update(str(p).encode("utf-8"))
        h.update(b"|")
    return h.hexdigest()


@dataclass(frozen=True)
class ModelArtifact:
    name: str
    family: str
    params: dict
    model_hash: str
    state_hash: str
    feature_fingerprint: str = ""
    label_fingerprint: str = ""
    label_name: str = ""
    train_fold: str = ""                 # 训练窗口 (train_start~train_end)
    train_data_fingerprint: str = ""
    metrics_version: str = METRICS_VERSION
    version: int = 1
    created_at: str = ""
    note: str = ""

    def to_dict(self) -> dict:
        return asdict(self)


def _state_hash(state: dict) -> str:
    return _sha1(pickle.dumps(state))


def register_model(name: str, model, *, params: dict | None = None,
                   feature_fingerprint: str = "", label_fingerprint: str = "",
                   label_name: str = "", train_fold: str = "",
                   train_data_fingerprint: str = "",
                   metrics_version: str = METRICS_VERSION,
                   note: str = "", root: str | None = None) -> ModelArtifact:
    """注册一个**已训练**的模型。返回 artifact (含 model_hash)。"""
    if not name:
        raise ValueError("模型名不能为空")
    state = model.state()
    sh = _state_hash(state)
    params = params or {}
    mh = _sha1(name, model.family, repr(sorted(params.items())),
               sh, feature_fingerprint, label_fingerprint, train_fold)
    # 版本: 同 name 已存在的最大版本 + 1 (同 hash 复用旧版本, 不可变)
    root = root or MODELS_DIR
    d = os.path.join(root, name)
    os.makedirs(d, exist_ok=True)
    version = 1
    existing = list_models(name=name, root=root)
    if existing:
        if any(e["model_hash"] == mh for e in existing):
            # 同模型已注册 -> 复用原版本 (不可变), 返回其 artifact
            prev = next(e for e in existing if e["model_hash"] == mh)
            return ModelArtifact(**{k: v for k, v in prev.items()
                                    if k in ModelArtifact.__annotations__})
        version = max(int(e["version"]) for e in existing) + 1
    art = ModelArtifact(name=name, family=model.family, params=dict(params),
                        model_hash=mh, state_hash=sh,
                        feature_fingerprint=feature_fingerprint,
                        label_fingerprint=label_fingerprint,
                        label_name=label_name, train_fold=train_fold,
                        train_data_fingerprint=train_data_fingerprint,
                        metrics_version=metrics_version, version=version,
                        created_at=_now(), note=note)
    with open(os.path.join(d, f"{mh[:8]}.pkl"), "wb") as f:
        pickle.dump({"state": state, "artifact": art}, f)
    with open(os.path.join(d, f"{mh[:8]}.meta.json"), "w", encoding="utf-8") as f:
        json.dump(art.to_dict(), f, ensure_ascii=False, indent=2)
    return art


def load_model(name: str, model_hash: str | None = None, *,
               root: str | None = None,
               verify_feature_fingerprint: str | None = None):
    """加载模型 (返回 (model, artifact))。指纹校验通过才允许应用。"""
    root = root or MODELS_DIR
    art = describe_model(name, model_hash=model_hash, root=root)
    path = os.path.join(root, name, f"{art['model_hash'][:8]}.pkl")
    with open(path, "rb") as f:
        blob = pickle.load(f)
    state = blob["state"]
    if _state_hash(state) != art["state_hash"]:
        raise ValueError(f"模型 {name} 权重哈希不符 —— 文件被改动过 (不可变版本)")
    model = make_model(art["family"], **art["params"])
    model.load_state(state)
    if verify_feature_fingerprint is not None and \
            art["feature_fingerprint"] != verify_feature_fingerprint:
        raise ValueError(
            f"特征指纹不符: 模型训练时用 {art['feature_fingerprint'][:8]}, "
            f"当前特征集 {verify_feature_fingerprint[:8]} —— 拒绝应用 "
            f"(防训练/评估特征错配)")
    return model, ModelArtifact(**{k: v for k, v in art.items()
                                   if k in ModelArtifact.__annotations__})


def describe_model(name: str, model_hash: str | None = None, *,
                   root: str | None = None) -> dict:
    root = root or MODELS_DIR
    d = os.path.join(root, name)
    if not os.path.isdir(d):
        raise FileNotFoundError(f"模型 {name!r} 未注册 ({d})")
    rows = []
    for fn in sorted(os.listdir(d)):
        if fn.endswith(".meta.json"):
            with open(os.path.join(d, fn), encoding="utf-8") as f:
                rows.append(json.load(f))
    if not rows:
        raise FileNotFoundError(f"模型 {name!r} 无 meta")
    if model_hash is None:
        return rows[-1]
    for r in rows:
        if r["model_hash"] == model_hash:
            return r
    raise KeyError(f"模型 {name} 无 hash={model_hash} 的版本")


def list_models(name: str | None = None, *, root: str | None = None) -> list[dict]:
    root = root or MODELS_DIR
    if not os.path.isdir(root):
        return []
    out = []
    for nm in sorted(os.listdir(root)):
        d = os.path.join(root, nm)
        if not os.path.isdir(d):
            continue
        if name and nm != name:
            continue
        for fn in sorted(os.listdir(d)):
            if fn.endswith(".meta.json"):
                with open(os.path.join(d, fn), encoding="utf-8") as f:
                    out.append(json.load(f))
    return out


if __name__ == "__main__":  # pragma: no cover
    import tempfile
    import numpy as np
    import pandas as pd
    rng = np.random.default_rng(0)
    X = pd.DataFrame(rng.normal(size=(100, 3)), columns=list("abc"))
    y = pd.Series(X["a"] + rng.normal(0, 0.1, 100))
    m = make_model("linear").fit(X, y)
    with tempfile.TemporaryDirectory() as td:
        art = register_model("probe", m, params={"alpha": 1.0},
                             feature_fingerprint="fp1", root=td)
        print("registered:", art.name, "v", art.version, art.model_hash[:8])
        m2, art2 = load_model("probe", art.model_hash, root=td)
        assert np.allclose(m.predict(X), m2.predict(X))
        # 指纹不符 -> 拒绝
        try:
            load_model("probe", art.model_hash, root=td,
                       verify_feature_fingerprint="fp2")
            raise AssertionError("指纹不符应拒绝")
        except ValueError as e:
            print("[自检] 指纹不符拒绝:", str(e)[:40])
        # 同模型重复注册 -> 复用同 hash
        art3 = register_model("probe", m, params={"alpha": 1.0},
                              feature_fingerprint="fp1", root=td)
        assert art3.model_hash == art.model_hash and art3.version == 1
    print("registry OK")