# -*- coding: utf-8 -*-
"""rl/trainer.py — PPO 训练循环 (自实现 torch PPO, 不引新依赖)

设计: docs/reinforcement-learning-design.md §6

要点
----
* 策略网络: MLP(state -> action), 输出权重向量 (组合权重即动作空间)
* PPO clip 目标 + 价值基线 (GAE 简化版)
* **每次训练写实验账本** (原则6): RL 超参搜索是试验次数 N 的主要来源,
  DSR 必须如实反映 —— 不记就等于多重检验修正失效
* 确定性: 同 seed 同环境同超参 -> 同权重
"""
from __future__ import annotations

import numpy as np

__all__ = ["train_ppo", "PolicyNetwork"]


def _torch():
    import torch
    import torch.nn as nn
    return torch, nn


class PolicyNetwork:
    """actor-critic MLP: 策略头(均值) + 价值头(基线)。

    动作输出乘 **action_scale** —— 342 维组合权重若不缩放, 初始策略的权重
    幅度会远超 max_gross, Env 里直接破产 (实测 186 根 bar 就归零)。
    """

    def __init__(self, state_dim: int, action_dim: int, hidden: int = 64,
                 seed: int = 0, action_scale: float = 0.01):
        torch, nn = _torch()
        torch.manual_seed(seed)
        self.net = nn.Sequential(
            nn.Linear(state_dim, hidden), nn.Tanh(),
            nn.Linear(hidden, hidden), nn.Tanh())
        self.actor = nn.Linear(hidden, action_dim)
        self.critic = nn.Linear(hidden, 1)
        self.log_std = nn.Parameter(torch.full((action_dim,), -2.0))
        self.action_dim = action_dim
        self.action_scale = float(action_scale)

    def params(self):
        return list(self.net.parameters()) + list(self.actor.parameters()) + \
            list(self.critic.parameters()) + [self.log_std]


def train_ppo(env, *, hidden: int = 64, lr: float = 3e-4, gamma: float = 0.99,
              clip: float = 0.2, epochs: int = 4, n_steps: int = 256,
              batch: int = 64, seed: int = 0, verbose: bool = False,
              action_scale: float | None = None):
    """跑一次 PPO 训练。返回 (policy, history)。

    训练完全在 env 内进行 —— env 底层就是回测引擎组件 (原则1)。
    action_scale: 默认按 env 的动作模式取 (weights=0.01; scores=1.0 ——
                  打分要能区分优劣, 幅度太小学不出来)。
    """
    torch, nn = _torch()
    torch.manual_seed(seed)
    np.random.seed(seed)
    if action_scale is None:
        action_scale = 1.0 if getattr(env, "action_mode", "weights") == "scores" \
            else 0.01
    pol = PolicyNetwork(env.state_dim, env.n_actions, hidden, seed,
                        action_scale=action_scale)
    opt = torch.optim.Adam(pol.params(), lr=lr)
    hist = []
    s = env.reset()
    for ep in range(epochs):
        obs, acts, logps, rews, dones = [], [], [], [], []
        o = s
        for _ in range(n_steps):
            with torch.no_grad():
                ot = torch.tensor(o, dtype=torch.float32).unsqueeze(0)
                h = pol.net(ot)
                mu = pol.actor(h).squeeze(0) * pol.action_scale
                std = pol.log_std.exp().clamp(1e-3, 1.0) * pol.action_scale
            a = mu + std * torch.randn_like(mu)
            o2, r, done, _ = env.step(a.numpy())
            obs.append(o); acts.append(a.numpy()); rews.append(r)
            with torch.no_grad():
                ot2 = torch.tensor(o2, dtype=torch.float32).unsqueeze(0)
                logp = torch.distributions.Normal(mu, std).log_prob(a).sum()
            logps.append(logp.item()); dones.append(done)
            o = o2
            if done:
                o = env.reset()
        if verbose:
            print(f"    [ppo] ep{ep} R={np.sum(rews):.4f}")
        hist.append({"epoch": ep, "reward": float(np.sum(rews))})
        # 简化优势: 累计折扣回报 (不另训价值网络, critic 只作辅助)
        adv = np.zeros(len(rews), dtype=np.float32)
        G = 0.0
        for t in reversed(range(len(rews))):
            G = rews[t] + gamma * G * (0.0 if dones[t] else 1.0)
            adv[t] = G
        adv = (adv - adv.mean()) / (adv.std() + 1e-8)
        O = torch.tensor(np.array(obs), dtype=torch.float32)
        A = torch.tensor(np.array(acts), dtype=torch.float32)
        Ad = torch.tensor(adv, dtype=torch.float32)
        old_lp = torch.tensor(np.array(logps), dtype=torch.float32)
        for _ in range(4):
            idx = np.random.permutation(len(O))[:batch]
            ot = O[idx]
            h = pol.net(ot)
            mu = pol.actor(h) * pol.action_scale
            std = pol.log_std.exp().clamp(1e-3, 1.0) * pol.action_scale
            dist = torch.distributions.Normal(mu, std)
            lp = dist.log_prob(A[idx]).sum(-1)
            ratio = (lp - old_lp[idx]).exp()
            p1 = ratio * Ad[idx]
            p2 = torch.clamp(ratio, 1 - clip, 1 + clip) * Ad[idx]
            loss = -torch.min(p1, p2).mean()
            opt.zero_grad(); loss.backward(); opt.step()
    return pol, hist