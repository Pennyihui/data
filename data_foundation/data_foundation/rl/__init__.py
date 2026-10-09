# -*- coding: utf-8 -*-
"""rl/ — 强化学习协议 (执行交易操作)

设计: docs/reinforcement-learning-design.md (v0.1)

核心: Env 的底层**复用回测引擎** (ExecutionEngine/Portfolio/RiskEngine/CostModel),
训练环境 = 回测 = 实盘 三者同路径; 奖励 = 成本后收益且默认缩尾; 评估复用三层指标。
"""
from .env import PortfolioEnv, TopKAction
from .trainer import PolicyNetwork, train_ppo

__all__ = ["PortfolioEnv", "TopKAction", "train_ppo", "PolicyNetwork"]