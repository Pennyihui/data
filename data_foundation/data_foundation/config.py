# -*- coding: utf-8 -*-
"""全局配置: 目录布局、MVP 资产清单、周期、venus。"""
import os

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

# 数据根目录
DATA_ROOT = os.path.join(PROJECT_ROOT, "data")
# L0 原始层 (不可变, 按批次+checksum)
RAW_DIR = os.path.join(DATA_ROOT, "raw")
# L1 标准化层 (Parquet)
L1_DIR = os.path.join(DATA_ROOT, "l1")
# L2 认证快照 (研究默认读取)
CERTIFIED_DIR = os.path.join(DATA_ROOT, "l2", "certified")
# 未认证/临时区 (未收盘等)
STAGING_DIR = os.path.join(DATA_ROOT, "l2", "staging")
# 统一 L0/L1/L2 三层的便捷根
BASE_DIR = DATA_ROOT
L0_DIR = RAW_DIR

# MVP 资产 (设计文档第七节) — 对应 Binance 永续/现货交易对 XUSDT
MVP_ASSETS = ["BTC", "ETH", "SOL", "BNB", "XRP", "ADA", "DOGE",
              "AVAX", "LINK", "LTC", "DOT", "UNI", "AAVE", "ARB", "POL"]

# 周期: 标准层保存 1h, 日/周由 1h 派生 (聚合规则见 l1.py)
INTERVALS = {"spot": ["1h"], "derivatives": ["1h"]}
DERIVED_INTERVALS = {"1d": "1D", "1w": "W-MON"}

# venue 定义
VENUES = {"binance": {"base": "https://api.binance.com",
                      "fapi": "https://fapi.binance.com",
                      "spot_exchange_info": "/api/v3/exchangeInfo",
                      "futures_exchange_info": "/fapi/v1/exchangeInfo"}}

# 外部已有数据 (L0 导入源): Data_pipeline 项目抓好的 CSV (data_foundation 位于其内)
_PIPE_ROOT = os.path.dirname(PROJECT_ROOT)
EXTERNAL_KLINE_DIR = os.path.join(_PIPE_ROOT, "data_new", "klines_1h")
EXTERNAL_MULTI_DIR = os.path.join(_PIPE_ROOT, "data_new", "additional", "multi_symbol")

# 时区与时间戳约定
TIMEZONE = "UTC"
TIMESTAMP_UNIT = "us"          # L1/L2 统一微秒 (Parquet timestamp[us, tz=UTC])
SOURCE_TIMESTAMP_UNITS = {"binance_spot": "ms", "binance_futures": "ms"}

# 质量规则版本
QUALITY_RULE_VERSION = "1.0.0"
SCHEMA_VERSION = "1.0.0"

# 认证: K 线是否已收盘的判定期限 (1h 周期: 收盘后 10 分钟视为最终)
CLOSE_CONFIRM_MINUTES = 10
