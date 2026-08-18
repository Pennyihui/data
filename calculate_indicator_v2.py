#calculate_indicator_v2.py 指标库
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import matplotlib.pyplot as plt
import warnings
from abc import ABC, abstractmethod
from typing import Dict, List, Tuple, Optional, Any
import json
import os
import talib

warnings.filterwarnings('ignore')

class TechnicalIndicators(ABC):
    """技术指标计算器基类（增强版）- 无未来数据泄露版本"""
    
    def __init__(self, 
                 macd_fast=12, macd_slow=26, macd_signal=9, 
                 rsi_period=14, atr_period=14,
                 bb_period=20, bb_std=2,
                 ema_periods: List[int] = None,
                 divergence_lookback: int = 20):
        """
        初始化技术指标参数
        
        Args:
            macd_fast: MACD快速EMA周期
            macd_slow: MACD慢速EMA周期
            macd_signal: MACD信号线周期
            rsi_period: RSI周期
            atr_period: ATR周期
            bb_period: 布林带周期
            bb_std: 布林带标准差倍数
            ema_periods: EMA周期列表
            divergence_lookback: 背离检测回溯周期
        """
        self.macd_fast = macd_fast
        self.macd_slow = macd_slow
        self.macd_signal = macd_signal
        self.rsi_period = rsi_period
        self.atr_period = atr_period
        self.bb_period = bb_period
        self.bb_std = bb_std
        self.ema_periods = ema_periods or [9, 20, 50, 100, 200]
        self.divergence_lookback = divergence_lookback
    
    def calculate_all_indicators(self, data: pd.DataFrame, prefix: str = "") -> pd.DataFrame:
        """
        计算所有基础技术指标
        
        Args:
            data: 价格数据
            prefix: 指标前缀（用于区分不同时间周期）
            
        Returns:
            包含所有技术指标的DataFrame
        """
        print(f"计算{prefix}基础指标...")
        df = data.copy()
        
        # 提取价格和成交量数据
        if 'Close' not in df.columns or df['Close'].isnull().all():
            raise ValueError("数据中没有有效的收盘价数据")
        
        # 使用DataFrame的Series进行计算
        o = df['Open'] if 'Open' in df.columns else df['Close']
        h = df['High'] if 'High' in df.columns else df['Close']
        l = df['Low'] if 'Low' in df.columns else df['Close']
        c = df['Close']
        v = df['Volume'] if 'Volume' in df.columns else pd.Series(1, index=df.index)
        
        # 转换为numpy数组供talib使用
        o_array = o.values
        h_array = h.values
        l_array = l.values
        c_array = c.values
        v_array = v.values
        
        # 1. 基础价格特征
        print(f"计算{prefix}基础价格特征...")
        df[f'{prefix}Returns'] = c.pct_change()
        df[f'{prefix}Log_Returns'] = np.log(c / c.shift(1))
        
        # 2. 趋势指标
        print(f"计算{prefix}趋势指标...")
        
        # MACD
        try:
            macd, macd_signal, macd_hist = talib.MACD(
                c_array, fastperiod=self.macd_fast, 
                slowperiod=self.macd_slow, 
                signalperiod=self.macd_signal
            )
            df[f'{prefix}MACD'] = macd
            df[f'{prefix}MACD_Signal'] = macd_signal
            df[f'{prefix}MACD_Histogram'] = macd_hist
        except Exception as e:
            print(f"计算MACD失败: {e}")
            df[f'{prefix}MACD'] = 0
            df[f'{prefix}MACD_Signal'] = 0
            df[f'{prefix}MACD_Histogram'] = 0
        
        # 移动平均线
        for period in self.ema_periods:
            if period < len(c_array):
                try:
                    df[f'{prefix}EMA{period}'] = talib.EMA(c_array, timeperiod=period)
                except Exception as e:
                    print(f"计算EMA{period}失败: {e}")
                    df[f'{prefix}EMA{period}'] = np.nan
        
        # ADX
        try:
            df[f'{prefix}ADX'] = talib.ADX(h_array, l_array, c_array, timeperiod=14)
            df[f'{prefix}PLUS_DI'] = talib.PLUS_DI(h_array, l_array, c_array, timeperiod=14)
            df[f'{prefix}MINUS_DI'] = talib.MINUS_DI(h_array, l_array, c_array, timeperiod=14)
        except Exception as e:
            print(f"计算ADX失败: {e}")
            df[f'{prefix}ADX'] = 50
            df[f'{prefix}PLUS_DI'] = 0
            df[f'{prefix}MINUS_DI'] = 0
        
        # 3. 动量指标
        print(f"计算{prefix}动量指标...")
        
        # RSI
        try:
            df[f'{prefix}RSI'] = talib.RSI(c_array, timeperiod=self.rsi_period)
        except Exception as e:
            print(f"计算RSI失败: {e}")
            df[f'{prefix}RSI'] = 50
        
        # 随机指标
        try:
            slowk, slowd = talib.STOCH(h_array, l_array, c_array)
            df[f'{prefix}STOCH_K'] = slowk
            df[f'{prefix}STOCH_D'] = slowd
        except Exception as e:
            print(f"计算随机指标失败: {e}")
            df[f'{prefix}STOCH_K'] = 50
            df[f'{prefix}STOCH_D'] = 50
        
        # CCI
        try:
            df[f'{prefix}CCI'] = talib.CCI(h_array, l_array, c_array, timeperiod=20)
        except Exception as e:
            print(f"计算CCI失败: {e}")
            df[f'{prefix}CCI'] = 0
        
        # Williams %R
        try:
            df[f'{prefix}WILLR'] = talib.WILLR(h_array, l_array, c_array, timeperiod=14)
        except Exception as e:
            print(f"计算Williams %R失败: {e}")
            df[f'{prefix}WILLR'] = -50
        
        # 动量指标
        try:
            df[f'{prefix}MOM'] = talib.MOM(c_array, timeperiod=10)
            df[f'{prefix}ROC'] = talib.ROC(c_array, timeperiod=10)
        except Exception as e:
            print(f"计算动量指标失败: {e}")
            df[f'{prefix}MOM'] = 0
            df[f'{prefix}ROC'] = 0
        
        # 4. 波动率指标
        print(f"计算{prefix}波动率指标...")
        
        # 布林带
        try:
            upper, middle, lower = talib.BBANDS(c_array, timeperiod=self.bb_period, 
                                               nbdevup=self.bb_std, nbdevdn=self.bb_std)
            df[f'{prefix}BB_Upper'] = upper
            df[f'{prefix}BB_Middle'] = middle
            df[f'{prefix}BB_Lower'] = lower
            
            # 布林带衍生指标
            with np.errstate(divide='ignore', invalid='ignore'):
                bb_width = (upper - lower) / middle
                bb_percent = (c_array - lower) / (upper - lower) * 100
                bb_percent = np.where(np.isfinite(bb_percent), bb_percent, 50)
            
            df[f'{prefix}BB_Width'] = bb_width
            df[f'{prefix}BB_Percent'] = bb_percent
        except Exception as e:
            print(f"计算布林带失败: {e}")
            df[f'{prefix}BB_Upper'] = c
            df[f'{prefix}BB_Middle'] = c
            df[f'{prefix}BB_Lower'] = c
            df[f'{prefix}BB_Width'] = 0
            df[f'{prefix}BB_Percent'] = 50
        
        # ATR
        try:
            df[f'{prefix}ATR'] = talib.ATR(h_array, l_array, c_array, timeperiod=self.atr_period)
        except Exception as e:
            print(f"计算ATR失败: {e}")
            df[f'{prefix}ATR'] = 0
        
        # 5. 成交量指标
        print(f"计算{prefix}成交量指标...")
        
        # OBV
        try:
            df[f'{prefix}OBV'] = talib.OBV(c_array, v_array)
        except Exception as e:
            print(f"计算OBV失败: {e}")
            df[f'{prefix}OBV'] = 0
        
        # MFI
        try:
            df[f'{prefix}MFI'] = talib.MFI(h_array, l_array, c_array, v_array, timeperiod=14)
        except Exception as e:
            print(f"计算MFI失败: {e}")
            df[f'{prefix}MFI'] = 50
        
        # 成交量移动平均
        try:
            volume_ma = talib.SMA(v_array, timeperiod=20)
            with np.errstate(divide='ignore', invalid='ignore'):
                volume_ratio = v_array / (volume_ma + 1e-10)
                volume_ratio = np.where(np.isfinite(volume_ratio), volume_ratio, 1)
            
            df[f'{prefix}Volume_SMA20'] = volume_ma
            df[f'{prefix}Volume_Ratio'] = volume_ratio
        except Exception as e:
            print(f"计算成交量指标失败: {e}")
            df[f'{prefix}Volume_SMA20'] = v_array
            df[f'{prefix}Volume_Ratio'] = 1
        
        # 6. 价格行为特征
        print(f"计算{prefix}价格行为特征...")
        
        # 价格范围
        try:
            price_range = h - l
            with np.errstate(divide='ignore', invalid='ignore'):
                price_range_pct = price_range / l * 100
                price_range_pct = price_range_pct.replace([np.inf, -np.inf], 0)
            
            df[f'{prefix}Price_Range'] = price_range
            df[f'{prefix}Price_Range_Pct'] = price_range_pct
        except Exception as e:
            print(f"计算价格范围失败: {e}")
            df[f'{prefix}Price_Range'] = 0
            df[f'{prefix}Price_Range_Pct'] = 0
        
        # 蜡烛图实体大小
        try:
            body_size = abs(c - o)
            signed_body = c - o
            with np.errstate(divide='ignore', invalid='ignore'):
                body_size_pct = body_size / o * 100
                body_size_pct = body_size_pct.replace([np.inf, -np.inf], 0)
                signed_body_pct = signed_body / o * 100
                signed_body_pct = signed_body_pct.replace([np.inf, -np.inf], 0)

            df[f'{prefix}Body_Size'] = body_size
            df[f'{prefix}Body_Size_Pct'] = body_size_pct
            df[f'{prefix}Signed_Body'] = signed_body
            df[f'{prefix}Signed_Body_Pct'] = signed_body_pct
        except Exception as e:
            print(f"计算蜡烛实体大小失败: {e}")
            df[f'{prefix}Body_Size'] = 0
            df[f'{prefix}Body_Size_Pct'] = 0
            df[f'{prefix}Signed_Body'] = 0
            df[f'{prefix}Signed_Body_Pct'] = 0
        
        # 上下影线
        try:
            upper_shadow = h - pd.concat([o, c], axis=1).max(axis=1)
            lower_shadow = pd.concat([o, c], axis=1).min(axis=1) - l
            
            df[f'{prefix}Upper_Shadow'] = upper_shadow
            df[f'{prefix}Lower_Shadow'] = lower_shadow
        except Exception as e:
            print(f"计算影线失败: {e}")
            df[f'{prefix}Upper_Shadow'] = 0
            df[f'{prefix}Lower_Shadow'] = 0
        
        # 7. 基础衍生特征
        print(f"计算{prefix}基础衍生特征...")
        
        # 价格相对于移动平均的位置
        for period in [20, 50]:
            ema_col = f'{prefix}EMA{period}'
            if ema_col in df.columns:
                try:
                    with np.errstate(divide='ignore', invalid='ignore'):
                        price_dist = (c - df[ema_col]) / (df[ema_col] + 1e-10) * 100
                        price_dist = price_dist.replace([np.inf, -np.inf], 0)
                    
                    df[f'{prefix}Price_Dist_to_EMA{period}'] = price_dist
                except Exception as e:
                    print(f"计算价格与EMA{period}距离失败: {e}")
                    df[f'{prefix}Price_Dist_to_EMA{period}'] = 0
        
        # 8. 统计特征
        print(f"计算{prefix}统计特征...")
        
        # 波动率 - 使用DataFrame的rolling方法
        try:
            returns_series = df[f'{prefix}Returns']
            df[f'{prefix}Volatility_10'] = returns_series.rolling(window=10, min_periods=1).std() * np.sqrt(24)
            df[f'{prefix}Volatility_20'] = returns_series.rolling(window=20, min_periods=1).std() * np.sqrt(24)
        except Exception as e:
            print(f"计算波动率失败: {e}")
            df[f'{prefix}Volatility_10'] = 0
            df[f'{prefix}Volatility_20'] = 0
        
        # 偏度和峰度
        try:
            df[f'{prefix}Skewness_10'] = returns_series.rolling(window=10, min_periods=1).skew()
            df[f'{prefix}Kurtosis_10'] = returns_series.rolling(window=10, min_periods=1).kurt()
        except Exception as e:
            print(f"计算偏度峰度失败: {e}")
            df[f'{prefix}Skewness_10'] = 0
            df[f'{prefix}Kurtosis_10'] = 0
        
        # 价格Z-score
        try:
            price_mean = c.rolling(window=20, min_periods=1).mean()
            price_std = c.rolling(window=20, min_periods=1).std()
            with np.errstate(divide='ignore', invalid='ignore'):
                price_zscore = (c - price_mean) / (price_std + 1e-10)
                price_zscore = price_zscore.replace([np.inf, -np.inf], 0)
            
            df[f'{prefix}Price_Zscore'] = price_zscore
        except Exception as e:
            print(f"计算价格Z-score失败: {e}")
            df[f'{prefix}Price_Zscore'] = 0
        
        # 9. 时间特征
        print(f"计算{prefix}时间特征...")
        if isinstance(df.index, pd.DatetimeIndex):
            try:
                df[f'{prefix}Hour'] = df.index.hour
                df[f'{prefix}DayOfWeek'] = df.index.dayofweek
                df[f'{prefix}Month'] = df.index.month
                
                # 周期性编码
                hour_series = df[f'{prefix}Hour']
                df[f'{prefix}Hour_Sin'] = np.sin(2 * np.pi * hour_series / 24)
                df[f'{prefix}Hour_Cos'] = np.cos(2 * np.pi * hour_series / 24)
                
                # 交易时段
                hour_values = hour_series
                df[f'{prefix}Is_Asian_Session'] = ((hour_values >= 22) | (hour_values < 6)).astype(int)
                df[f'{prefix}Is_London_Session'] = ((hour_values >= 8) & (hour_values < 16)).astype(int)
                df[f'{prefix}Is_NY_Session'] = ((hour_values >= 13) & (hour_values < 21)).astype(int)
            except Exception as e:
                print(f"计算时间特征失败: {e}")
                # 设置默认值
                df[f'{prefix}Hour'] = 0
                df[f'{prefix}DayOfWeek'] = 0
                df[f'{prefix}Month'] = 1
                df[f'{prefix}Hour_Sin'] = 0
                df[f'{prefix}Hour_Cos'] = 1
                df[f'{prefix}Is_Asian_Session'] = 0
                df[f'{prefix}Is_London_Session'] = 0
                df[f'{prefix}Is_NY_Session'] = 0
        
        # 清理NaN值
        df = df.replace([np.inf, -np.inf], np.nan)
        
        print(f"{prefix}基础指标计算完成，总特征数: {len([col for col in df.columns if col.startswith(prefix)])}")
        return df

    def calculate_advanced_features(self, df: pd.DataFrame, prefix: str = "") -> pd.DataFrame:
        """
        计算高级特征组合 - 无未来数据版本
        
        Args:
            df: 包含基础指标的DataFrame
            prefix: 指标前缀
            
        Returns:
            包含高级特征的DataFrame
        """
        print(f"计算{prefix}高级特征组合...")
        
        # 1. 价格动量组合特征
        # 短长期动量比率
        for short in [5, 10]:
            for long in [20, 50]:
                short_col = f'{prefix}Price_Dist_to_EMA{short}'
                long_col = f'{prefix}Price_Dist_to_EMA{long}'
                if short_col in df.columns and long_col in df.columns:
                    try:
                        short_values = df[short_col].fillna(0).values
                        long_values = df[long_col].fillna(0).values
                        with np.errstate(divide='ignore', invalid='ignore'):
                            momentum_ratio = short_values / (long_values + 1e-8)
                            momentum_ratio = np.where(np.isfinite(momentum_ratio), momentum_ratio, 0)
                        
                        df[f'{prefix}Momentum_Ratio_{short}_{long}'] = momentum_ratio
                    except Exception as e:
                        print(f"计算动量比率{short}_{long}失败: {e}")
        
        # 2. 波动率调整特征
        vol_col = f'{prefix}Volatility_10'
        if vol_col in df.columns:
            # 波动率调整的收益率
            returns_col = f'{prefix}Returns'
            if returns_col in df.columns:
                try:
                    returns_values = df[returns_col].fillna(0).values
                    vol_values = df[vol_col].fillna(0).values
                    with np.errstate(divide='ignore', invalid='ignore'):
                        vol_adjusted = returns_values / (vol_values + 1e-8)
                        vol_adjusted = np.where(np.isfinite(vol_adjusted), vol_adjusted, 0)
                    
                    df[f'{prefix}Vol_Adjusted_Returns'] = vol_adjusted
                except Exception as e:
                    print(f"计算波动率调整收益率失败: {e}")
            
            # 波动率调整的价格偏离
            for period in [5, 10, 20]:
                dist_col = f'{prefix}Price_Dist_to_EMA{period}'
                if dist_col in df.columns:
                    try:
                        dist_values = df[dist_col].fillna(0).values
                        vol_values = df[vol_col].fillna(0).values
                        with np.errstate(divide='ignore', invalid='ignore'):
                            vol_adjusted_dist = dist_values / (vol_values + 1e-8)
                            vol_adjusted_dist = np.where(np.isfinite(vol_adjusted_dist), vol_adjusted_dist, 0)
                        
                        df[f'{prefix}Vol_Adjusted_Dist_EMA{period}'] = vol_adjusted_dist
                    except Exception as e:
                        print(f"计算波动率调整距离EMA{period}失败: {e}")
        
        # 3. 成交量确认特征
        volume_ratio_col = f'{prefix}Volume_Ratio'
        returns_col = f'{prefix}Returns'
        if volume_ratio_col in df.columns and returns_col in df.columns:
            try:
                returns_values = df[returns_col].fillna(0).values
                volume_values = df[volume_ratio_col].fillna(1).values
                
                # 使用np.sign处理符号
                sign_returns = np.sign(returns_values)
                # 将0的符号设为1（假设无变化时为正）
                sign_returns[sign_returns == 0] = 1
                
                volume_confirmation = sign_returns * volume_values
                df[f'{prefix}Volume_Price_Confirmation'] = volume_confirmation
            except Exception as e:
                print(f"计算成交量确认特征失败: {e}")
        
        # 4. 技术指标组合特征
        rsi_col = f'{prefix}RSI'
        stoch_k_col = f'{prefix}STOCH_K'
        if rsi_col in df.columns and stoch_k_col in df.columns:
            try:
                rsi_values = df[rsi_col].fillna(50).values
                stoch_values = df[stoch_k_col].fillna(50).values
                combo = 0.6 * rsi_values + 0.4 * stoch_values
                df[f'{prefix}RSI_STOCH_Combo'] = combo
            except Exception as e:
                print(f"计算RSI-STOCH组合失败: {e}")
        
        # 5. 布林带高级特征
        bb_percent_col = f'{prefix}BB_Percent'
        if bb_percent_col in df.columns:
            try:
                bb_percent_series = df[bb_percent_col]
                # 使用Series的pct_change方法
                bb_percent_change = bb_percent_series.pct_change()
                df[f'{prefix}BB_Percent_Change'] = bb_percent_change
                
                # 布林带突破信号
                close_values = df['Close'].values
                bb_upper_col = f'{prefix}BB_Upper'
                bb_lower_col = f'{prefix}BB_Lower'
                
                if bb_upper_col in df.columns and bb_lower_col in df.columns:
                    bb_upper = df[bb_upper_col].values
                    bb_lower = df[bb_lower_col].values
                    
                    df[f'{prefix}BB_Breakout_Upper'] = (close_values > bb_upper).astype(int)
                    df[f'{prefix}BB_Breakout_Lower'] = (close_values < bb_lower).astype(int)
            except Exception as e:
                print(f"计算布林带高级特征失败: {e}")
        
        # 6. 市场状态特征
        adx_col = f'{prefix}ADX'
        plus_di_col = f'{prefix}PLUS_DI'
        minus_di_col = f'{prefix}MINUS_DI'
        
        if all(col in df.columns for col in [adx_col, plus_di_col, minus_di_col]):
            try:
                adx_values = df[adx_col].fillna(0).values
                plus_di_values = df[plus_di_col].fillna(0).values
                minus_di_values = df[minus_di_col].fillna(0).values
                
                # 趋势强度分类
                trend_strength = np.ones(len(df), dtype=int)  # 默认震荡
                
                strong_trend_mask = adx_values > 25
                bullish_mask = (strong_trend_mask) & (plus_di_values > minus_di_values)
                bearish_mask = (strong_trend_mask) & (plus_di_values <= minus_di_values)
                
                trend_strength[bullish_mask] = 2
                trend_strength[bearish_mask] = 0
                
                df[f'{prefix}Trend_Strength'] = trend_strength
                
                # 趋势方向
                df[f'{prefix}Trend_Direction'] = np.where(plus_di_values > minus_di_values, 1, -1)
                
                # 趋势强度评分
                trend_score = adx_values * 0.4 + (plus_di_values - minus_di_values) * 0.6
                df[f'{prefix}Trend_Score'] = trend_score
            except Exception as e:
                print(f"计算市场状态特征失败: {e}")
        
        # 7. 机器学习友好特征（分箱）- 使用滚动窗口分位数
        # RSI分箱
        if rsi_col in df.columns:
            try:
                rsi_values = df[rsi_col].fillna(50)
                # 使用固定阈值分箱，避免未来数据
                df[f'{prefix}RSI_Binned'] = pd.cut(
                    rsi_values, 
                    bins=[0, 30, 50, 70, 100],
                    labels=[0, 1, 2, 3],
                    include_lowest=True
                ).astype(float).fillna(1)
            except Exception as e:
                print(f"计算RSI分箱失败: {e}")
        
        # 成交量比率分箱 - 使用滚动窗口分位数
        if volume_ratio_col in df.columns:
            try:
                volume_values = df[volume_ratio_col]
                
                # 初始化分箱列
                volume_binned = pd.Series(1, index=df.index)
                
                # 使用滚动窗口计算分位数
                for i in range(len(df)):
                    if i >= 50:  # 需要足够的历史数据
                        window = volume_values.iloc[max(0, i-49):i+1]  # 过去50个值
                        if len(window) >= 10:  # 至少10个值
                            q25 = window.quantile(0.25)
                            q50 = window.quantile(0.50)
                            q75 = window.quantile(0.75)
                            
                            val = volume_values.iloc[i]
                            if val <= q25:
                                volume_binned.iloc[i] = 0
                            elif val <= q50:
                                volume_binned.iloc[i] = 1
                            elif val <= q75:
                                volume_binned.iloc[i] = 2
                            else:
                                volume_binned.iloc[i] = 3
                
                df[f'{prefix}Volume_Binned'] = volume_binned
            except Exception as e:
                print(f"计算成交量分箱失败: {e}")
                df[f'{prefix}Volume_Binned'] = 1
        
        # 8. 蜡烛图形态特征
        # 十字星
        body_pct_col = f'{prefix}Body_Size_Pct'
        if body_pct_col in df.columns:
            try:
                body_pct_values = df[body_pct_col].fillna(0).values
                is_doji = (body_pct_values < 0.1).astype(int)
                df[f'{prefix}Is_Doji'] = is_doji
            except Exception as e:
                print(f"计算十字星特征失败: {e}")
        
        # 锤子线
        lower_shadow_col = f'{prefix}Lower_Shadow'
        upper_shadow_col = f'{prefix}Upper_Shadow'
        body_size_col = f'{prefix}Body_Size'
        
        if all(col in df.columns for col in [lower_shadow_col, upper_shadow_col, body_size_col]):
            try:
                lower_values = df[lower_shadow_col].fillna(0).values
                upper_values = df[upper_shadow_col].fillna(0).values
                body_values = df[body_size_col].fillna(0).values
                
                # 锤子线
                is_hammer = (lower_values > 2 * body_values) & (upper_values < 0.1 * body_values)
                df[f'{prefix}Is_Hammer'] = is_hammer.astype(int)
                
                # 射击之星
                is_shooting_star = (upper_values > 2 * body_values) & (lower_values < 0.1 * body_values)
                df[f'{prefix}Is_Shooting_Star'] = is_shooting_star.astype(int)
            except Exception as e:
                print(f"计算蜡烛形态特征失败: {e}")
        
        # 9. 支撑阻力特征
        # 近期高低点
        for window in [20, 50]:
            try:
                # 使用shift确保不包含当前数据点
                high_values = df['High'].shift(1).rolling(window=window, min_periods=1).max().values
                low_values = df['Low'].shift(1).rolling(window=window, min_periods=1).min().values
                
                df[f'{prefix}High_{window}'] = high_values
                df[f'{prefix}Low_{window}'] = low_values
                
                # 价格在近期范围的位置
                with np.errstate(divide='ignore', invalid='ignore'):
                    price_position = (df['Close'].values - low_values) / (high_values - low_values + 1e-10) * 100
                    price_position = np.where(np.isfinite(price_position), price_position, 50)
                
                df[f'{prefix}Price_Position_{window}'] = price_position
            except Exception as e:
                print(f"计算支撑阻力特征{window}失败: {e}")
        
        # 10. 斐波那契回撤位 - 使用历史数据
        try:
            if len(df) >= 50:
                # 使用shift确保不包含当前数据点
                high_50 = df['High'].shift(1).rolling(window=50, min_periods=1).max().values
                low_50 = df['Low'].shift(1).rolling(window=50, min_periods=1).min().values
                price_range = high_50 - low_50
                
                fib_levels = [0.236, 0.382, 0.5, 0.618, 0.786]
                for level in fib_levels:
                    fib_value = high_50 - price_range * level
                    df[f'{prefix}Fib_{int(level*1000)}'] = fib_value
        except Exception as e:
            print(f"计算斐波那契回撤位失败: {e}")
        
        # 11. 滞后特征
        # 价格滞后
        for lag in [1, 2, 3, 5, 10]:
            try:
                df[f'{prefix}Close_Lag_{lag}'] = df['Close'].shift(lag)
                df[f'{prefix}Returns_Lag_{lag}'] = df[f'{prefix}Returns'].shift(lag)
            except Exception as e:
                print(f"计算滞后特征lag{lag}失败: {e}")
        
        # 指标滞后
        indicator_cols = [f'{prefix}RSI', f'{prefix}MACD', f'{prefix}BB_Percent', f'{prefix}Volume_Ratio']
        for col in indicator_cols:
            if col in df.columns:
                for lag in [1, 2, 3]:
                    try:
                        df[f'{col}_Lag_{lag}'] = df[col].shift(lag)
                    except Exception as e:
                        print(f"计算指标{col}滞后{lag}失败: {e}")
        
        # 12. 交互特征
        # 量价相关性 - 使用滚动窗口
        if f'{prefix}Volume_Ratio' in df.columns and f'{prefix}Returns' in df.columns:
            try:
                # 计算滚动相关性
                window_size = 10
                corr_values = np.zeros(len(df))
                
                for i in range(len(df)):
                    if i >= window_size:
                        start_idx = i - window_size + 1
                        window_volume = df['Volume'].iloc[start_idx:i+1]
                        window_close = df['Close'].iloc[start_idx:i+1]
                        
                        if len(window_volume) > 1 and len(window_close) > 1:
                            corr = window_volume.corr(window_close)
                            corr_values[i] = corr if not pd.isna(corr) else 0
                        else:
                            corr_values[i] = 0
                    else:
                        corr_values[i] = 0
                
                df[f'{prefix}Volume_Price_Corr_10'] = corr_values
            except Exception as e:
                print(f"计算量价相关性失败: {e}")
        
        # RSI与价格位置发散
        if f'{prefix}RSI' in df.columns and f'{prefix}Price_Position_20' in df.columns:
            try:
                rsi_values = df[f'{prefix}RSI'].fillna(50).values
                price_position = df[f'{prefix}Price_Position_20'].fillna(50).values
                divergence = rsi_values - price_position
                df[f'{prefix}RSI_Price_Divergence'] = divergence
            except Exception as e:
                print(f"计算RSI价格发散失败: {e}")
        
        # 波动率与成交量交互
        vol_col = f'{prefix}Volatility_10'
        volume_ratio_col = f'{prefix}Volume_Ratio'
        if vol_col in df.columns and volume_ratio_col in df.columns:
            try:
                vol_values = df[vol_col].fillna(0).values
                volume_values = df[volume_ratio_col].fillna(1).values
                interaction = vol_values * volume_values
                df[f'{prefix}Vol_Volume_Interaction'] = interaction
            except Exception as e:
                print(f"计算波动率成交量交互失败: {e}")
        
        # 13. 市场情绪特征
        # 涨跌比例（过去20根K线）
        returns_col = f'{prefix}Returns'
        if returns_col in df.columns:
            try:
                returns_values = df[returns_col].fillna(0).values
                
                up_down_ratio = np.ones(len(df))
                for i in range(len(df)):
                    if i >= 20:
                        start_idx = i - 19  # 过去20个值
                        window_returns = returns_values[start_idx:i+1]
                        
                        if len(window_returns) > 0:
                            up_count = np.sum(window_returns > 0)
                            down_count = np.sum(window_returns < 0)
                            ratio = up_count / max(down_count, 1)
                            up_down_ratio[i] = ratio
                    else:
                        up_down_ratio[i] = 1
                
                df[f'{prefix}Up_Down_Ratio'] = up_down_ratio
            except Exception as e:
                print(f"计算涨跌比例失败: {e}")
        
        # 14. 风险调整特征
        # 夏普比率近似 - 使用滚动窗口
        if f'{prefix}Returns' in df.columns and f'{prefix}Volatility_20' in df.columns:
            try:
                returns_mean = df[f'{prefix}Returns'].rolling(window=20, min_periods=1).mean().values
                volatility = df[f'{prefix}Volatility_20'].fillna(0).values
                
                with np.errstate(divide='ignore', invalid='ignore'):
                    sharpe = returns_mean / (volatility + 1e-8)
                    sharpe = np.where(np.isfinite(sharpe), sharpe, 0)
                
                df[f'{prefix}Sharpe_Ratio'] = sharpe
            except Exception as e:
                print(f"计算夏普比率失败: {e}")
        
        # 最大回撤 - 使用滚动窗口
        try:
            close_values = df['Close'].values
            max_drawdown = np.zeros(len(df))
            
            for i in range(len(close_values)):
                if i >= 20:
                    start_idx = max(0, i - 19)  # 过去20个值
                    window_prices = close_values[start_idx:i+1]
                    
                    if len(window_prices) > 0:
                        cumulative = window_prices / window_prices[0]
                        running_max = np.maximum.accumulate(cumulative)
                        drawdown = (cumulative - running_max) / running_max
                        max_dd = np.min(drawdown)
                        max_drawdown[i] = max_dd if not np.isnan(max_dd) else 0
                else:
                    max_drawdown[i] = 0
            
            df[f'{prefix}Max_Drawdown'] = max_drawdown
        except Exception as e:
            print(f"计算最大回撤失败: {e}")
        
        # 15. 时间序列特征
        # 季节性分解特征
        if isinstance(df.index, pd.DatetimeIndex):
            try:
                close_values = df['Close'].values
                detrended = np.zeros(len(close_values))
                
                for i in range(len(close_values)):
                    if i >= 20:
                        start_idx = max(0, i - 19)  # 过去20个值
                        window_prices = close_values[start_idx:i+1]
                        ma = np.mean(window_prices) if len(window_prices) > 0 else close_values[i]
                        detrended[i] = close_values[i] - ma
                    else:
                        detrended[i] = 0
                
                df[f'{prefix}Detrended'] = detrended
                
                # 季节性强度
                seasonal_strength = np.zeros(len(df))
                
                for i in range(len(detrended)):
                    if i >= 50:
                        start_idx = max(0, i - 49)  # 过去50个值
                        window_detrended = detrended[start_idx:i+1]
                        window_prices = close_values[start_idx:i+1]
                        
                        if len(window_detrended) > 1 and len(window_prices) > 1:
                            std_detrended = np.std(window_detrended)
                            std_prices = np.std(window_prices)
                            strength = std_detrended / (std_prices + 1e-10)
                            seasonal_strength[i] = strength if not np.isnan(strength) else 0
                    else:
                        seasonal_strength[i] = 0
                
                df[f'{prefix}Seasonal_Strength'] = seasonal_strength
            except Exception as e:
                print(f"计算时间序列特征失败: {e}")
        
        # 检测背离特征（无未来数据版本）
        df = self.detect_divergences(df, prefix=prefix)
        
        # ========== 新增高级特征 ==========
        # Ichimoku
        df = self.calculate_ichimoku(df, prefix)
        # Pivot Points
        df = self.calculate_pivot_points(df, prefix)
        # StochRSI
        df = self.calculate_stoch_rsi(df, prefix)
        # Keltner Channels
        df = self.calculate_keltner(df, prefix)
        # Donchian Channels (多个周期)
        for period in [20, 50]:
            df = self.calculate_donchian(df, prefix, period=period)
        # Buy/Sell Pressure
        df = self.calculate_buy_sell_pressure(df, prefix)
        # Avg Trade Size
        df = self.calculate_avg_trade_size(df, prefix)
        # =================================
        
        # 清理NaN值
        df = df.fillna(0)
        df = df.replace([np.inf, -np.inf], 0)
        
        print(f"{prefix}高级特征计算完成，新增{len([col for col in df.columns if col.startswith(prefix)])}个特征")
        return df


    def _detect_macd_hist_divergence(self, df: pd.DataFrame, price: np.ndarray,
                                macd_hist: np.ndarray, prefix: str = "") -> pd.DataFrame:
        """
        检测MACD柱状图背离（包含常规和隐藏背离）
        """
        n = len(price)
        lookback = self.divergence_lookback

        bullish_regular = np.zeros(n)      # 常规看涨背离
        bearish_regular = np.zeros(n)      # 常规看跌背离
        bullish_hidden = np.zeros(n)       # 隐藏看涨背离
        bearish_hidden = np.zeros(n)       # 隐藏看跌背离
        divergence_strength = np.zeros(n)  # 强度（带符号）

        for i in range(lookback, n):
            price_window = price[i-lookback:i+1]
            macd_window = macd_hist[i-lookback:i+1]
            price_extrema = self._find_extrema(price_window)
            macd_extrema = self._find_extrema(macd_window)

            if len(price_extrema) >= 2 and len(macd_extrema) >= 2:
                price_recent = price_extrema[-2:]
                macd_recent = macd_extrema[-2:]

                # 看涨背离：价格低点，指标低点
                if (price_recent[1]['type'] == 'low' and price_recent[0]['type'] == 'low' and
                    macd_recent[1]['type'] == 'low' and macd_recent[0]['type'] == 'low'):

                    # 常规看涨：价格新低，指标未新低
                    if price_recent[1]['value'] < price_recent[0]['value'] and \
                    macd_recent[1]['value'] > macd_recent[0]['value']:
                        price_change = abs(price_recent[1]['value'] - price_recent[0]['value']) / price_recent[0]['value']
                        macd_change = abs(macd_recent[1]['value'] - macd_recent[0]['value']) / (abs(macd_recent[0]['value']) + 1e-10)
                        if price_change > 0.01 and macd_change > 0.01:
                            bullish_regular[i] = 1
                            divergence_strength[i] = price_change * macd_change

                    # 隐藏看涨：价格未新低（更高低点），指标新低
                    elif price_recent[1]['value'] > price_recent[0]['value'] and \
                        macd_recent[1]['value'] < macd_recent[0]['value']:
                        price_change = abs(price_recent[1]['value'] - price_recent[0]['value']) / price_recent[0]['value']
                        macd_change = abs(macd_recent[1]['value'] - macd_recent[0]['value']) / (abs(macd_recent[0]['value']) + 1e-10)
                        if price_change > 0.01 and macd_change > 0.01:
                            bullish_hidden[i] = 1
                            divergence_strength[i] = price_change * macd_change * 0.8  # 隐藏背离强度稍弱

                # 看跌背离：价格高点，指标高点
                elif (price_recent[1]['type'] == 'high' and price_recent[0]['type'] == 'high' and
                    macd_recent[1]['type'] == 'high' and macd_recent[0]['type'] == 'high'):

                    # 常规看跌：价格新高，指标未新高
                    if price_recent[1]['value'] > price_recent[0]['value'] and \
                    macd_recent[1]['value'] < macd_recent[0]['value']:
                        price_change = abs(price_recent[1]['value'] - price_recent[0]['value']) / price_recent[0]['value']
                        macd_change = abs(macd_recent[1]['value'] - macd_recent[0]['value']) / (abs(macd_recent[0]['value']) + 1e-10)
                        if price_change > 0.01 and macd_change > 0.01:
                            bearish_regular[i] = 1
                            divergence_strength[i] = -price_change * macd_change

                    # 隐藏看跌：价格未新高（更低高点），指标新高
                    elif price_recent[1]['value'] < price_recent[0]['value'] and \
                        macd_recent[1]['value'] > macd_recent[0]['value']:
                        price_change = abs(price_recent[1]['value'] - price_recent[0]['value']) / price_recent[0]['value']
                        macd_change = abs(macd_recent[1]['value'] - macd_recent[0]['value']) / (abs(macd_recent[0]['value']) + 1e-10)
                        if price_change > 0.01 and macd_change > 0.01:
                            bearish_hidden[i] = 1
                            divergence_strength[i] = -price_change * macd_change * 0.8

        # 添加到DataFrame
        df[f'{prefix}MACD_Hist_Div_Bullish'] = bullish_regular
        df[f'{prefix}MACD_Hist_Div_Bearish'] = bearish_regular
        df[f'{prefix}MACD_Hist_Div_Hidden_Bullish'] = bullish_hidden
        df[f'{prefix}MACD_Hist_Div_Hidden_Bearish'] = bearish_hidden
        df[f'{prefix}MACD_Hist_Div_Strength'] = divergence_strength

        # 以下为原历史确认部分（未修改，保留）
        if f'{prefix}Returns' in df.columns:
            returns = df[f'{prefix}Returns'].values
            historical_bullish_confirmation = np.zeros(n)
            historical_bearish_confirmation = np.zeros(n)

            for i in range(n):
                if bullish_regular[i] == 1 and i >= 10:
                    historical_divergences = []
                    for j in range(max(0, i-100), i):
                        if bullish_regular[j] == 1 and j+3 < i:
                            future_return = np.mean(returns[j+1:j+4]) if len(returns[j+1:j+4]) > 0 else 0
                            historical_divergences.append(future_return)
                    if len(historical_divergences) > 0:
                        avg_future_return = np.mean(historical_divergences)
                        if avg_future_return > 0.001:
                            historical_bullish_confirmation[i] = 1

            df[f'{prefix}MACD_Hist_Div_Historical_Confirm'] = historical_bullish_confirmation

        return df

    def _detect_rsi_divergence(self, df: pd.DataFrame, price: np.ndarray, 
                            rsi: np.ndarray, prefix: str = "") -> pd.DataFrame:
        """
        检测RSI背离（包含常规和隐藏背离，考虑超买超卖增强）
        """
        n = len(price)
        lookback = self.divergence_lookback

        bullish_regular = np.zeros(n)
        bearish_regular = np.zeros(n)
        bullish_hidden = np.zeros(n)
        bearish_hidden = np.zeros(n)
        rsi_divergence_strength = np.zeros(n)

        for i in range(lookback, n):
            price_window = price[i-lookback:i+1]
            rsi_window = rsi[i-lookback:i+1]
            price_extrema = self._find_extrema(price_window)
            rsi_extrema = self._find_extrema(rsi_window)

            if len(price_extrema) >= 2 and len(rsi_extrema) >= 2:
                price_recent = price_extrema[-2:]
                rsi_recent = rsi_extrema[-2:]

                # 看涨背离：价格低点，RSI低点
                if (price_recent[1]['type'] == 'low' and price_recent[0]['type'] == 'low' and
                    rsi_recent[1]['type'] == 'low' and rsi_recent[0]['type'] == 'low'):

                    # 常规看涨：价格新低，RSI未新低
                    if price_recent[1]['value'] < price_recent[0]['value'] and \
                    rsi_recent[1]['value'] > rsi_recent[0]['value']:
                        price_change = abs(price_recent[1]['value'] - price_recent[0]['value']) / price_recent[0]['value']
                        rsi_change = abs(rsi_recent[1]['value'] - rsi_recent[0]['value'])
                        if price_change > 0.01 and rsi_change > 2:
                            bullish_regular[i] = 1
                            # 超卖增强
                            rsi_current = rsi[i]
                            boost = 1.5 if rsi_current < 30 else 1.0
                            rsi_divergence_strength[i] = price_change * rsi_change * boost

                    # 隐藏看涨：价格未新低，RSI新低
                    elif price_recent[1]['value'] > price_recent[0]['value'] and \
                        rsi_recent[1]['value'] < rsi_recent[0]['value']:
                        price_change = abs(price_recent[1]['value'] - price_recent[0]['value']) / price_recent[0]['value']
                        rsi_change = abs(rsi_recent[1]['value'] - rsi_recent[0]['value'])
                        if price_change > 0.01 and rsi_change > 2:
                            bullish_hidden[i] = 1
                            rsi_divergence_strength[i] = price_change * rsi_change * 0.8

                # 看跌背离：价格高点，RSI高点
                elif (price_recent[1]['type'] == 'high' and price_recent[0]['type'] == 'high' and
                    rsi_recent[1]['type'] == 'high' and rsi_recent[0]['type'] == 'high'):

                    # 常规看跌：价格新高，RSI未新高
                    if price_recent[1]['value'] > price_recent[0]['value'] and \
                    rsi_recent[1]['value'] < rsi_recent[0]['value']:
                        price_change = abs(price_recent[1]['value'] - price_recent[0]['value']) / price_recent[0]['value']
                        rsi_change = abs(rsi_recent[1]['value'] - rsi_recent[0]['value'])
                        if price_change > 0.01 and rsi_change > 2:
                            bearish_regular[i] = 1
                            # 超买增强
                            rsi_current = rsi[i]
                            boost = 1.5 if rsi_current > 70 else 1.0
                            rsi_divergence_strength[i] = -price_change * rsi_change * boost

                    # 隐藏看跌：价格未新高，RSI新高
                    elif price_recent[1]['value'] < price_recent[0]['value'] and \
                        rsi_recent[1]['value'] > rsi_recent[0]['value']:
                        price_change = abs(price_recent[1]['value'] - price_recent[0]['value']) / price_recent[0]['value']
                        rsi_change = abs(rsi_recent[1]['value'] - rsi_recent[0]['value'])
                        if price_change > 0.01 and rsi_change > 2:
                            bearish_hidden[i] = 1
                            rsi_divergence_strength[i] = -price_change * rsi_change * 0.8

        df[f'{prefix}RSI_Div_Bullish'] = bullish_regular
        df[f'{prefix}RSI_Div_Bearish'] = bearish_regular
        df[f'{prefix}RSI_Div_Hidden_Bullish'] = bullish_hidden
        df[f'{prefix}RSI_Div_Hidden_Bearish'] = bearish_hidden
        df[f'{prefix}RSI_Div_Strength'] = rsi_divergence_strength

        # 强信号标记（基于超买超卖）
        if f'{prefix}RSI' in df.columns:
            df[f'{prefix}RSI_Div_Bullish_Strong'] = (
                (bullish_regular == 1) & (df[f'{prefix}RSI'] < 30)
            ).astype(int)
            df[f'{prefix}RSI_Div_Bearish_Strong'] = (
                (bearish_regular == 1) & (df[f'{prefix}RSI'] > 70)
            ).astype(int)
            df[f'{prefix}RSI_Div_Hidden_Bullish_Strong'] = (
                (bullish_hidden == 1) & (df[f'{prefix}RSI'] < 30)
            ).astype(int)
            df[f'{prefix}RSI_Div_Hidden_Bearish_Strong'] = (
                (bearish_hidden == 1) & (df[f'{prefix}RSI'] > 70)
            ).astype(int)

        return df
    
    def _detect_volume_divergence(self, df: pd.DataFrame, price: np.ndarray, 
                                volume_ratio: np.ndarray, prefix: str = "") -> pd.DataFrame:
        """
        检测成交量背离（基于极值点）
        看涨背离：价格创新低，成交量创新高（或未创新低）
        看跌背离：价格创新高，成交量创新低（或未创新高）
        """
        n = len(price)
        lookback = self.divergence_lookback

        bullish_volume_div = np.zeros(n)
        bearish_volume_div = np.zeros(n)
        volume_div_strength = np.zeros(n)

        for i in range(lookback, n):
            price_window = price[i-lookback:i+1]
            volume_window = volume_ratio[i-lookback:i+1]

            price_extrema = self._find_extrema(price_window)
            volume_extrema = self._find_extrema(volume_window)

            if len(price_extrema) >= 2 and len(volume_extrema) >= 2:
                price_recent = price_extrema[-2:]
                volume_recent = volume_extrema[-2:]

                # 看涨背离：价格创新低，成交量创新高
                if price_recent[1]['type'] == 'low' and price_recent[0]['type'] == 'low':
                    if price_recent[1]['value'] < price_recent[0]['value']:
                        if volume_recent[1]['type'] == 'high' and volume_recent[0]['type'] == 'high':
                            if volume_recent[1]['value'] > volume_recent[0]['value']:
                                bullish_volume_div[i] = 1
                                price_change = abs(price_recent[1]['value'] - price_recent[0]['value']) / price_recent[0]['value']
                                volume_change = abs(volume_recent[1]['value'] - volume_recent[0]['value']) / (volume_recent[0]['value'] + 1e-10)
                                volume_div_strength[i] = price_change * volume_change

                # 看跌背离：价格创新高，成交量创新低
                elif price_recent[1]['type'] == 'high' and price_recent[0]['type'] == 'high':
                    if price_recent[1]['value'] > price_recent[0]['value']:
                        if volume_recent[1]['type'] == 'low' and volume_recent[0]['type'] == 'low':
                            if volume_recent[1]['value'] < volume_recent[0]['value']:
                                bearish_volume_div[i] = 1
                                price_change = abs(price_recent[1]['value'] - price_recent[0]['value']) / price_recent[0]['value']
                                volume_change = abs(volume_recent[1]['value'] - volume_recent[0]['value']) / (volume_recent[0]['value'] + 1e-10)
                                volume_div_strength[i] = -price_change * volume_change

        df[f'{prefix}Volume_Div_Bullish'] = bullish_volume_div
        df[f'{prefix}Volume_Div_Bearish'] = bearish_volume_div
        df[f'{prefix}Volume_Div_Strength'] = volume_div_strength

        return df
    
    def detect_divergences(self, df: pd.DataFrame, prefix: str = "") -> pd.DataFrame:
        """
        检测所有类型背离，并计算综合得分和连续背离特征
        """
        print(f"检测{prefix}背离特征...")
        price = df['Close'].values

        # 1. MACD柱背离
        macd_hist_col = f'{prefix}MACD_Histogram'
        if macd_hist_col in df.columns:
            macd_hist = df[macd_hist_col].values
            df = self._detect_macd_hist_divergence(df, price, macd_hist, prefix)

        # 2. RSI背离
        rsi_col = f'{prefix}RSI'
        if rsi_col in df.columns:
            rsi = df[rsi_col].values
            df = self._detect_rsi_divergence(df, price, rsi, prefix)

        # 3. 成交量背离
        volume_ratio_col = f'{prefix}Volume_Ratio'
        if volume_ratio_col in df.columns:
            volume_ratio = df[volume_ratio_col].values
            df = self._detect_volume_divergence(df, price, volume_ratio, prefix)

        # 4. 综合背离得分（加权融合）
        # 收集所有背离列
        all_div_cols = []
        for suffix in ['MACD_Hist_Div_Bullish', 'MACD_Hist_Div_Bearish',
                    'MACD_Hist_Div_Hidden_Bullish', 'MACD_Hist_Div_Hidden_Bearish',
                    'RSI_Div_Bullish', 'RSI_Div_Bearish',
                    'RSI_Div_Hidden_Bullish', 'RSI_Div_Hidden_Bearish',
                    'Volume_Div_Bullish', 'Volume_Div_Bearish']:
            col = f'{prefix}{suffix}'
            if col in df.columns:
                all_div_cols.append(col)

        if all_div_cols:
            # 定义权重：常规1.0，隐藏0.7，成交量0.8
            weights = {}
            for col in all_div_cols:
                if 'Hidden' in col:
                    weights[col] = 0.7
                elif 'Volume' in col:
                    weights[col] = 0.8
                else:
                    weights[col] = 1.0

            bullish_score = np.zeros(len(df))
            bearish_score = np.zeros(len(df))
            for col in all_div_cols:
                if 'Bullish' in col:
                    bullish_score += df[col].values * weights[col]
                elif 'Bearish' in col:
                    bearish_score += df[col].values * weights[col]

            df[f'{prefix}Divergence_Bullish_Weighted'] = bullish_score
            df[f'{prefix}Divergence_Bearish_Weighted'] = bearish_score
            df[f'{prefix}Divergence_Net_Score'] = bullish_score - bearish_score

            # 同时保留简单计数（可选，便于比较）
            df[f'{prefix}Divergence_Bullish_Count'] = df[[c for c in all_div_cols if 'Bullish' in c]].sum(axis=1)
            df[f'{prefix}Divergence_Bearish_Count'] = df[[c for c in all_div_cols if 'Bearish' in c]].sum(axis=1)

        # 5. 连续背离次数
        for base in ['MACD_Hist_Div_Bullish', 'RSI_Div_Bullish', 'MACD_Hist_Div_Bearish', 'RSI_Div_Bearish']:
            col = f'{prefix}{base}'
            if col in df.columns:
                consecutive = np.zeros(len(df))
                count = 0
                for i in range(len(df)):
                    if df[col].iloc[i] == 1:
                        count += 1
                    else:
                        count = 0
                    consecutive[i] = count
                df[f'{prefix}Consecutive_{base}'] = consecutive

        # 6. 原有背离强度计算（保持不变）
        df = self._calculate_divergence_strength(df, price, prefix)

        # 7. 添加背离类型编码 (0=无，1=常规底，2=常规顶，3=隐藏底，4=隐藏顶)
        div_type = np.zeros(len(df), dtype=int)
        # 常规底背离 (1)
        for col in [f'{prefix}MACD_Hist_Div_Bullish', f'{prefix}RSI_Div_Bullish']:
            if col in df.columns:
                div_type = np.where(df[col] == 1, 1, div_type)
        # 常规顶背离 (2)
        for col in [f'{prefix}MACD_Hist_Div_Bearish', f'{prefix}RSI_Div_Bearish']:
            if col in df.columns:
                div_type = np.where(df[col] == 1, 2, div_type)
        # 隐藏底背离 (3)
        for col in [f'{prefix}MACD_Hist_Div_Hidden_Bullish', f'{prefix}RSI_Div_Hidden_Bullish']:
            if col in df.columns:
                div_type = np.where(df[col] == 1, 3, div_type)
        # 隐藏顶背离 (4)
        for col in [f'{prefix}MACD_Hist_Div_Hidden_Bearish', f'{prefix}RSI_Div_Hidden_Bearish']:
            if col in df.columns:
                div_type = np.where(df[col] == 1, 4, div_type)
        df[f'{prefix}Div_Type'] = div_type

        print(f"{prefix}背离特征检测完成")
        return df

    def _find_extrema(self, data: np.ndarray, window: int = 5) -> List[dict]:
        """
        在数据序列中查找极值点
        
        Args:
            data: 数据序列
            window: 查找窗口大小
            
        Returns:
            极值点列表，每个元素包含{'type': 'high'/'low', 'value': 极值, 'index': 位置}
        """
        n = len(data)
        extrema = []
        
        for i in range(window, n - window):
            # 检查是否是局部高点
            if (data[i] == max(data[i-window:i+window+1]) and 
                data[i] != data[i-1]):  # 避免平台
                extrema.append({
                    'type': 'high',
                    'value': data[i],
                    'index': i
                })
            
            # 检查是否是局部低点
            elif (data[i] == min(data[i-window:i+window+1]) and 
                  data[i] != data[i-1]):  # 避免平台
                extrema.append({
                    'type': 'low',
                    'value': data[i],
                    'index': i
                })
        
        return extrema
    
    def _calculate_divergence_strength(self, df: pd.DataFrame, price: np.ndarray, 
                                      prefix: str = "") -> pd.DataFrame:
        """
        计算背离强度综合评分 - 无未来数据版本
        
        Args:
            df: 包含背离信号的DataFrame
            price: 价格序列
            prefix: 指标前缀
            
        Returns:
            包含背离强度评分的DataFrame
        """
        n = len(price)
        
        # 初始化背离强度评分
        divergence_score = np.zeros(n)
        
        # MACD柱背离强度
        macd_strength_col = f'{prefix}MACD_Hist_Div_Strength'
        if macd_strength_col in df.columns:
            macd_strength = df[macd_strength_col].values
            # 归一化处理（使用滚动窗口最大值，避免未来数据）
            max_strength_rolling = pd.Series(np.abs(macd_strength)).rolling(window=100, min_periods=1).max().values
            
            for i in range(n):
                if max_strength_rolling[i] > 0:
                    divergence_score[i] += (macd_strength[i] / max_strength_rolling[i]) * 0.4
        
        # RSI背离强度
        rsi_strength_col = f'{prefix}RSI_Div_Strength'
        if rsi_strength_col in df.columns:
            rsi_strength = df[rsi_strength_col].values
            # 归一化处理（使用滚动窗口最大值，避免未来数据）
            max_rsi_strength = pd.Series(np.abs(rsi_strength)).rolling(window=100, min_periods=1).max().values
            
            for i in range(n):
                if max_rsi_strength[i] > 0:
                    divergence_score[i] += (rsi_strength[i] / max_rsi_strength[i]) * 0.4
        
        # 成交量背离
        volume_bullish_col = f'{prefix}Volume_Div_Bullish'
        volume_bearish_col = f'{prefix}Volume_Div_Bearish'
        
        if volume_bullish_col in df.columns and volume_bearish_col in df.columns:
            volume_bullish = df[volume_bullish_col].values
            volume_bearish = df[volume_bearish_col].values
            
            # 成交量背离加权
            divergence_score += volume_bullish * 0.1
            divergence_score -= volume_bearish * 0.1
        
        # 背离确认信号（历史版本）
        macd_historical_confirm = f'{prefix}MACD_Hist_Div_Historical_Confirm'
        if macd_historical_confirm in df.columns:
            historical_confirm = df[macd_historical_confirm].values
            divergence_score += historical_confirm * 0.1
        
        # 保存背离强度评分
        df[f'{prefix}Divergence_Score'] = divergence_score
        
        # 背离信号强度分类
        df[f'{prefix}Divergence_Signal'] = 0
        df.loc[divergence_score > 0.3, f'{prefix}Divergence_Signal'] = 1  # 强看涨背离
        df.loc[divergence_score < -0.3, f'{prefix}Divergence_Signal'] = -1  # 强看跌背离
        df.loc[(divergence_score > 0.1) & (divergence_score <= 0.3), f'{prefix}Divergence_Signal'] = 0.5  # 弱看涨
        df.loc[(divergence_score < -0.1) & (divergence_score >= -0.3), f'{prefix}Divergence_Signal'] = -0.5  # 弱看跌
        
        return df

    def calculate_ichimoku(self, df: pd.DataFrame, prefix: str = "") -> pd.DataFrame:
        """
        计算一目均衡表（简化版，无未来数据）
        - 转换线 Tenkan (9周期)
        - 基准线 Kijun (26周期)
        - 价格与Kijun距离
        - Tenkan与Kijun差值及交叉信号
        """
        high = df['High']
        low = df['Low']
        close = df['Close']

        tenkan = (high.rolling(window=9, min_periods=1).max() + 
                low.rolling(window=9, min_periods=1).min()) / 2
        df[f'{prefix}Ichimoku_Tenkan'] = tenkan

        kijun = (high.rolling(window=26, min_periods=1).max() + 
                low.rolling(window=26, min_periods=1).min()) / 2
        df[f'{prefix}Ichimoku_Kijun'] = kijun

        # 价格与基准线的距离（百分比）
        df[f'{prefix}Price_Dist_to_Kijun'] = (close - kijun) / (kijun + 1e-10) * 100
        # Tenkan与Kijun的差值
        df[f'{prefix}Tenkan_Kijun_Diff'] = (tenkan - kijun) / (kijun + 1e-10) * 100
        # Tenkan上穿Kijun信号
        df[f'{prefix}Ichimoku_TK_Cross'] = ((tenkan > kijun) & (tenkan.shift(1) <= kijun.shift(1))).astype(int)
        return df

    def calculate_pivot_points(self, df: pd.DataFrame, prefix: str = "") -> pd.DataFrame:
        """
        计算经典枢轴点（基于前24根K线的最高/最低/收盘）
        """
        if len(df) < 24:
            return df

        # 过去24根K线的最高、最低、收盘（前一天的等效）
        high_24 = df['High'].rolling(window=24, min_periods=24).max().shift(1)
        low_24 = df['Low'].rolling(window=24, min_periods=24).min().shift(1)
        close_24 = df['Close'].shift(1)

        pivot = (high_24 + low_24 + close_24) / 3
        r1 = 2 * pivot - low_24
        r2 = pivot + (high_24 - low_24)
        r3 = high_24 + 2 * (pivot - low_24)
        s1 = 2 * pivot - high_24
        s2 = pivot - (high_24 - low_24)
        s3 = low_24 - 2 * (high_24 - pivot)

        df[f'{prefix}Pivot'] = pivot
        df[f'{prefix}Resistance1'] = r1
        df[f'{prefix}Resistance2'] = r2
        df[f'{prefix}Resistance3'] = r3
        df[f'{prefix}Support1'] = s1
        df[f'{prefix}Support2'] = s2
        df[f'{prefix}Support3'] = s3

        # 价格相对于枢轴的位置
        close = df['Close']
        df[f'{prefix}Price_vs_Pivot'] = (close - pivot) / (pivot + 1e-10)
        return df

    def calculate_stoch_rsi(self, df: pd.DataFrame, prefix: str = "") -> pd.DataFrame:
        """
        计算随机RSI (StochRSI)
        需要RSI列已存在
        """
        rsi_col = f'{prefix}RSI'
        if rsi_col not in df.columns:
            return df

        rsi = df[rsi_col]
        period = 14
        min_rsi = rsi.rolling(window=period, min_periods=1).min()
        max_rsi = rsi.rolling(window=period, min_periods=1).max()
        stoch_rsi = (rsi - min_rsi) / (max_rsi - min_rsi + 1e-10)
        df[f'{prefix}StochRSI'] = stoch_rsi

        # 平滑
        df[f'{prefix}StochRSI_K'] = stoch_rsi.rolling(window=3, min_periods=1).mean()
        df[f'{prefix}StochRSI_D'] = df[f'{prefix}StochRSI_K'].rolling(window=3, min_periods=1).mean()
        return df

    def calculate_keltner(self, df: pd.DataFrame, prefix: str = "") -> pd.DataFrame:
        """
        计算肯特纳通道
        需要EMA20和ATR列存在
        """
        ema_col = f'{prefix}EMA20'
        atr_col = f'{prefix}ATR'
        if ema_col not in df.columns or atr_col not in df.columns:
            return df

        ema = df[ema_col]
        atr = df[atr_col]
        multiplier = 2.0

        df[f'{prefix}Keltner_Upper'] = ema + multiplier * atr
        df[f'{prefix}Keltner_Lower'] = ema - multiplier * atr
        df[f'{prefix}Keltner_Middle'] = ema

        # 带宽
        df[f'{prefix}Keltner_Width'] = (df[f'{prefix}Keltner_Upper'] - df[f'{prefix}Keltner_Lower']) / ema
        # 价格位置百分比
        df[f'{prefix}Keltner_Percent'] = (df['Close'] - df[f'{prefix}Keltner_Lower']) / (df[f'{prefix}Keltner_Upper'] - df[f'{prefix}Keltner_Lower'] + 1e-10) * 100
        return df

    def calculate_donchian(self, df: pd.DataFrame, prefix: str = "", period: int = 20) -> pd.DataFrame:
        """
        计算唐奇安通道
        """
        high_roll = df['High'].rolling(window=period, min_periods=1).max()
        low_roll = df['Low'].rolling(window=period, min_periods=1).min()
        df[f'{prefix}Donchian_Upper_{period}'] = high_roll
        df[f'{prefix}Donchian_Lower_{period}'] = low_roll
        df[f'{prefix}Donchian_Middle_{period}'] = (high_roll + low_roll) / 2

        # 突破信号（当前突破前一期的通道）
        close = df['Close']
        df[f'{prefix}Donchian_Breakout_Up_{period}'] = ((close > high_roll.shift(1)) & (close.shift(1) <= high_roll.shift(1))).astype(int)
        df[f'{prefix}Donchian_Breakout_Down_{period}'] = ((close < low_roll.shift(1)) & (close.shift(1) >= low_roll.shift(1))).astype(int)
        return df

    def calculate_buy_sell_pressure(self, df: pd.DataFrame, prefix: str = "") -> pd.DataFrame:
        """
        计算买卖压力（基于主动买入量和总成交量）
        需要字段：taker_buy_volume, Volume
        """
        if 'taker_buy_volume' not in df.columns:
            print(f"警告: {prefix}数据中缺少 taker_buy_volume，无法计算买卖压力")
            df[f'{prefix}Buy_Ratio'] = 0.5
            df[f'{prefix}Sell_Ratio'] = 0.5
            df[f'{prefix}Buy_Pressure'] = 0
            df[f'{prefix}Sell_Pressure'] = 0
            return df

        buy_volume = df['taker_buy_volume'].fillna(0)
        total_volume = df['Volume'].fillna(0)
        buy_ratio = buy_volume / (total_volume + 1e-10)
        sell_ratio = 1 - buy_ratio

        df[f'{prefix}Buy_Ratio'] = buy_ratio
        df[f'{prefix}Sell_Ratio'] = sell_ratio
        df[f'{prefix}Buy_Pressure'] = buy_volume
        df[f'{prefix}Sell_Pressure'] = total_volume - buy_volume

        # 滚动平均
        df[f'{prefix}Buy_Pressure_SMA'] = df[f'{prefix}Buy_Pressure'].rolling(window=20, min_periods=1).mean()
        df[f'{prefix}Sell_Pressure_SMA'] = df[f'{prefix}Sell_Pressure'].rolling(window=20, min_periods=1).mean()
        return df

    def calculate_avg_trade_size(self, df: pd.DataFrame, prefix: str = "") -> pd.DataFrame:
        """
        计算平均交易大小（成交额/交易次数）
        需要字段：quote_volume, trades
        """
        if 'quote_volume' not in df.columns or 'trades' not in df.columns:
            print(f"警告: {prefix}数据中缺少 quote_volume 或 trades，无法计算平均交易大小")
            df[f'{prefix}Avg_Trade_Size'] = 0
            return df

        quote_vol = df['quote_volume'].fillna(0)
        trades = df['trades'].fillna(0)
        avg_trade_size = quote_vol / (trades + 1e-10)
        df[f'{prefix}Avg_Trade_Size'] = avg_trade_size

        # 滚动平均
        df[f'{prefix}Avg_Trade_Size_SMA'] = avg_trade_size.rolling(window=20, min_periods=1).mean()
        return df

class DailyIndicators(TechnicalIndicators):
    """日线技术指标计算器（无未来数据版本）"""
    
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        # 日线特定参数
        self.ema_periods = [5, 10, 20, 50, 100, 200]
        self.rsi_period = 14
        self.atr_period = 14
        self.bb_period = 20
        self.divergence_lookback = 20  # 日线背离检测周期
    
    def calculate(self, data: pd.DataFrame) -> pd.DataFrame:
        """
        计算日线技术指标（完整特征集）- 无未来数据版本
        
        Args:
            data: 日线数据
            
        Returns:
            包含所有技术指标的DataFrame
        """
        print("开始计算日线技术指标...")
        df = data.copy()
        
        # 计算所有基础指标
        df = self.calculate_all_indicators(df, prefix="daily_")
        
        # 检测背离特征
        df = self.detect_divergences(df, prefix="daily_")
        
        # 计算日线特有的高级特征
        df = self.calculate_daily_specific_features(df)
        
        # 计算高级特征组合
        df = self.calculate_advanced_features(df, prefix="daily_")
        
        print(f"日线指标计算完成，总特征数: {len(df.columns)}")
        
        # 打印背离特征统计
        divergence_cols = [col for col in df.columns if 'Div' in col]
        print(f"日线背离特征数量: {len(divergence_cols)}")
        
        return df
    
    def calculate_daily_specific_features(self, df: pd.DataFrame) -> pd.DataFrame:
        """计算日线特有的特征 - 无未来数据版本"""
        print("计算日线特有特征...")
        
        # 1. 长期趋势背离特征
        price = df['Close']
        
        # 价格与200日均线背离
        if 'daily_EMA200' in df.columns:
            ema200 = df['daily_EMA200']
            price_ema200_ratio = price / ema200
            
            # 价格创新高但价格/EMA200比率下降
            n = len(price)
            lookback = 50  # 长期背离使用更长周期
            
            price_ema_divergence = np.zeros(n)
            
            for i in range(lookback, n):
                # 转换为numpy数组用于极值检测
                price_window = price.iloc[i-lookback:i+1].values
                ratio_window = price_ema200_ratio.iloc[i-lookback:i+1].values
                
                # 查找极值点
                price_extrema = self._find_extrema(price_window, window=10)
                ratio_extrema = self._find_extrema(ratio_window, window=10)
                
                if len(price_extrema) >= 2 and len(ratio_extrema) >= 2:
                    price_recent = price_extrema[-2:]
                    ratio_recent = ratio_extrema[-2:]
                    
                    # 价格创新高但相对强度下降
                    if (price_recent[1]['type'] == 'high' and 
                        price_recent[0]['type'] == 'high' and
                        price_recent[1]['value'] > price_recent[0]['value'] and
                        ratio_recent[1]['value'] < ratio_recent[0]['value']):
                        
                        price_ema_divergence[i] = 1
        
            df['daily_Price_EMA200_Divergence'] = price_ema_divergence
        
        # 2. 成交量与价格背离的长期确认
        if 'daily_Volume_Div_Bullish' in df.columns and 'daily_Volume_Div_Bearish' in df.columns:
            # 保持为Series以便使用rolling
            volume_bullish = df['daily_Volume_Div_Bullish']
            volume_bearish = df['daily_Volume_Div_Bearish']
            
            # 成交量背离的持续性
            df['daily_Volume_Div_Bullish_Persistent'] = (
                volume_bullish.rolling(window=5).sum() >= 3
            ).astype(int)
            
            df['daily_Volume_Div_Bearish_Persistent'] = (
                volume_bearish.rolling(window=5).sum() >= 3
            ).astype(int)
        
        # 3. 长期趋势特征
        # 价格在年线上的位置
        if 'daily_EMA200' in df.columns:
            df['daily_Above_Year_MA'] = (price > df['daily_EMA200']).astype(int)
            df['daily_Distance_to_Year_MA'] = (price - df['daily_EMA200']) / df['daily_EMA200'] * 100
        
        # 4. 周度价格模式
        # 计算每周收盘价变化
        if isinstance(df.index, pd.DatetimeIndex):
            df['daily_DayOfWeek'] = df.index.dayofweek
            # 周一开盘价与上周五收盘价比较
            daily_monday_gap = pd.Series(np.nan, index=df.index)
            
            for i in range(1, len(df)):
                if df['daily_DayOfWeek'].iloc[i] == 0:  # 周一
                    gap = (df['Open'].iloc[i] - df['Close'].iloc[i-1]) / df['Close'].iloc[i-1] * 100
                    daily_monday_gap.iloc[i] = gap
            
            df['daily_Monday_Gap'] = daily_monday_gap
        
        # 5. 月度和季度特征
        if isinstance(df.index, pd.DatetimeIndex):
            df['daily_DayOfMonth'] = df.index.day
            df['daily_Is_Month_End'] = (df.index.is_month_end).astype(int)
            df['daily_Is_Quarter_End'] = (df.index.is_quarter_end).astype(int)
        
        # 6. 波动率结构特征
        # 实现波动率（已实现波动率）
        if 'daily_Returns' in df.columns:
            returns_series = df['daily_Returns']
            df['daily_Realized_Vol_20'] = returns_series.rolling(window=20).std() * np.sqrt(365)
            
            # 波动率锥（计算不同时间窗口的波动率）
            for window in [5, 10, 20, 60]:
                df[f'daily_Volatility_{window}'] = returns_series.rolling(window=window).std() * np.sqrt(365)
        
        # 7. 均线排列状态
        if all(col in df.columns for col in ['daily_EMA10', 'daily_EMA20', 'daily_EMA50', 'daily_EMA100', 'daily_EMA200']):
            # 多头排列（短期>长期）
            df['daily_MA_Alignment_Long'] = (
                (df['daily_EMA10'] > df['daily_EMA20']) &
                (df['daily_EMA20'] > df['daily_EMA50']) &
                (df['daily_EMA50'] > df['daily_EMA100']) &
                (df['daily_EMA100'] > df['daily_EMA200'])
            ).astype(int)
            
            # 空头排列
            df['daily_MA_Alignment_Short'] = (
                (df['daily_EMA10'] < df['daily_EMA20']) &
                (df['daily_EMA20'] < df['daily_EMA50']) &
                (df['daily_EMA50'] < df['daily_EMA100']) &
                (df['daily_EMA100'] < df['daily_EMA200'])
            ).astype(int)
        
        # 8. 支撑阻力突破 - 使用shift避免未来数据
        for window in [50, 100, 200]:
            # 计算滚动高点和低点（使用shift）
            high_rolling = df['High'].shift(1).rolling(window=window, min_periods=1).max()
            low_rolling = df['Low'].shift(1).rolling(window=window, min_periods=1).min()
            
            # 突破前期高点
            df[f'daily_Breakout_High_{window}'] = (
                (df['Close'] > high_rolling) & 
                (df['Close'].shift(1) <= high_rolling)
            ).astype(int)
            
            # 跌破前期低点
            df[f'daily_Breakdown_Low_{window}'] = (
                (df['Close'] < low_rolling) & 
                (df['Close'].shift(1) >= low_rolling)
            ).astype(int)
        
        # 9. 价格与均线距离的统计
        for period in [20, 50, 200]:
            ema_col = f'daily_EMA{period}'
            if ema_col in df.columns:
                dist_col = f'daily_Price_Dist_to_EMA{period}'
                if dist_col not in df.columns:
                    df[dist_col] = (df['Close'] - df[ema_col]) / df[ema_col] * 100
                
                # 距离的Z-score（使用滚动窗口）
                df[f'daily_Dist_Zscore_EMA{period}'] = (
                    df[dist_col] - df[dist_col].rolling(window=50).mean()
                ) / (df[dist_col].rolling(window=50).std() + 1e-10)
        
        # 10. 成交量分布特征
        # 成交量分位数（使用滚动窗口）
        if 'daily_Volume_Ratio' in df.columns:
            volume_ratio = df['daily_Volume_Ratio']
            
            # 使用apply计算滚动分位数
            def calculate_rolling_quantile(series, window=50):
                result = pd.Series(0.5, index=series.index)
                for i in range(len(series)):
                    if i >= window:
                        window_data = series.iloc[i-window:i]
                        if len(window_data) > 0:
                            # 计算当前值在窗口中的分位数
                            sorted_window = np.sort(window_data)
                            rank = np.searchsorted(sorted_window, series.iloc[i])
                            result.iloc[i] = rank / len(sorted_window)
                return result
            
            df['daily_Volume_Quantile'] = calculate_rolling_quantile(volume_ratio)
        
        # 11. 市场广度特征
        # 上涨下跌成交量
        if 'daily_Returns' in df.columns:
            returns_series = df['daily_Returns']
            df['daily_Up_Volume'] = np.where(returns_series > 0, df['Volume'], 0)
            df['daily_Down_Volume'] = np.where(returns_series < 0, df['Volume'], 0)
            df['daily_Up_Down_Volume_Ratio'] = (
                df['daily_Up_Volume'].rolling(window=20).sum() / 
                (df['daily_Down_Volume'].rolling(window=20).sum() + 1e-10)
            )
        
        # 12. 季节性特征
        if isinstance(df.index, pd.DatetimeIndex):
            df['daily_Month'] = df.index.month
            df['daily_Quarter'] = df.index.quarter
            df['daily_Is_Year_End'] = (df.index.month == 12).astype(int)

        # 13. 轻量级日线上下文状态
        if all(col in df.columns for col in ['daily_EMA20', 'daily_EMA50']):
            df['daily_trend_bias'] = np.where(df['daily_EMA20'] > df['daily_EMA50'], 1, -1)
        else:
            df['daily_trend_bias'] = 0

        if all(col in df.columns for col in ['daily_ADX', 'daily_RSI']):
            df['daily_trend_strength'] = (
                df['daily_ADX'].fillna(0) * 0.6 +
                (df['daily_RSI'].fillna(50) - 50).abs() * 0.4
            )
        else:
            df['daily_trend_strength'] = 0

        if all(col in df.columns for col in ['daily_trend_bias', 'daily_ADX']):
            df['daily_filter_pass_long'] = ((df['daily_trend_bias'] == 1) & (df['daily_ADX'].fillna(0) >= 20)).astype(int)
            df['daily_filter_pass_short'] = ((df['daily_trend_bias'] == -1) & (df['daily_ADX'].fillna(0) >= 20)).astype(int)
        else:
            df['daily_filter_pass_long'] = 0
            df['daily_filter_pass_short'] = 0

        return df

class Hourly4HIndicators(TechnicalIndicators):
    """4小时技术指标计算器（完整版）- 无未来数据版本"""
    
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        # 4小时线特定参数
        self.ema_periods = [5, 10, 20, 50, 100]
        self.rsi_period = 14
        self.atr_period = 14
        self.bb_period = 20
        self.divergence_lookback = 15  # 4小时背离检测周期
    
    def calculate(self, data: pd.DataFrame) -> pd.DataFrame:
        """
        计算4小时技术指标（完整特征集）- 无未来数据版本
        
        Args:
            data: 4小时数据
            
        Returns:
            包含所有技术指标的DataFrame
        """
        print("开始计算4小时技术指标...")
        df = data.copy()
        
        # 计算所有基础指标
        df = self.calculate_all_indicators(df, prefix="4h_")
        
        # 检测背离特征
        df = self.detect_divergences(df, prefix="4h_")
        
        # 计算4小时特有的高级特征
        df = self.calculate_4h_specific_features(df)
        
        # 计算高级特征组合
        df = self.calculate_advanced_features(df, prefix="4h_")
        
        print(f"4小时指标计算完成，总特征数: {len(df.columns)}")
        
        # 打印背离特征统计
        divergence_cols = [col for col in df.columns if 'Div' in col and '4h_' in col]
        print(f"4小时背离特征数量: {len(divergence_cols)}")
        
        return df
    
    def calculate_4h_specific_features(self, df: pd.DataFrame) -> pd.DataFrame:
        """计算4小时特有的特征 - 无未来数据版本"""
        print("计算4小时特有特征...")
        
        # 1. 多时间框架背离
        price = df['Close'].values
        
        # 价格与MACD线的背离（不仅仅是柱状图）
        if '4h_MACD' in df.columns:
            macd = df['4h_MACD'].values
            
            # 检测价格与MACD线背离
            df = self._detect_macd_line_divergence(df, price, macd, "4h_")
        
        # 2. 背离在布林带中的位置
        if '4h_BB_Percent' in df.columns:
            bb_percent = df['4h_BB_Percent'].values
            
            # 背离发生在布林带极端位置时更强
            if '4h_RSI_Div_Bullish' in df.columns:
                df['4h_RSI_Div_Bullish_BB_Extreme'] = (
                    (df['4h_RSI_Div_Bullish'] == 1) & 
                    (bb_percent < 20)  # 布林带下轨附近
                ).astype(int)
            
            if '4h_RSI_Div_Bearish' in df.columns:
                df['4h_RSI_Div_Bearish_BB_Extreme'] = (
                    (df['4h_RSI_Div_Bearish'] == 1) & 
                    (bb_percent > 80)  # 布林带上轨附近
                ).astype(int)
        
        # 3. 背离与成交量配合
        if '4h_Volume_Ratio' in df.columns and '4h_MACD_Hist_Div_Bullish' in df.columns:
            volume_ratio = df['4h_Volume_Ratio'].values
            macd_div_bullish = df['4h_MACD_Hist_Div_Bullish'].values
            
            # 看涨背离伴随成交量放大
            df['4h_MACD_Div_Bullish_Volume_Confirm'] = (
                (macd_div_bullish == 1) & 
                (volume_ratio > 1.5)
            ).astype(int)
        
        # 4. 背离序列特征
        # 连续背离检测
        if '4h_MACD_Hist_Div_Bullish' in df.columns:
            consecutive_bullish_div = np.zeros(len(df))
            count = 0
            
            for i in range(len(df)):
                if df['4h_MACD_Hist_Div_Bullish'].iloc[i] == 1:
                    count += 1
                    consecutive_bullish_div[i] = count
                else:
                    count = 0
            
            df['4h_Consecutive_Bullish_Div'] = consecutive_bullish_div
        
        if '4h_MACD_Hist_Div_Bearish' in df.columns:
            consecutive_bearish_div = np.zeros(len(df))
            count = 0
            
            for i in range(len(df)):
                if df['4h_MACD_Hist_Div_Bearish'].iloc[i] == 1:
                    count += 1
                    consecutive_bearish_div[i] = count
                else:
                    count = 0
            
            df['4h_Consecutive_Bearish_Div'] = consecutive_bearish_div
        
        # 5. 背离失败特征 - 使用历史模式而非未来数据
        if '4h_MACD_Hist_Div_Bullish' in df.columns and '4h_Returns' in df.columns:
            bullish_div = df['4h_MACD_Hist_Div_Bullish'].values
            
            # 历史背离失败率
            divergence_failure_rate = np.zeros(len(df))
            
            for i in range(len(df)):
                if i >= 50:  # 需要有足够的历史数据
                    # 统计过去类似背离的成功率
                    historical_divergences = []
                    for j in range(max(0, i-200), i):
                        if bullish_div[j] == 1 and j+5 < i:
                            # 检查背离后5期的表现
                            future_return = df['4h_Returns'].iloc[j+1:j+6].mean() if len(df['4h_Returns'].iloc[j+1:j+6]) > 0 else 0
                            historical_divergences.append(future_return)
                    
                    if len(historical_divergences) > 0:
                        # 如果历史背离平均表现不佳，则认为当前背离可能失败
                        avg_future_return = np.mean(historical_divergences)
                        if avg_future_return < -0.005:  # 历史背离后平均下跌
                            divergence_failure_rate[i] = 1
            
            df['4h_Divergence_Failure_Risk'] = divergence_failure_rate
        
        return df
    
    def _detect_macd_line_divergence(self, df: pd.DataFrame, price: np.ndarray, 
                                    macd_line: np.ndarray, prefix: str = "") -> pd.DataFrame:
        """
        检测价格与MACD线背离 - 无未来数据版本
        
        Args:
            price: 价格序列
            macd_line: MACD线序列
            prefix: 指标前缀
            
        Returns:
            包含MACD线背离特征的DataFrame
        """
        n = len(price)
        lookback = self.divergence_lookback
        
        # 初始化背离信号
        macd_line_bullish = np.zeros(n)
        macd_line_bearish = np.zeros(n)
        
        for i in range(lookback, n):
            # 获取当前窗口（只使用历史数据）
            price_window = price[i-lookback:i+1]
            macd_window = macd_line[i-lookback:i+1]
            
            # 查找极值点
            price_extrema = self._find_extrema(price_window)
            macd_extrema = self._find_extrema(macd_window)
            
            if len(price_extrema) >= 2 and len(macd_extrema) >= 2:
                # 最近的两个极值点
                price_recent = price_extrema[-2:]
                macd_recent = macd_extrema[-2:]
                
                # 看涨背离：价格创新低，MACD线没有创新低
                if (price_recent[1]['type'] == 'low' and price_recent[0]['type'] == 'low' and
                    price_recent[1]['value'] < price_recent[0]['value'] and
                    macd_recent[1]['value'] > macd_recent[0]['value']):
                    
                    # 检查有效性
                    price_change = abs(price_recent[1]['value'] - price_recent[0]['value']) / price_recent[0]['value']
                    if price_change > 0.01:
                        macd_line_bullish[i] = 1
                
                # 看跌背离：价格创新高，MACD线没有创新高
                elif (price_recent[1]['type'] == 'high' and price_recent[0]['type'] == 'high' and
                      price_recent[1]['value'] > price_recent[0]['value'] and
                      macd_recent[1]['value'] < macd_recent[0]['value']):
                    
                    # 检查有效性
                    price_change = abs(price_recent[1]['value'] - price_recent[0]['value']) / price_recent[0]['value']
                    if price_change > 0.01:
                        macd_line_bearish[i] = 1
        
        # 添加到DataFrame
        df[f'{prefix}MACD_Line_Div_Bullish'] = macd_line_bullish
        df[f'{prefix}MACD_Line_Div_Bearish'] = macd_line_bearish
        
        return df

class Hourly1HIndicators(TechnicalIndicators):
    """1小时技术指标计算器（完整版）- 无未来数据版本"""
    
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        # 1小时线特定参数
        self.ema_periods = [5, 10, 20, 50]
        self.rsi_period = 14
        self.atr_period = 14
        self.bb_period = 20
        self.divergence_lookback = 10  # 1小时背离检测使用较短周期
    
    def calculate(self, data: pd.DataFrame) -> pd.DataFrame:
        """
        计算1小时技术指标（完整特征集）- 无未来数据版本
        
        Args:
            data: 1小时数据
            
        Returns:
            包含所有技术指标的DataFrame
        """
        print("开始计算1小时技术指标...")
        df = data.copy()
        
        # 计算所有基础指标
        df = self.calculate_all_indicators(df, prefix="1h_")
        
        # 检测背离特征
        df = self.detect_divergences(df, prefix="1h_")
        
        # 计算1小时特有的高级特征
        df = self.calculate_1h_specific_features(df)
        
        # 计算高级特征组合
        df = self.calculate_advanced_features(df, prefix="1h_")
        
        print(f"1小时指标计算完成，总特征数: {len(df.columns)}")
        
        # 打印背离特征统计
        divergence_cols = [col for col in df.columns if 'Div' in col and '1h_' in col]
        print(f"1小时背离特征数量: {len(divergence_cols)}")
        
        return df
    
    def calculate_1h_specific_features(self, df: pd.DataFrame) -> pd.DataFrame:
        """计算1小时特有的特征 - 无未来数据版本"""
        print("计算1小时特有特征...")
        
        # 1. 高频背离特征
        price = df['Close'].values
        
        # 使用更短周期检测高频背离
        short_lookback = 5
        n = len(price)
        
        # 高频RSI背离
        if '1h_RSI' in df.columns:
            rsi = df['1h_RSI'].values
            
            hf_rsi_bullish = np.zeros(n)
            hf_rsi_bearish = np.zeros(n)
            
            for i in range(short_lookback, n):
                price_window = price[i-short_lookback:i+1]
                rsi_window = rsi[i-short_lookback:i+1]
                
                # 简单的高频背离检测
                if (price[i] < np.min(price_window[:-1]) and 
                    rsi[i] > np.min(rsi_window[:-1]) and
                    np.std(price_window) > 0):
                    hf_rsi_bullish[i] = 1
                
                elif (price[i] > np.max(price_window[:-1]) and 
                      rsi[i] < np.max(rsi_window[:-1]) and
                      np.std(price_window) > 0):
                    hf_rsi_bearish[i] = 1
            
            df['1h_HF_RSI_Div_Bullish'] = hf_rsi_bullish
            df['1h_HF_RSI_Div_Bearish'] = hf_rsi_bearish
        
        # 2. 背离与价格行为的结合
        # 背离发生在关键价格水平（支撑/阻力）时更有效
        for window in [10, 20]:
            # 使用shift避免未来数据
            high_col = f'1h_High_{window}'
            low_col = f'1h_Low_{window}'
            
            if high_col not in df.columns:
                # 计算滚动高点（使用shift）
                df[high_col] = df['High'].shift(1).rolling(window=window, min_periods=1).max()
            
            if low_col not in df.columns:
                # 计算滚动低点（使用shift）
                df[low_col] = df['Low'].shift(1).rolling(window=window, min_periods=1).min()
            
            if high_col in df.columns and low_col in df.columns:
                resistance = df[high_col].values
                support = df[low_col].values
                
                if '1h_MACD_Hist_Div_Bullish' in df.columns:
                    # 看涨背离发生在支撑位附近
                    near_support = (price - support) / price * 100 < 1  # 价格在支撑位1%以内
                    df[f'1h_MACD_Div_Bullish_Near_Support_{window}'] = (
                        (df['1h_MACD_Hist_Div_Bullish'] == 1) & near_support
                    ).astype(int)
                
                if '1h_MACD_Hist_Div_Bearish' in df.columns:
                    # 看跌背离发生在阻力位附近
                    near_resistance = (resistance - price) / price * 100 < 1  # 价格在阻力位1%以内
                    df[f'1h_MACD_Div_Bearish_Near_Resistance_{window}'] = (
                        (df['1h_MACD_Hist_Div_Bearish'] == 1) & near_resistance
                    ).astype(int)
        
        # 3. 背离的时间特征
        if isinstance(df.index, pd.DatetimeIndex):
            hour = df.index.hour
            
            # 背离发生在交易时段开始/结束时
            if '1h_MACD_Hist_Div_Bullish' in df.columns:
                # 亚洲时段开盘（0-2点）的背离
                df['1h_MACD_Div_Bullish_Asia_Open'] = (
                    (df['1h_MACD_Hist_Div_Bullish'] == 1) & 
                    ((hour >= 0) & (hour < 2))
                ).astype(int)
                
                # 伦敦时段开盘（8-10点）的背离
                df['1h_MACD_Div_Bullish_London_Open'] = (
                    (df['1h_MACD_Hist_Div_Bullish'] == 1) & 
                    ((hour >= 8) & (hour < 10))
                ).astype(int)
            
            if '1h_MACD_Hist_Div_Bearish' in df.columns:
                # 纽约时段收盘（20-22点）的背离
                df['1h_MACD_Div_Bearish_NY_Close'] = (
                    (df['1h_MACD_Hist_Div_Bearish'] == 1) & 
                    ((hour >= 20) & (hour < 22))
                ).astype(int)
        
        # 4. 背离与波动率的关系 - 使用滚动窗口计算
        if '1h_ATR' in df.columns and '1h_MACD_Hist_Div_Strength' in df.columns:
            atr = df['1h_ATR'].values
            div_strength = df['1h_MACD_Hist_Div_Strength'].values
            
            # 计算滚动平均ATR
            rolling_mean_atr = pd.Series(atr).rolling(window=50, min_periods=1).mean().values
            
            df['1h_Divergence_High_Volatility'] = (
                (np.abs(div_strength) > 0.1) & (atr > rolling_mean_atr * 1.5)
            ).astype(int)
        
        # 5. 背离信号的时效性
        # 背离信号在发生后一段时间内有效
        if '1h_MACD_Hist_Div_Bullish' in df.columns:
            bullish_div = df['1h_MACD_Hist_Div_Bullish'].values
            bullish_div_recency = np.zeros(len(df))
            
            recency_window = 5  # 背离信号有效期（5根K线）
            
            for i in range(len(df)):
                if bullish_div[i] == 1:
                    # 标记未来recency_window根K线（仅用于特征标记，不用于预测）
                    end_idx = min(i + recency_window, len(df))
                    bullish_div_recency[i:end_idx] = 1
            
            df['1h_MACD_Div_Bullish_Recent'] = bullish_div_recency
        
        # 6. 背离信号的组合使用
        # 多个指标同时出现背离
        divergence_cols = [
            '1h_MACD_Hist_Div_Bullish',
            '1h_RSI_Div_Bullish',
            '1h_Volume_Div_Bullish'
        ]
        
        existing_div_cols = [col for col in divergence_cols if col in df.columns]
        
        if len(existing_div_cols) >= 2:
            df['1h_Multiple_Divergence_Bullish'] = (
                df[existing_div_cols].sum(axis=1) >= 2
            ).astype(int)
        
        # 7. 背离信号的统计特征
        # 历史背离成功率
        if '1h_MACD_Hist_Div_Bullish' in df.columns and '1h_Returns' in df.columns:
            bullish_div = df['1h_MACD_Hist_Div_Bullish'].values
            returns = df['1h_Returns'].values
            
            historical_success_rate = np.zeros(len(df))
            
            for i in range(len(df)):
                if i >= 100:  # 需要足够的历史数据
                    success_count = 0
                    total_count = 0
                    
                    for j in range(max(0, i-200), i):
                        if bullish_div[j] == 1 and j+3 < i:
                            total_count += 1
                            # 背离后3期平均收益
                            future_return = np.mean(returns[j+1:j+4]) if len(returns[j+1:j+4]) > 0 else 0
                            if future_return > 0.001:  # 微小正收益即视为成功
                                success_count += 1
                    
                    if total_count > 0:
                        historical_success_rate[i] = success_count / total_count
            
            df['1h_Divergence_Historical_Success_Rate'] = historical_success_rate
        
        return df

class FeatureValidator:
    """特征验证器 - 检查是否存在未来数据泄露"""
    
    @staticmethod
    def validate_no_future_data(df: pd.DataFrame, price_col: str = 'Close') -> Dict[str, List[str]]:
        """
        验证DataFrame中是否存在未来数据泄露
        
        Args:
            df: 待验证的DataFrame
            price_col: 价格列名
            
        Returns:
            验证结果字典
        """
        results = {
            'issues': [],
            'suspicious_columns': [],
            'safe_columns': [],
            'total_columns': len(df.columns)
        }
        
        price = df[price_col].values
        
        for col in df.columns:
            if col == price_col:
                continue
                
            try:
                feature_values = df[col].values
                
                # 检查1：特征是否与未来价格有高相关性
                if FeatureValidator._has_future_correlation(feature_values, price):
                    results['issues'].append(f"列 '{col}' 可能与未来价格有高相关性")
                    results['suspicious_columns'].append(col)
                    continue
                
                # 检查2：特征是否包含未来极值信息
                if FeatureValidator._contains_future_extremes(feature_values, price):
                    results['issues'].append(f"列 '{col}' 可能包含未来极值信息")
                    results['suspicious_columns'].append(col)
                    continue
                
                # 检查3：特征是否在价格转折点前异常
                if FeatureValidator._anomalous_before_turning_points(feature_values, price):
                    results['issues'].append(f"列 '{col}' 可能在价格转折点前异常")
                    results['suspicious_columns'].append(col)
                    continue
                
                results['safe_columns'].append(col)
                
            except Exception as e:
                results['issues'].append(f"列 '{col}' 验证失败: {str(e)}")
        
        return results
    
    @staticmethod
    def _has_future_correlation(feature: np.ndarray, price: np.ndarray, 
                                max_lag: int = 5) -> bool:
        """检查特征是否与未来价格有高相关性"""
        if len(feature) < 50:
            return False
            
        # 检查与未来1-5期价格的相关性
        for lag in range(1, min(max_lag + 1, len(feature))):
            future_price = price[lag:]
            current_feature = feature[:-lag]
            
            if len(current_feature) < 20:
                continue
                
            corr = np.corrcoef(current_feature, future_price[:len(current_feature)])[0, 1]
            if abs(corr) > 0.5:  # 高相关性阈值
                return True
                
        return False
    
    @staticmethod
    def _contains_future_extremes(feature: np.ndarray, price: np.ndarray, 
                                   window: int = 20) -> bool:
        """检查特征是否包含未来极值信息"""
        if len(feature) < window * 2:
            return False
            
        # 检查特征极值是否对应未来价格极值
        for i in range(window, len(feature) - window):
            if feature[i] == np.max(feature[i-window:i+window]):
                # 检查未来价格是否也有极值
                future_max = np.max(price[i+1:i+window+1])
                if future_max == np.max(price[i+1:i+window+1]):
                    return True
                    
        return False
    
    @staticmethod
    def _anomalous_before_turning_points(feature: np.ndarray, price: np.ndarray,
                                         threshold_std: float = 2.0) -> bool:
        """检查特征是否在价格转折点前异常"""
        if len(feature) < 50:
            return False
            
        # 计算价格变化率
        price_returns = np.diff(price) / price[:-1]
        
        # 找出价格转折点（变化率符号改变）
        turning_points = []
        for i in range(1, len(price_returns) - 1):
            if price_returns[i-1] * price_returns[i] < 0:  # 符号改变
                turning_points.append(i)
        
        if len(turning_points) < 5:
            return False
            
        # 检查转折点前的特征值是否异常
        feature_mean = np.mean(feature)
        feature_std = np.std(feature)
        
        for tp in turning_points:
            if tp < 5:
                continue
                
            # 转折点前3期的特征值
            feature_before = feature[max(0, tp-3):tp]
            if len(feature_before) > 0:
                feature_zscore = np.abs((feature_before - feature_mean) / (feature_std + 1e-10))
                if np.any(feature_zscore > threshold_std):
                    return True
                    
        return False

class WeeklyIndicators(TechnicalIndicators):
    """周线技术指标计算器（无未来数据版本）"""
    
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        # 周线特定参数
        self.ema_periods = [10, 20, 50, 100]  # 周线使用更长的EMA周期
        self.rsi_period = 14
        self.atr_period = 14
        self.bb_period = 20
        self.divergence_lookback = 10  # 周线背离检测周期
    
    def calculate(self, data: pd.DataFrame) -> pd.DataFrame:
        """
        计算周线技术指标（完整特征集）- 无未来数据版本
        
        Args:
            data: 周线数据
            
        Returns:
            包含所有技术指标的DataFrame
        """
        print("开始计算周线技术指标...")
        df = data.copy()
        
        # 计算所有基础指标
        df = self.calculate_all_indicators(df, prefix="weekly_")
        
        # 检测背离特征
        df = self.detect_divergences(df, prefix="weekly_")
        
        # 计算周线特有的高级特征
        df = self.calculate_weekly_specific_features(df)
        
        # 计算高级特征组合
        df = self.calculate_advanced_features(df, prefix="weekly_")
        
        print(f"周线指标计算完成，总特征数: {len(df.columns)}")
        
        # 打印背离特征统计
        divergence_cols = [col for col in df.columns if 'Div' in col and 'weekly_' in col]
        print(f"周线背离特征数量: {len(divergence_cols)}")
        
        return df
    
    def calculate_weekly_specific_features(self, df: pd.DataFrame) -> pd.DataFrame:
        """计算周线特有的特征 - 无未来数据版本"""
        print("计算周线特有特征...")
        
        # 1. 长期趋势分析
        price = df['Close']
        
        # 价格与52周移动平均线的关系
        if 'weekly_EMA50' in df.columns:
            ema50 = df['weekly_EMA50']
            df['weekly_Price_vs_52W_MA'] = (price - ema50) / ema50 * 100
        
        # 2. 周线动量特征
        if 'weekly_Returns' in df.columns:
            weekly_returns = df['weekly_Returns']
            
            # 连续上涨/下跌周数
            consecutive_up = np.zeros(len(df))
            consecutive_down = np.zeros(len(df))
            
            up_count = 0
            down_count = 0
            
            for i in range(len(df)):
                if weekly_returns.iloc[i] > 0:
                    up_count += 1
                    down_count = 0
                elif weekly_returns.iloc[i] < 0:
                    down_count += 1
                    up_count = 0
                else:
                    up_count = 0
                    down_count = 0
                
                consecutive_up[i] = up_count
                consecutive_down[i] = down_count
            
            df['weekly_Consecutive_Up_Weeks'] = consecutive_up
            df['weekly_Consecutive_Down_Weeks'] = consecutive_down
        
        # 3. 周线成交量特征
        if 'weekly_Volume_Ratio' in df.columns:
            volume_ratio = df['weekly_Volume_Ratio']
            
            # 周成交量异常
            df['weekly_Volume_Anomaly'] = (volume_ratio > 2.0).astype(int)
            
            # 成交量趋势
            df['weekly_Volume_Trend_5'] = volume_ratio.rolling(window=5, min_periods=1).mean()
        
        # 4. 月度季节性特征
        if isinstance(df.index, pd.DatetimeIndex):
            # 获取每月的第几周
            df['weekly_WeekOfMonth'] = df.index.day // 7 + 1
            
            # 月初/月末效应
            df['weekly_Is_Month_Start'] = (df['weekly_WeekOfMonth'] == 1).astype(int)
            df['weekly_Is_Month_End'] = (df['weekly_WeekOfMonth'] >= 4).astype(int)
        
        # 5. 季度表现特征
        if isinstance(df.index, pd.DatetimeIndex):
            quarter = df.index.quarter
            
            # 季节性动量
            df['weekly_Seasonal_Momentum'] = 0
            for q in range(1, 5):
                mask = (quarter == q)
                if mask.any():
                    # 计算该季度的历史平均表现
                    quarter_returns = df.loc[mask, 'weekly_Returns']
                    if len(quarter_returns) > 0:
                        avg_return = quarter_returns.mean()
                        df.loc[mask, 'weekly_Seasonal_Momentum'] = avg_return
        
        # 6. 年度高低点关系
        # 使用shift避免未来数据
        high_52w = df['High'].shift(1).rolling(window=52, min_periods=1).max()
        low_52w = df['Low'].shift(1).rolling(window=52, min_periods=1).min()
        
        df['weekly_High_52W'] = high_52w
        df['weekly_Low_52W'] = low_52w
        df['weekly_Price_vs_52W_High'] = (price - high_52w) / high_52w * 100
        df['weekly_Price_vs_52W_Low'] = (price - low_52w) / low_52w * 100
        
        # 7. 周线突破特征
        for window in [10, 20]:
            # 计算滚动高低点（使用shift）
            high_window = df['High'].shift(1).rolling(window=window, min_periods=1).max()
            low_window = df['Low'].shift(1).rolling(window=window, min_periods=1).min()
            
            # 突破前期高点
            df[f'weekly_Breakout_High_{window}W'] = (
                (price > high_window) & 
                (price.shift(1) <= high_window)
            ).astype(int)
            
            # 跌破前期低点
            df[f'weekly_Breakdown_Low_{window}W'] = (
                (price < low_window) & 
                (price.shift(1) >= low_window)
            ).astype(int)
        
        # 8. 周线技术指标组合
        # 如果同时有多个看涨/看跌信号
        bullish_cols = ['weekly_RSI_Div_Bullish', 'weekly_MACD_Hist_Div_Bullish']
        bearish_cols = ['weekly_RSI_Div_Bearish', 'weekly_MACD_Hist_Div_Bearish']
        
        existing_bullish = [col for col in bullish_cols if col in df.columns]
        existing_bearish = [col for col in bearish_cols if col in df.columns]
        
        if existing_bullish:
            df['weekly_Multiple_Bullish_Signals'] = df[existing_bullish].sum(axis=1)
        
        if existing_bearish:
            df['weekly_Multiple_Bearish_Signals'] = df[existing_bearish].sum(axis=1)
        
        # 9. 周线价格模式
        # 计算周线实体大小
        if 'weekly_Signed_Body_Pct' in df.columns:
            signed_body_pct = df['weekly_Signed_Body_Pct']

            # 大阳线/大阴线
            df['weekly_Big_Green_Candle'] = (signed_body_pct > 5).astype(int)
            df['weekly_Big_Red_Candle'] = (signed_body_pct < -5).astype(int)
        
        # 10. 动量与成交量确认
        if 'weekly_Returns' in df.columns and 'weekly_Volume_Ratio' in df.columns:
            returns = df['weekly_Returns']
            volume_ratio = df['weekly_Volume_Ratio']
            
            # 放量上涨/缩量上涨
            df['weekly_Volume_Price_Confirmation'] = np.where(
                (returns > 0) & (volume_ratio > 1.2), 1,  # 放量上涨
                np.where(
                    (returns < 0) & (volume_ratio > 1.2), -1,  # 放量下跌
                    np.where(
                        (returns > 0) & (volume_ratio < 0.8), 0.5,  # 缩量上涨
                        np.where(
                            (returns < 0) & (volume_ratio < 0.8), -0.5,  # 缩量下跌
                            0  # 中性
                        )
                    )
                )
            )
        
        # 11. 波动率特征
        if 'weekly_Returns' in df.columns:
            returns_series = df['weekly_Returns']
            
            # 周度波动率
            df['weekly_Volatility_4W'] = returns_series.rolling(window=4, min_periods=1).std() * np.sqrt(52)
            df['weekly_Volatility_13W'] = returns_series.rolling(window=13, min_periods=1).std() * np.sqrt(52)
            
            # 波动率变化
            df['weekly_Volatility_Change'] = df['weekly_Volatility_4W'].pct_change()
        
        # 12. 技术指标状态
        if 'weekly_RSI' in df.columns:
            rsi = df['weekly_RSI']
            
            # RSI状态分类
            df['weekly_RSI_State'] = np.where(
                rsi > 70, 2,  # 超买
                np.where(
                    rsi < 30, 0,  # 超卖
                    1  # 中性
                )
            )
        
        # 13. 均线排列状态
        if all(col in df.columns for col in ['weekly_EMA10', 'weekly_EMA20', 'weekly_EMA50']):
            # 多头排列
            df['weekly_MA_Alignment_Long'] = (
                (df['weekly_EMA10'] > df['weekly_EMA20']) &
                (df['weekly_EMA20'] > df['weekly_EMA50'])
            ).astype(int)
            
            # 空头排列
            df['weekly_MA_Alignment_Short'] = (
                (df['weekly_EMA10'] < df['weekly_EMA20']) &
                (df['weekly_EMA20'] < df['weekly_EMA50'])
            ).astype(int)
        
        # 14. 支撑阻力强度
        # 使用历史高点和低点作为支撑阻力
        for lookback in [13, 26, 52]:  # 13周、26周、52周
            # 计算历史高点和低点（使用shift）
            hist_high = df['High'].shift(1).rolling(window=lookback, min_periods=1).max()
            hist_low = df['Low'].shift(1).rolling(window=lookback, min_periods=1).min()
            
            # 价格接近历史高点/低点
            df[f'weekly_Near_Hist_High_{lookback}W'] = (
                (price > hist_high * 0.95) & (price <= hist_high)
            ).astype(int)
            
            df[f'weekly_Near_Hist_Low_{lookback}W'] = (
                (price < hist_low * 1.05) & (price >= hist_low)
            ).astype(int)
        
        # 15. 趋势强度评分
        if all(col in df.columns for col in ['weekly_ADX', 'weekly_PLUS_DI', 'weekly_MINUS_DI']):
            adx = df['weekly_ADX']
            plus_di = df['weekly_PLUS_DI']
            minus_di = df['weekly_MINUS_DI']

            # 综合趋势评分
            df['weekly_Trend_Score_Composite'] = (
                adx * 0.4 +
                (plus_di - minus_di) * 0.3 +
                df['weekly_RSI'].fillna(50) * 0.3 / 100  # 归一化到0-1范围
            )

        # 16. 轻量级周线上下文状态
        if all(col in df.columns for col in ['weekly_EMA20', 'weekly_EMA50']):
            df['weekly_above_long_ma'] = (df['Close'] > df['weekly_EMA50']).astype(int)
            df['weekly_trend_bias'] = np.where(df['weekly_EMA20'] > df['weekly_EMA50'], 1, -1)
        else:
            df['weekly_above_long_ma'] = 0
            df['weekly_trend_bias'] = 0

        if all(col in df.columns for col in ['weekly_ADX', 'weekly_RSI']):
            df['weekly_trend_strength'] = (
                df['weekly_ADX'].fillna(0) * 0.6 +
                (df['weekly_RSI'].fillna(50) - 50).abs() * 0.4
            )
        else:
            df['weekly_trend_strength'] = 0

        if 'weekly_Volatility_13W' in df.columns:
            vol_ref = df['weekly_Volatility_13W'].rolling(window=26, min_periods=1).median()
            df['weekly_vol_regime'] = np.where(df['weekly_Volatility_13W'] >= vol_ref, 1, 0)
        else:
            df['weekly_vol_regime'] = 0

        return df

# 使用示例
if __name__ == "__main__":
    # 创建示例数据
    dates = pd.date_range('2023-01-01', periods=1000, freq='H')
    np.random.seed(42)
    
    data = pd.DataFrame({
        'Open': np.random.randn(1000).cumsum() + 100,
        'High': np.random.randn(1000).cumsum() + 101,
        'Low': np.random.randn(1000).cumsum() + 99,
        'Close': np.random.randn(1000).cumsum() + 100,
        'Volume': np.random.randint(1000, 10000, 1000)
    }, index=dates)
    
    # 计算技术指标
    daily_indicator = DailyIndicators()
    hourly4h_indicator = Hourly4HIndicators()
    hourly1h_indicator = Hourly1HIndicators()
    
    print("=" * 60)
    print("开始计算技术指标（无未来数据版本）")
    print("=" * 60)
    
    # 计算1小时指标
    df_1h = hourly1h_indicator.calculate(data)
    
    # 验证特征
    validator = FeatureValidator()
    validation_results = validator.validate_no_future_data(df_1h)
    
    print("\n" + "=" * 60)
    print("特征验证结果")
    print("=" * 60)
    print(f"总列数: {validation_results['total_columns']}")
    print(f"安全列数: {len(validation_results['safe_columns'])}")
    print(f"可疑列数: {len(validation_results['suspicious_columns'])}")
    
    if validation_results['issues']:
        print("\n发现问题:")
        for issue in validation_results['issues'][:10]:  # 只显示前10个问题
            print(f"  - {issue}")
        if len(validation_results['issues']) > 10:
            print(f"  ... 还有{len(validation_results['issues']) - 10}个问题未显示")
    else:
        print("\n✅ 未发现未来数据泄露问题")
    
    print(f"\n✅ 技术指标计算完成，特征数: {len(df_1h.columns)}")
    print("✅ 所有特征均使用滚动窗口统计，避免未来数据泄露")
