import pandas as pd
import numpy as np
import talib
from typing import List, Dict, Optional, Union
import warnings
warnings.filterwarnings('ignore')

class CryptoFeatureEngineer:
    def __init__(self, data: pd.DataFrame):
        """
        初始化特征工程类
        
        Args:
            data: 包含原始K线数据的DataFrame
        """
        self.data = data.copy()
        self.features_with_original = pd.DataFrame()
        self.future_targets = pd.DataFrame()
        
    def preprocess_data(self):
        """数据预处理"""
        print("原始数据形状:", self.data.shape)
        
        # 确保时间列是datetime类型
        if 'Open Time' in self.data.columns:
            self.data['Open Time'] = pd.to_datetime(self.data['Open Time'])
            # 按时间排序
            self.data = self.data.sort_values('Open Time')
            self.data.set_index('Open Time', inplace=True)
        
        print("排序后数据形状:", self.data.shape)
        print("数据时间范围:", self.data.index.min(), "到", self.data.index.max())
        
        # 确保数值列是float类型
        price_cols = ['Open', 'High', 'Low', 'Close', 'Volume']
        for col in price_cols:
            if col in self.data.columns:
                self.data[col] = pd.to_numeric(self.data[col], errors='coerce')
        
        # 检查是否有NaN值
        print("数据缺失值统计:")
        print(self.data[price_cols].isnull().sum())
        
        # 填充缺失值
        self.data = self.data.ffill().bfill()
        
        # 创建features_with_original DataFrame，确保索引与data一致
        self.features_with_original = self.data.copy()
        
        # 创建future_targets DataFrame，与data相同的索引
        self.future_targets = pd.DataFrame(index=self.data.index)
        
        # 计算基础价格特征
        self.features_with_original['returns'] = self.data['Close'].pct_change()
        self.features_with_original['log_returns'] = np.log(self.data['Close'] / self.data['Close'].shift(1))
        
        print("returns列的统计信息:")
        print(f"非NaN值数量: {self.features_with_original['returns'].notnull().sum()}")
        print(f"NaN值数量: {self.features_with_original['returns'].isnull().sum()}")
        print(f"前5个returns值: {self.features_with_original['returns'].head().values}")
        
    def add_trend_indicators(self):
        """添加趋势类指标"""
        close_prices = self.data['Close'].values
        
        # 移动平均线
        periods = [5, 10, 20, 50, 200]
        for period in periods:
            if len(close_prices) >= period:
                try:
                    # 简单移动平均
                    sma = talib.SMA(close_prices, timeperiod=period)
                    self.features_with_original[f'SMA_{period}'] = sma
                    
                    # 指数移动平均
                    ema = talib.EMA(close_prices, timeperiod=period)
                    self.features_with_original[f'EMA_{period}'] = ema
                    
                    # 价格相对于移动平均线的偏离度
                    sma_series = pd.Series(sma, index=self.data.index)
                    self.features_with_original[f'price_dist_to_SMA_{period}'] = (
                        (self.data['Close'] - sma_series) / sma_series * 100
                    )
                except Exception as e:
                    print(f"计算移动平均线(period={period})时出错: {e}")
        
        # MACD
        try:
            macd, macd_signal, macd_hist = talib.MACD(
                close_prices, 
                fastperiod=12, 
                slowperiod=26, 
                signalperiod=9
            )
            self.features_with_original['MACD'] = macd
            self.features_with_original['MACD_signal'] = macd_signal
            self.features_with_original['MACD_hist'] = macd_hist
            
            # 计算MACD直方图的发散
            if len(macd_hist) >= 5:
                macd_hist_sma = talib.SMA(macd_hist, timeperiod=5)
                self.features_with_original['MACD_hist_divergence'] = macd_hist - macd_hist_sma
        except Exception as e:
            print(f"计算MACD时出错: {e}")
        
        # 布林带
        try:
            bb_upper, bb_middle, bb_lower = talib.BBANDS(
                close_prices, 
                timeperiod=20, 
                nbdevup=2, 
                nbdevdn=2
            )
            self.features_with_original['BB_upper'] = bb_upper
            self.features_with_original['BB_middle'] = bb_middle
            self.features_with_original['BB_lower'] = bb_lower
            
            # 布林带位置 (%B)
            with np.errstate(divide='ignore', invalid='ignore'):
                bb_percent = np.where(
                    (bb_upper - bb_lower) != 0,
                    (close_prices - bb_lower) / (bb_upper - bb_lower),
                    np.nan
                )
            self.features_with_original['BB_percent'] = bb_percent
            
            # 布林带宽度（波动性指标）
            self.features_with_original['BB_width'] = (bb_upper - bb_lower) / bb_middle
        except Exception as e:
            print(f"计算布林带时出错: {e}")
        
        # 价格通道
        try:
            self.features_with_original['high_20'] = self.data['High'].rolling(window=20, min_periods=1).max()
            self.features_with_original['low_20'] = self.data['Low'].rolling(window=20, min_periods=1).min()
            
            # 避免除零错误
            denominator = self.features_with_original['high_20'] - self.features_with_original['low_20']
            denominator = denominator.replace(0, np.nan)  # 将0替换为NaN
            self.features_with_original['price_channel_position'] = (
                (self.data['Close'] - self.features_with_original['low_20']) / denominator
            )
        except Exception as e:
            print(f"计算价格通道时出错: {e}")
    
    def add_momentum_indicators(self):
        """添加动量/震荡类指标"""
        high = self.data['High'].values
        low = self.data['Low'].values
        close = self.data['Close'].values
        
        # RSI
        try:
            rsi = talib.RSI(close, timeperiod=14)
            self.features_with_original['RSI'] = rsi
            
            # 慢速RSI
            rsi_slow = talib.RSI(close, timeperiod=28)
            self.features_with_original['RSI_slow'] = rsi_slow
            
            # RSI发散
            if len(rsi) >= 5:
                rsi_ema = talib.EMA(rsi, timeperiod=5)
                self.features_with_original['RSI_divergence'] = rsi - rsi_ema
        except Exception as e:
            print(f"计算RSI时出错: {e}")
        
        # 随机震荡指标
        try:
            slowk, slowd = talib.STOCH(
                high, low, close,
                fastk_period=14,
                slowk_period=3,
                slowk_matype=0,
                slowd_period=3,
                slowd_matype=0
            )
            self.features_with_original['STOCH_K'] = slowk
            self.features_with_original['STOCH_D'] = slowd
            self.features_with_original['STOCH_RSI'] = (slowk + slowd) / 2
        except Exception as e:
            print(f"计算随机指标时出错: {e}")
        
        # KDJ指标
        try:
            fastk, fastd = talib.STOCHRSI(close, timeperiod=14, fastk_period=3, fastd_period=3)
            self.features_with_original['KDJ_K'] = fastk
            self.features_with_original['KDJ_D'] = fastd
            self.features_with_original['KDJ_J'] = 3 * fastk - 2 * fastd
        except Exception as e:
            print(f"计算KDJ时出错: {e}")
        
        # CCI（商品通道指数）
        try:
            self.features_with_original['CCI'] = talib.CCI(high, low, close, timeperiod=20)
        except Exception as e:
            print(f"计算CCI时出错: {e}")
        
        # Williams %R
        try:
            self.features_with_original['WILLR'] = talib.WILLR(high, low, close, timeperiod=14)
        except Exception as e:
            print(f"计算Williams %R时出错: {e}")
        
        # 动量指标
        try:
            self.features_with_original['MOM'] = talib.MOM(close, timeperiod=10)
        except Exception as e:
            print(f"计算MOM时出错: {e}")
        
        # ROC（价格变化率）
        try:
            self.features_with_original['ROC'] = talib.ROC(close, timeperiod=10)
        except Exception as e:
            print(f"计算ROC时出错: {e}")
        
        # 平均方向指数（ADX）
        try:
            self.features_with_original['ADX'] = talib.ADX(high, low, close, timeperiod=14)
            self.features_with_original['PLUS_DI'] = talib.PLUS_DI(high, low, close, timeperiod=14)
            self.features_with_original['MINUS_DI'] = talib.MINUS_DI(high, low, close, timeperiod=14)
        except Exception as e:
            print(f"计算ADX时出错: {e}")
    
    def add_volume_indicators(self):
        """添加成交量类指标"""
        close = self.data['Close'].values
        volume = self.data['Volume'].values
        high = self.data['High'].values
        low = self.data['Low'].values
        
        # OBV（能量潮）
        try:
            obv = talib.OBV(close, volume)
            self.features_with_original['OBV'] = obv
            
            # OBV移动平均
            if len(obv) >= 20:
                obv_ma = talib.SMA(obv, timeperiod=20)
                self.features_with_original['OBV_MA'] = obv_ma
                self.features_with_original['OBV_divergence'] = obv - obv_ma
        except Exception as e:
            print(f"计算OBV时出错: {e}")
        
        # 成交量加权平均价 (VWAP) - 需要分钟级数据，这里用小时数据近似
        try:
            typical_price = (high + low + close) / 3
            # 使用cumulative sum计算VWAP
            cumulative_tpv = np.cumsum(typical_price * volume)
            cumulative_volume = np.cumsum(volume)
            vwap = np.where(cumulative_volume != 0, cumulative_tpv / cumulative_volume, np.nan)
            self.features_with_original['VWAP'] = vwap
            
            # 价格与VWAP的差异
            with np.errstate(divide='ignore', invalid='ignore'):
                self.features_with_original['price_vwap_diff'] = np.where(
                    vwap != 0,
                    (close - vwap) / vwap * 100,
                    np.nan
                )
        except Exception as e:
            print(f"计算VWAP时出错: {e}")
        
        # 成交量移动平均
        try:
            volume_series = pd.Series(volume, index=self.data.index)
            self.features_with_original['VOLUME_SMA_10'] = volume_series.rolling(window=10, min_periods=1).mean()
            self.features_with_original['VOLUME_SMA_20'] = volume_series.rolling(window=20, min_periods=1).mean()
            
            # 成交量比率
            with np.errstate(divide='ignore', invalid='ignore'):
                self.features_with_original['volume_ratio'] = np.where(
                    self.features_with_original['VOLUME_SMA_20'] != 0,
                    volume / self.features_with_original['VOLUME_SMA_20'],
                    np.nan
                )
        except Exception as e:
            print(f"计算成交量指标时出错: {e}")
        
        # 量价关系指标
        try:
            self.features_with_original['MFI'] = talib.MFI(high, low, close, volume, timeperiod=14)
        except Exception as e:
            print(f"计算MFI时出错: {e}")
        
        # 成交量震荡指标
        try:
            volume_ema_short = talib.EMA(volume, timeperiod=10)
            volume_ema_long = talib.EMA(volume, timeperiod=20)
            
            with np.errstate(divide='ignore', invalid='ignore'):
                volume_osc = np.where(
                    volume_ema_long != 0,
                    (volume_ema_short - volume_ema_long) / volume_ema_long * 100,
                    np.nan
                )
            self.features_with_original['volume_oscillator'] = volume_osc
        except Exception as e:
            print(f"计算成交量震荡指标时出错: {e}")
    
    def add_price_action_features(self):
        """添加价格行为特征"""
        try:
            # 价格范围特征
            self.features_with_original['price_range'] = self.data['High'] - self.data['Low']
            
            with np.errstate(divide='ignore', invalid='ignore'):
                self.features_with_original['price_range_pct'] = np.where(
                    self.data['Close'] != 0,
                    self.features_with_original['price_range'] / self.data['Close'] * 100,
                    np.nan
                )
            
            self.features_with_original['body_size'] = abs(self.data['Close'] - self.data['Open'])
            
            with np.errstate(divide='ignore', invalid='ignore'):
                self.features_with_original['body_size_pct'] = np.where(
                    self.data['Close'] != 0,
                    self.features_with_original['body_size'] / self.data['Close'] * 100,
                    np.nan
                )
            
            # 上影线和下影线
            self.features_with_original['upper_shadow'] = self.data['High'] - np.maximum(self.data['Open'], self.data['Close'])
            self.features_with_original['lower_shadow'] = np.minimum(self.data['Open'], self.data['Close']) - self.data['Low']
            
            # 蜡烛形态特征
            self.features_with_original['is_doji'] = (self.features_with_original['body_size_pct'] < 0.1).astype(int)  # 十字星
            
            # 锤子线：下影线至少是实体的2倍，上影线很小
            is_hammer = (
                (self.features_with_original['lower_shadow'] > 2 * self.features_with_original['body_size']) & 
                (self.features_with_original['upper_shadow'] < 0.1 * self.features_with_original['body_size'])
            )
            self.features_with_original['is_hammer'] = is_hammer.astype(int)
            
            # 射击之星：上影线至少是实体的2倍，下影线很小
            is_shooting_star = (
                (self.features_with_original['upper_shadow'] > 2 * self.features_with_original['body_size']) & 
                (self.features_with_original['lower_shadow'] < 0.1 * self.features_with_original['body_size'])
            )
            self.features_with_original['is_shooting_star'] = is_shooting_star.astype(int)
            
            # 吞没形态检测
            prev_close = self.data['Close'].shift(1)
            prev_open = self.data['Open'].shift(1)
            
            # 看涨吞没
            bullish_engulfing = (
                (self.data['Close'] > self.data['Open']) &  # 当前阳线
                (prev_close < prev_open) &  # 前一根阴线
                (self.data['Open'] < prev_close) &  # 开盘价低于前收盘
                (self.data['Close'] > prev_open)  # 收盘价高于前开盘
            )
            self.features_with_original['bullish_engulfing'] = bullish_engulfing.astype(int)
            
            # 看跌吞没
            bearish_engulfing = (
                (self.data['Close'] < self.data['Open']) &  # 当前阴线
                (prev_close > prev_open) &  # 前一根阳线
                (self.data['Open'] > prev_close) &  # 开盘价高于前收盘
                (self.data['Close'] < prev_open)  # 收盘价低于前开盘
            )
            self.features_with_original['bearish_engulfing'] = bearish_engulfing.astype(int)
            
            # 价格位置特征
            denominator = self.data['High'] - self.data['Low']
            denominator = denominator.replace(0, np.nan)  # 避免除零
            self.features_with_original['close_position'] = (
                (self.data['Close'] - self.data['Low']) / denominator
            )
        except Exception as e:
            print(f"计算价格行为特征时出错: {e}")
    
    def add_statistical_features(self):
        """添加统计特征"""
        try:
            close = self.data['Close']
            returns = self.features_with_original['returns']
            
            # 波动率特征
            self.features_with_original['volatility_10'] = returns.rolling(window=10, min_periods=1).std() * np.sqrt(24)  # 年化波动率
            self.features_with_original['volatility_20'] = returns.rolling(window=20, min_periods=1).std() * np.sqrt(24)
            
            # 偏度和峰度
            self.features_with_original['skewness_10'] = returns.rolling(window=10, min_periods=1).skew()
            self.features_with_original['kurtosis_10'] = returns.rolling(window=10, min_periods=1).kurt()
            
            # 分位数特征
            def calc_quantile(x):
                if len(x) < 2:
                    return np.nan
                return pd.Series(x).rank(pct=True).iloc[-1]
            
            self.features_with_original['price_quantile_20'] = close.rolling(window=20, min_periods=1).apply(
                calc_quantile, raw=False
            )
            
            # Z-score
            rolling_mean = close.rolling(window=20, min_periods=1).mean()
            rolling_std = close.rolling(window=20, min_periods=1).std()
            
            with np.errstate(divide='ignore', invalid='ignore'):
                self.features_with_original['price_zscore'] = np.where(
                    rolling_std != 0,
                    (close - rolling_mean) / rolling_std,
                    np.nan
                )
            
            # 最大回撤
            rolling_max = close.rolling(window=20, min_periods=1).max()
            
            with np.errstate(divide='ignore', invalid='ignore'):
                self.features_with_original['drawdown'] = np.where(
                    rolling_max != 0,
                    (close - rolling_max) / rolling_max,
                    np.nan
                )
        except Exception as e:
            print(f"计算统计特征时出错: {e}")
    
    def add_time_features(self):
        """添加时间特征"""
        try:
            if isinstance(self.data.index, pd.DatetimeIndex):
                self.features_with_original['hour'] = self.data.index.hour
                self.features_with_original['day_of_week'] = self.data.index.dayofweek
                self.features_with_original['day_of_month'] = self.data.index.day
                self.features_with_original['month'] = self.data.index.month
                
                # 交易时段特征
                self.features_with_original['is_london_session'] = ((self.features_with_original['hour'] >= 8) & (self.features_with_original['hour'] < 16)).astype(int)
                self.features_with_original['is_ny_session'] = ((self.features_with_original['hour'] >= 13) & (self.features_with_original['hour'] < 21)).astype(int)
                self.features_with_original['is_asian_session'] = ((self.features_with_original['hour'] >= 22) | (self.features_with_original['hour'] < 6)).astype(int)
                
                # 周期性编码
                self.features_with_original['hour_sin'] = np.sin(2 * np.pi * self.features_with_original['hour'] / 24)
                self.features_with_original['hour_cos'] = np.cos(2 * np.pi * self.features_with_original['hour'] / 24)
        except Exception as e:
            print(f"添加时间特征时出错: {e}")
    
    def add_lag_features(self, lags: List[int] = [1, 2, 3, 5, 10]):
        """添加滞后特征"""
        try:
            # 价格滞后
            for lag in lags:
                self.features_with_original[f'close_lag_{lag}'] = self.data['Close'].shift(lag)
                self.features_with_original[f'returns_lag_{lag}'] = self.features_with_original['returns'].shift(lag)
                self.features_with_original[f'volume_lag_{lag}'] = self.data['Volume'].shift(lag)
            
            # 技术指标滞后
            indicator_cols = ['RSI', 'MACD', 'BB_percent', 'volume_ratio']
            for col in indicator_cols:
                if col in self.features_with_original.columns:
                    for lag in [1, 2, 3]:
                        self.features_with_original[f'{col}_lag_{lag}'] = self.features_with_original[col].shift(lag)
            
            # 变化率特征
            self.features_with_original['returns_5'] = self.data['Close'].pct_change(5)
            self.features_with_original['returns_10'] = self.data['Close'].pct_change(10)
            self.features_with_original['returns_20'] = self.data['Close'].pct_change(20)
        except Exception as e:
            print(f"添加滞后特征时出错: {e}")
    
    def add_interaction_features(self):
        """添加交互特征"""
        try:
            # 量价交互
            if 'volume_ratio' in self.features_with_original.columns and 'returns' in self.features_with_original.columns:
                # 使用rolling计算相关性
                self.features_with_original['volume_price_correlation_10'] = self.data['Volume'].rolling(
                    window=10, min_periods=1
                ).corr(self.data['Close'])
            
            # RSI和价格位置的交互
            if 'RSI' in self.features_with_original.columns and 'price_channel_position' in self.features_with_original.columns:
                self.features_with_original['RSI_price_divergence'] = (
                    self.features_with_original['RSI'] - self.features_with_original['price_channel_position'] * 100
                )
            
            # 波动率和成交量的交互
            if 'volatility_10' in self.features_with_original.columns and 'volume_ratio' in self.features_with_original.columns:
                self.features_with_original['vol_volume_interaction'] = (
                    self.features_with_original['volatility_10'] * self.features_with_original['volume_ratio']
                )
        except Exception as e:
            print(f"添加交互特征时出错: {e}")
    
    def add_future_returns_features(self, 
                                   periods: List[int] = [1, 3, 5, 10, 20],
                                   include_classification: bool = True,
                                   include_risk_adjusted: bool = True,
                                   include_rolling: bool = True):
        """
        添加未来收益率特征（目标变量）
        
        Args:
            periods: 未来周期列表
            include_classification: 是否包含分类目标
            include_risk_adjusted: 是否包含风险调整目标
            include_rolling: 是否包含滚动窗口指标
        """
        print("添加未来收益率特征...")
        close = self.data['Close']
        
        # 1. 基础未来收益率
        for period in periods:
            # 简单收益率
            self.future_targets[f'future_{period}_return'] = (
                close.shift(-period) - close
            ) / close
            
            # 对数收益率
            self.future_targets[f'future_{period}_log_return'] = np.log(
                close.shift(-period) / close
            )
        
        # 2. 分类目标
        if include_classification:
            for period in periods[:3]:  # 只计算前3个周期的分类目标
                future_return = self.future_targets[f'future_{period}_return']
                
                # 二分类：上涨/下跌
                self.future_targets[f'future_{period}_up'] = (future_return > 0).astype(int)
                
                # 三分类：大幅上涨/小幅变动/大幅下跌
                rolling_std = future_return.rolling(window=50, min_periods=1).std()
                threshold = rolling_std
                
                label = pd.Series(1, index=future_return.index)  # 中性
                label[future_return > threshold] = 2  # 上涨
                label[future_return < -threshold] = 0  # 下跌
                self.future_targets[f'future_{period}_tri_class'] = label
        
        # 3. 滚动窗口指标
        if include_rolling:
            windows = [5, 10, 20]
            for window in windows:
                future_returns = pd.Series(index=self.data.index, dtype=float)
                future_volatility = pd.Series(index=self.data.index, dtype=float)
                
                for i in range(len(close) - window):
                    window_prices = close.iloc[i:i+window]
                    if len(window_prices) > 0:
                        future_returns.iloc[i] = (window_prices.iloc[-1] - window_prices.iloc[0]) / window_prices.iloc[0]
                        
                        # 计算窗口内的波动率
                        window_returns = window_prices.pct_change().dropna()
                        if len(window_returns) > 1:
                            future_volatility.iloc[i] = window_returns.std()
                
                self.future_targets[f'future_{window}d_cum_return'] = future_returns
                self.future_targets[f'future_{window}d_volatility'] = future_volatility
        
        # 4. 风险调整目标
        if include_risk_adjusted:
            for period in [5, 10, 20]:
                if f'future_{period}_return' in self.future_targets.columns:
                    future_cum_return = self.future_targets[f'future_{period}_return']
                    
                    # 计算未来窗口的波动率
                    future_vol = pd.Series(index=self.data.index, dtype=float)
                    future_downside_vol = pd.Series(index=self.data.index, dtype=float)
                    
                    for i in range(len(close) - period):
                        window_prices = close.iloc[i:i+period]
                        window_returns = window_prices.pct_change().dropna()
                        
                        if len(window_returns) > 1:
                            future_vol.iloc[i] = window_returns.std()
                            downside_returns = window_returns[window_returns < 0]
                            if len(downside_returns) > 1:
                                future_downside_vol.iloc[i] = downside_returns.std()
                    
                    # 夏普比率
                    with np.errstate(divide='ignore', invalid='ignore'):
                        sharpe = future_cum_return / future_vol
                    self.future_targets[f'future_{period}d_sharpe'] = sharpe
                    
                    # 索提诺比率
                    with np.errstate(divide='ignore', invalid='ignore'):
                        sortino = future_cum_return / future_downside_vol
                    self.future_targets[f'future_{period}d_sortino'] = sortino
        
        # 5. 趋势突破目标
        sma_20 = close.rolling(window=20).mean()
        for period in [1, 3, 5]:
            future_price = close.shift(-period)
            
            # 是否突破上轨
            self.future_targets[f'future_{period}d_breakout_up'] = (
                (close <= sma_20) & (future_price > sma_20)
            ).astype(int)
            
            # 是否突破下轨
            self.future_targets[f'future_{period}d_breakout_down'] = (
                (close >= sma_20) & (future_price < sma_20)
            ).astype(int)
        
        print(f"添加了 {len(self.future_targets.columns)} 个未来收益率特征")
        return self.future_targets
    
    def add_advanced_future_features(self):
        """添加高级未来特征"""
        print("添加高级未来特征...")
        close = self.data['Close']
        
        # 未来最大回撤
        windows = [10, 20, 50]
        for window in windows:
            future_max_drawdown = pd.Series(index=self.data.index, dtype=float)
            
            for i in range(len(close) - window):
                window_prices = close.iloc[i:i+window]
                if len(window_prices) > 0:
                    cumulative = (window_prices / window_prices.iloc[0]).values
                    running_max = np.maximum.accumulate(cumulative)
                    drawdown = (cumulative - running_max) / running_max
                    max_drawdown = np.min(drawdown)
                    future_max_drawdown.iloc[i] = max_drawdown
            
            self.future_targets[f'future_{window}d_max_drawdown'] = future_max_drawdown
        
        # 未来价格范围
        for period in [5, 10, 20]:
            future_high = close.rolling(window=period).max().shift(-period+1)
            future_low = close.rolling(window=period).min().shift(-period+1)
            
            self.future_targets[f'future_{period}d_high'] = future_high
            self.future_targets[f'future_{period}d_low'] = future_low
            
            # 价格目标收益率
            current_price = close
            self.future_targets[f'future_{period}d_upside_potential'] = (future_high - current_price) / current_price
            self.future_targets[f'future_{period}d_downside_risk'] = (future_low - current_price) / current_price
        
        print(f"添加了 {len(self.future_targets.columns)} 个高级未来特征")
        return self.future_targets
    
    def calculate_all_features(self, include_future_returns: bool = True, future_periods: List[int] = None):
        """
        计算所有特征，保留原始数据
        
        Args:
            include_future_returns: 是否包含未来收益率特征
            future_periods: 未来收益率周期列表，如果为None则使用默认值
        """
        print("开始特征工程...")
        
        # 数据预处理
        print("1. 数据预处理...")
        self.preprocess_data()
        
        # 添加各类特征
        print("2. 添加趋势指标...")
        self.add_trend_indicators()
        
        print("3. 添加动量指标...")
        self.add_momentum_indicators()
        
        print("4. 添加成交量指标...")
        self.add_volume_indicators()
        
        print("5. 添加价格行为特征...")
        self.add_price_action_features()
        
        print("6. 添加统计特征...")
        self.add_statistical_features()
        
        print("7. 添加时间特征...")
        self.add_time_features()
        
        print("8. 添加滞后特征...")
        self.add_lag_features()
        
        print("9. 添加交互特征...")
        self.add_interaction_features()
        
        # 添加未来收益率特征
        if include_future_returns:
            if future_periods is None:
                future_periods = [1, 3, 5, 10, 20]
            
            print("10. 添加未来收益率特征...")
            self.add_future_returns_features(periods=future_periods)
            
            print("11. 添加高级未来特征...")
            self.add_advanced_future_features()
        
        # 清理NaN值
        print("12. 清理数据...")
        self._clean_data()
        
        # 合并特征和未来目标
        if include_future_returns and len(self.future_targets.columns) > 0:
            # 对齐索引
            self.features_with_original = self.features_with_original.combine_first(self.future_targets)
        
        print("特征工程完成!")
        return self.get_full_data()
    
    def _clean_data(self):
        """清理数据"""
        initial_rows = len(self.features_with_original)
        
        # 替换无穷大
        self.features_with_original = self.features_with_original.replace([np.inf, -np.inf], np.nan)
        
        # 计算缺失值数量
        nan_count_before = self.features_with_original.isnull().sum().sum()
        print(f"清理前NaN总数: {nan_count_before}")
        
        # 对于时间序列数据，使用前向填充
        self.features_with_original = self.features_with_original.ffill()
        
        # 对于剩余的NaN，使用后向填充
        self.features_with_original = self.features_with_original.bfill()
        
        # 对于仍然存在的NaN，使用列均值填充
        self.features_with_original = self.features_with_original.fillna(self.features_with_original.mean())
        
        nan_count_after = self.features_with_original.isnull().sum().sum()
        print(f"清理后NaN总数: {nan_count_after}")
        
        final_rows = len(self.features_with_original)
        print(f"初始行数: {initial_rows}, 最终有效行数: {final_rows}")
        
        # 分离原始数据和特征，方便使用
        original_cols = ['Open', 'High', 'Low', 'Close', 'Volume']
        feature_cols = [col for col in self.features_with_original.columns if col not in original_cols]
        
        print(f"原始数据列: {len(original_cols)}列")
        print(f"特征列: {len(feature_cols)}列")
        print(f"总列数: {len(self.features_with_original.columns)}列")
        
        # 移除所有值都相同的列（除了原始数据列）
        constant_cols = []
        for col in self.features_with_original.columns:
            if col not in original_cols and self.features_with_original[col].nunique() <= 1:
                constant_cols.append(col)
        
        if constant_cols:
            print(f"移除{len(constant_cols)}个常数特征: {constant_cols}")
            self.features_with_original = self.features_with_original.drop(columns=constant_cols)
    
    def get_full_data(self) -> pd.DataFrame:
        """获取完整数据（特征 + 原始数据 + 未来目标）"""
        return self.features_with_original.copy()
    
    def get_features_data(self) -> pd.DataFrame:
        """获取特征数据（不包括原始数据和未来目标）"""
        original_cols = ['Open', 'High', 'Low', 'Close', 'Volume']
        future_cols = [col for col in self.features_with_original.columns if 'future_' in col]
        exclude_cols = original_cols + future_cols
        
        feature_cols = [col for col in self.features_with_original.columns if col not in exclude_cols]
        return self.features_with_original[feature_cols].copy()
    
    def get_future_targets_data(self) -> pd.DataFrame:
        """获取未来目标数据"""
        future_cols = [col for col in self.features_with_original.columns if 'future_' in col]
        return self.features_with_original[future_cols].copy() if future_cols else pd.DataFrame()
    
    def get_original_data(self) -> pd.DataFrame:
        """获取原始数据"""
        original_cols = ['Open', 'High', 'Low', 'Close', 'Volume']
        return self.features_with_original[original_cols].copy()
    
    def get_feature_summary(self) -> pd.DataFrame:
        """获取特征统计摘要"""
        # 排除原始数据和未来目标列
        original_cols = ['Open', 'High', 'Low', 'Close', 'Volume']
        future_cols = [col for col in self.features_with_original.columns if 'future_' in col]
        exclude_cols = original_cols + future_cols
        
        feature_cols = [col for col in self.features_with_original.columns if col not in exclude_cols]
        
        summary = pd.DataFrame({
            'feature': feature_cols,
            'dtype': [self.features_with_original[col].dtype for col in feature_cols],
            'missing_pct': (self.features_with_original[feature_cols].isnull().sum() / len(self.features_with_original) * 100).values,
            'unique_values': [self.features_with_original[col].nunique() for col in feature_cols],
            'mean': [self.features_with_original[col].mean() for col in feature_cols],
            'std': [self.features_with_original[col].std() for col in feature_cols]
        })
        
        return summary.sort_values('missing_pct')
    
    def get_target_summary(self) -> pd.DataFrame:
        """获取目标变量摘要"""
        future_cols = [col for col in self.features_with_original.columns if 'future_' in col]
        
        if not future_cols:
            return pd.DataFrame()
        
        summary = pd.DataFrame({
            'target': future_cols,
            'dtype': self.features_with_original[future_cols].dtypes,
            'missing_pct': (self.features_with_original[future_cols].isnull().sum() / len(self.features_with_original) * 100).values,
            'mean': self.features_with_original[future_cols].mean(),
            'std': self.features_with_original[future_cols].std(),
            'min': self.features_with_original[future_cols].min(),
            'max': self.features_with_original[future_cols].max()
        })
        
        # 分类目标统计
        categorical_targets = [col for col in future_cols if 'up' in col or 'class' in col or 'breakout' in col]
        
        if categorical_targets:
            cat_summary = pd.DataFrame({
                'target': categorical_targets,
                'value_counts': [dict(self.features_with_original[col].value_counts()) 
                                for col in categorical_targets]
            })
            return summary, cat_summary
        
        return summary
    
    def prepare_ml_data(self, test_size: float = 0.2, gap_periods: int = 5, 
                       target_column: str = 'future_1_return') -> tuple:
        """
        准备机器学习数据
        
        Args:
            test_size: 测试集比例
            gap_periods: 特征和目标之间的gap（避免信息泄露）
            target_column: 目标变量列名
            
        Returns:
            X_train, X_test, y_train, y_test
        """
        # 获取特征数据和目标数据
        features_df = self.get_features_data()
        if target_column not in self.features_with_original.columns:
            raise ValueError(f"目标列 '{target_column}' 不存在")
        
        targets_df = self.features_with_original[[target_column]]
        
        # 对齐索引
        common_index = features_df.index.intersection(targets_df.index)
        features_df = features_df.loc[common_index]
        targets_df = targets_df.loc[common_index]
        
        # 确保没有NaN
        features_df = features_df.ffill().bfill()
        targets_df = targets_df.ffill().bfill()
        
        # 移除有NaN的行
        valid_idx = features_df.notnull().all(axis=1) & targets_df.notnull().all(axis=1)
        features_df = features_df[valid_idx]
        targets_df = targets_df[valid_idx]
        
        # 划分训练集和测试集（时间序列，不能随机分割）
        split_idx = int(len(features_df) * (1 - test_size))
        
        X_train = features_df.iloc[:split_idx - gap_periods]
        X_test = features_df.iloc[split_idx:]
        
        y_train = targets_df.iloc[gap_periods:split_idx]
        y_test = targets_df.iloc[split_idx + gap_periods:]
        
        print(f"训练集形状: X={X_train.shape}, y={y_train.shape}")
        print(f"测试集形状: X={X_test.shape}, y={y_test.shape}")
        
        return X_train, X_test, y_train, y_test


# 使用示例
if __name__ == "__main__":
    # 读取数据
    try:
        df = pd.read_csv('/LocalSSD/p9056/TestTools_ANALY/data/b_1d.csv')
        print(f"成功读取数据，形状: {df.shape}")
    except FileNotFoundError:
        print("文件未找到，使用示例数据...")
        # 创建示例数据
        dates = pd.date_range(start='2023-01-01', periods=1000, freq='H')
        df = pd.DataFrame({
            'Open Time': dates,
            'Open': np.random.randn(1000).cumsum() + 100,
            'High': np.random.randn(1000).cumsum() + 105,
            'Low': np.random.randn(1000).cumsum() + 95,
            'Close': np.random.randn(1000).cumsum() + 100,
            'Volume': np.random.uniform(100, 1000, 1000)
        })
    
    # 初始化特征工程类
    feature_engineer = CryptoFeatureEngineer(df)
    
    # 计算所有特征（包含未来收益率）
    full_data = feature_engineer.calculate_all_features(
        include_future_returns=True,
        future_periods=[1, 3, 5, 10, 20]
    )
    
    # 查看数据信息
    print("\n数据信息:")
    print(f"总数据形状: {full_data.shape}")
    
    # 分离不同类型的数据
    original_data = feature_engineer.get_original_data()
    feature_data = feature_engineer.get_features_data()
    future_targets = feature_engineer.get_future_targets_data()
    
    print(f"原始数据形状: {original_data.shape}")
    print(f"特征数据形状: {feature_data.shape}")
    print(f"未来目标形状: {future_targets.shape}")
    
    # 查看特征摘要
    feature_summary = feature_engineer.get_feature_summary()
    print("\n特征统计摘要 (前20个特征):")
    print(feature_summary.head(20))
    
    # 查看目标摘要
    target_summary = feature_engineer.get_target_summary()
    print("\n目标变量摘要:")
    if isinstance(target_summary, tuple):
        print("数值目标摘要:")
        print(target_summary[0].head(20))
        print("\n分类目标摘要:")
        print(target_summary[1].head(20))
    else:
        print(target_summary.head(20))
    
    # 保存数据
    full_data.to_csv('full_data_with_features_and_targets.csv', index=True)
    original_data.to_csv('original_data.csv', index=True)
    feature_data.to_csv('crypto_features.csv', index=True)
    future_targets.to_csv('future_targets.csv', index=True)
    
    print("\n数据已保存:")
    print("- 'full_data_with_features_and_targets.csv' (完整数据)")
    print("- 'original_data.csv' (原始数据)")
    print("- 'crypto_features.csv' (特征数据)")
    print("- 'future_targets.csv' (未来目标)")
    
    # 查看前几行完整数据
    print("\n前5行完整数据:")
    print(full_data.head())
    
    # 分析特征与未来收益的相关性
    if len(future_targets.columns) > 0:
        # 选择第一个未来收益率作为分析目标
        target_col = 'future_10d_max_drawdown'
        # print(future_targets.columns)
        # target_col = future_targets.columns[0]
        if target_col in full_data.columns:
            # 计算相关性
            feature_corr = feature_data.corrwith(full_data[target_col])
            feature_corr = feature_corr.dropna().sort_values(ascending=False)
            
            print(f"\n与{target_col}相关性最高的10个特征:")
            print(feature_corr.head(10))
            print(f"\n与{target_col}相关性最低的10个特征:")
            print(feature_corr.tail(10))
    
    # 准备机器学习数据
    # print("\n准备机器学习数据...")
    # try:
    #     X_train, X_test, y_train, y_test = feature_engineer.prepare_ml_data(
    #         test_size=0.2,
    #         gap_periods=5,
    #         target_column='future_1_return'
    #     )
        
    #     print(f"训练集: X={X_train.shape}, y={y_train.shape}")
    #     print(f"测试集: X={X_test.shape}, y={y_test.shape}")
    # except Exception as e:
    #     print(f"准备机器学习数据时出错: {e}")
    
    print(f"\n最终完整数据形状: {full_data.shape}")
    print("特征工程完成!")
