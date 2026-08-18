import pandas as pd
import numpy as np
import talib
from typing import List, Dict, Optional
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
        self.features = pd.DataFrame()
        
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
        
        # 创建features DataFrame，确保索引与data一致
        self.features = pd.DataFrame(index=self.data.index)
        
        # 计算基础价格特征
        self.features['returns'] = self.data['Close'].pct_change()
        self.features['log_returns'] = np.log(self.data['Close'] / self.data['Close'].shift(1))
        
        print("returns列的统计信息:")
        print(f"非NaN值数量: {self.features['returns'].notnull().sum()}")
        print(f"NaN值数量: {self.features['returns'].isnull().sum()}")
        print(f"前5个returns值: {self.features['returns'].head().values}")
        
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
                    self.features[f'SMA_{period}'] = sma
                    
                    # 指数移动平均
                    ema = talib.EMA(close_prices, timeperiod=period)
                    self.features[f'EMA_{period}'] = ema
                    
                    # 价格相对于移动平均线的偏离度
                    sma_series = pd.Series(sma, index=self.data.index)
                    self.features[f'price_dist_to_SMA_{period}'] = (
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
            self.features['MACD'] = macd
            self.features['MACD_signal'] = macd_signal
            self.features['MACD_hist'] = macd_hist
            
            # 计算MACD直方图的发散
            if len(macd_hist) >= 5:
                macd_hist_sma = talib.SMA(macd_hist, timeperiod=5)
                self.features['MACD_hist_divergence'] = macd_hist - macd_hist_sma
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
            self.features['BB_upper'] = bb_upper
            self.features['BB_middle'] = bb_middle
            self.features['BB_lower'] = bb_lower
            
            # 布林带位置 (%B)
            with np.errstate(divide='ignore', invalid='ignore'):
                bb_percent = np.where(
                    (bb_upper - bb_lower) != 0,
                    (close_prices - bb_lower) / (bb_upper - bb_lower),
                    np.nan
                )
            self.features['BB_percent'] = bb_percent
            
            # 布林带宽度（波动性指标）
            self.features['BB_width'] = (bb_upper - bb_lower) / bb_middle
        except Exception as e:
            print(f"计算布林带时出错: {e}")
        
        # 价格通道
        try:
            self.features['high_20'] = self.data['High'].rolling(window=20, min_periods=1).max()
            self.features['low_20'] = self.data['Low'].rolling(window=20, min_periods=1).min()
            
            # 避免除零错误
            denominator = self.features['high_20'] - self.features['low_20']
            denominator = denominator.replace(0, np.nan)  # 将0替换为NaN
            self.features['price_channel_position'] = (
                (self.data['Close'] - self.features['low_20']) / denominator
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
            self.features['RSI'] = rsi
            
            # 慢速RSI
            rsi_slow = talib.RSI(close, timeperiod=28)
            self.features['RSI_slow'] = rsi_slow
            
            # RSI发散
            if len(rsi) >= 5:
                rsi_ema = talib.EMA(rsi, timeperiod=5)
                self.features['RSI_divergence'] = rsi - rsi_ema
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
            self.features['STOCH_K'] = slowk
            self.features['STOCH_D'] = slowd
            self.features['STOCH_RSI'] = (slowk + slowd) / 2
        except Exception as e:
            print(f"计算随机指标时出错: {e}")
        
        # KDJ指标
        try:
            fastk, fastd = talib.STOCHRSI(close, timeperiod=14, fastk_period=3, fastd_period=3)
            self.features['KDJ_K'] = fastk
            self.features['KDJ_D'] = fastd
            self.features['KDJ_J'] = 3 * fastk - 2 * fastd
        except Exception as e:
            print(f"计算KDJ时出错: {e}")
        
        # CCI（商品通道指数）
        try:
            self.features['CCI'] = talib.CCI(high, low, close, timeperiod=20)
        except Exception as e:
            print(f"计算CCI时出错: {e}")
        
        # Williams %R
        try:
            self.features['WILLR'] = talib.WILLR(high, low, close, timeperiod=14)
        except Exception as e:
            print(f"计算Williams %R时出错: {e}")
        
        # 动量指标
        try:
            self.features['MOM'] = talib.MOM(close, timeperiod=10)
        except Exception as e:
            print(f"计算MOM时出错: {e}")
        
        # ROC（价格变化率）
        try:
            self.features['ROC'] = talib.ROC(close, timeperiod=10)
        except Exception as e:
            print(f"计算ROC时出错: {e}")
        
        # 平均方向指数（ADX）
        try:
            self.features['ADX'] = talib.ADX(high, low, close, timeperiod=14)
            self.features['PLUS_DI'] = talib.PLUS_DI(high, low, close, timeperiod=14)
            self.features['MINUS_DI'] = talib.MINUS_DI(high, low, close, timeperiod=14)
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
            self.features['OBV'] = obv
            
            # OBV移动平均
            if len(obv) >= 20:
                obv_ma = talib.SMA(obv, timeperiod=20)
                self.features['OBV_MA'] = obv_ma
                self.features['OBV_divergence'] = obv - obv_ma
        except Exception as e:
            print(f"计算OBV时出错: {e}")
        
        # 成交量加权平均价 (VWAP) - 需要分钟级数据，这里用小时数据近似
        try:
            typical_price = (high + low + close) / 3
            # 使用cumulative sum计算VWAP
            cumulative_tpv = np.cumsum(typical_price * volume)
            cumulative_volume = np.cumsum(volume)
            vwap = np.where(cumulative_volume != 0, cumulative_tpv / cumulative_volume, np.nan)
            self.features['VWAP'] = vwap
            
            # 价格与VWAP的差异
            with np.errstate(divide='ignore', invalid='ignore'):
                self.features['price_vwap_diff'] = np.where(
                    vwap != 0,
                    (close - vwap) / vwap * 100,
                    np.nan
                )
        except Exception as e:
            print(f"计算VWAP时出错: {e}")
        
        # 成交量移动平均
        try:
            volume_series = pd.Series(volume, index=self.data.index)
            self.features['VOLUME_SMA_10'] = volume_series.rolling(window=10, min_periods=1).mean()
            self.features['VOLUME_SMA_20'] = volume_series.rolling(window=20, min_periods=1).mean()
            
            # 成交量比率
            with np.errstate(divide='ignore', invalid='ignore'):
                self.features['volume_ratio'] = np.where(
                    self.features['VOLUME_SMA_20'] != 0,
                    volume / self.features['VOLUME_SMA_20'],
                    np.nan
                )
        except Exception as e:
            print(f"计算成交量指标时出错: {e}")
        
        # 量价关系指标
        try:
            self.features['MFI'] = talib.MFI(high, low, close, volume, timeperiod=14)
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
            self.features['volume_oscillator'] = volume_osc
        except Exception as e:
            print(f"计算成交量震荡指标时出错: {e}")
    
    def add_price_action_features(self):
        """添加价格行为特征"""
        try:
            # 价格范围特征
            self.features['price_range'] = self.data['High'] - self.data['Low']
            
            with np.errstate(divide='ignore', invalid='ignore'):
                self.features['price_range_pct'] = np.where(
                    self.data['Close'] != 0,
                    self.features['price_range'] / self.data['Close'] * 100,
                    np.nan
                )
            
            self.features['body_size'] = abs(self.data['Close'] - self.data['Open'])
            
            with np.errstate(divide='ignore', invalid='ignore'):
                self.features['body_size_pct'] = np.where(
                    self.data['Close'] != 0,
                    self.features['body_size'] / self.data['Close'] * 100,
                    np.nan
                )
            
            # 上影线和下影线
            self.features['upper_shadow'] = self.data['High'] - np.maximum(self.data['Open'], self.data['Close'])
            self.features['lower_shadow'] = np.minimum(self.data['Open'], self.data['Close']) - self.data['Low']
            
            # 蜡烛形态特征
            self.features['is_doji'] = (self.features['body_size_pct'] < 0.1).astype(int)  # 十字星
            
            # 锤子线：下影线至少是实体的2倍，上影线很小
            is_hammer = (
                (self.features['lower_shadow'] > 2 * self.features['body_size']) & 
                (self.features['upper_shadow'] < 0.1 * self.features['body_size'])
            )
            self.features['is_hammer'] = is_hammer.astype(int)
            
            # 射击之星：上影线至少是实体的2倍，下影线很小
            is_shooting_star = (
                (self.features['upper_shadow'] > 2 * self.features['body_size']) & 
                (self.features['lower_shadow'] < 0.1 * self.features['body_size'])
            )
            self.features['is_shooting_star'] = is_shooting_star.astype(int)
            
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
            self.features['bullish_engulfing'] = bullish_engulfing.astype(int)
            
            # 看跌吞没
            bearish_engulfing = (
                (self.data['Close'] < self.data['Open']) &  # 当前阴线
                (prev_close > prev_open) &  # 前一根阳线
                (self.data['Open'] > prev_close) &  # 开盘价高于前收盘
                (self.data['Close'] < prev_open)  # 收盘价低于前开盘
            )
            self.features['bearish_engulfing'] = bearish_engulfing.astype(int)
            
            # 价格位置特征
            denominator = self.data['High'] - self.data['Low']
            denominator = denominator.replace(0, np.nan)  # 避免除零
            self.features['close_position'] = (
                (self.data['Close'] - self.data['Low']) / denominator
            )
        except Exception as e:
            print(f"计算价格行为特征时出错: {e}")
    
    def add_statistical_features(self):
        """添加统计特征"""
        try:
            close = self.data['Close']
            returns = self.features['returns']
            
            # 波动率特征
            self.features['volatility_10'] = returns.rolling(window=10, min_periods=1).std() * np.sqrt(24)  # 年化波动率
            self.features['volatility_20'] = returns.rolling(window=20, min_periods=1).std() * np.sqrt(24)
            
            # 偏度和峰度
            self.features['skewness_10'] = returns.rolling(window=10, min_periods=1).skew()
            self.features['kurtosis_10'] = returns.rolling(window=10, min_periods=1).kurt()
            
            # 分位数特征
            def calc_quantile(x):
                if len(x) < 2:
                    return np.nan
                return pd.Series(x).rank(pct=True).iloc[-1]
            
            self.features['price_quantile_20'] = close.rolling(window=20, min_periods=1).apply(
                calc_quantile, raw=False
            )
            
            # Z-score
            rolling_mean = close.rolling(window=20, min_periods=1).mean()
            rolling_std = close.rolling(window=20, min_periods=1).std()
            
            with np.errstate(divide='ignore', invalid='ignore'):
                self.features['price_zscore'] = np.where(
                    rolling_std != 0,
                    (close - rolling_mean) / rolling_std,
                    np.nan
                )
            
            # 最大回撤
            rolling_max = close.rolling(window=20, min_periods=1).max()
            
            with np.errstate(divide='ignore', invalid='ignore'):
                self.features['drawdown'] = np.where(
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
                self.features['hour'] = self.data.index.hour
                self.features['day_of_week'] = self.data.index.dayofweek
                self.features['day_of_month'] = self.data.index.day
                self.features['month'] = self.data.index.month
                
                # 交易时段特征
                self.features['is_london_session'] = ((self.features['hour'] >= 8) & (self.features['hour'] < 16)).astype(int)
                self.features['is_ny_session'] = ((self.features['hour'] >= 13) & (self.features['hour'] < 21)).astype(int)
                self.features['is_asian_session'] = ((self.features['hour'] >= 22) | (self.features['hour'] < 6)).astype(int)
                
                # 周期性编码
                self.features['hour_sin'] = np.sin(2 * np.pi * self.features['hour'] / 24)
                self.features['hour_cos'] = np.cos(2 * np.pi * self.features['hour'] / 24)
        except Exception as e:
            print(f"添加时间特征时出错: {e}")
    
    def add_lag_features(self, lags: List[int] = [1, 2, 3, 5, 10]):
        """添加滞后特征"""
        try:
            # 价格滞后
            for lag in lags:
                self.features[f'close_lag_{lag}'] = self.data['Close'].shift(lag)
                self.features[f'returns_lag_{lag}'] = self.features['returns'].shift(lag)
                self.features[f'volume_lag_{lag}'] = self.data['Volume'].shift(lag)
            
            # 技术指标滞后
            indicator_cols = ['RSI', 'MACD', 'BB_percent', 'volume_ratio']
            for col in indicator_cols:
                if col in self.features.columns:
                    for lag in [1, 2, 3]:
                        self.features[f'{col}_lag_{lag}'] = self.features[col].shift(lag)
            
            # 变化率特征
            self.features['returns_5'] = self.data['Close'].pct_change(5)
            self.features['returns_10'] = self.data['Close'].pct_change(10)
            self.features['returns_20'] = self.data['Close'].pct_change(20)
        except Exception as e:
            print(f"添加滞后特征时出错: {e}")
    
    def add_interaction_features(self):
        """添加交互特征"""
        try:
            # 量价交互
            if 'volume_ratio' in self.features.columns and 'returns' in self.features.columns:
                # 使用rolling计算相关性
                self.features['volume_price_correlation_10'] = self.data['Volume'].rolling(
                    window=10, min_periods=1
                ).corr(self.data['Close'])
            
            # RSI和价格位置的交互
            if 'RSI' in self.features.columns and 'price_channel_position' in self.features.columns:
                self.features['RSI_price_divergence'] = (
                    self.features['RSI'] - self.features['price_channel_position'] * 100
                )
            
            # 波动率和成交量的交互
            if 'volatility_10' in self.features.columns and 'volume_ratio' in self.features.columns:
                self.features['vol_volume_interaction'] = (
                    self.features['volatility_10'] * self.features['volume_ratio']
                )
        except Exception as e:
            print(f"添加交互特征时出错: {e}")
    
    def calculate_all_features(self):
        """计算所有特征"""
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
        
        # 清理NaN值
        print("10. 清理数据...")
        initial_rows = len(self.features)
        
        # 替换无穷大
        self.features = self.features.replace([np.inf, -np.inf], np.nan)
        
        # 计算缺失值数量
        nan_count_before = self.features.isnull().sum().sum()
        print(f"清理前NaN总数: {nan_count_before}")
        
        # 对于时间序列数据，使用前向填充
        self.features = self.features.ffill()
        
        # 对于剩余的NaN，使用后向填充
        self.features = self.features.bfill()
        
        # 对于仍然存在的NaN，使用列均值填充
        self.features = self.features.fillna(self.features.mean())
        
        nan_count_after = self.features.isnull().sum().sum()
        print(f"清理后NaN总数: {nan_count_after}")
        
        final_rows = len(self.features)
        print(f"特征工程完成！初始行数: {initial_rows}, 最终有效行数: {final_rows}")
        print(f"生成特征数量: {len(self.features.columns)}")
        
        # 移除所有值都相同的列
        constant_cols = [col for col in self.features.columns if self.features[col].nunique() <= 1]
        if constant_cols:
            print(f"移除{len(constant_cols)}个常数特征: {constant_cols}")
            self.features = self.features.drop(columns=constant_cols)
        
        return self.features
    
    def get_feature_summary(self) -> pd.DataFrame:
        """获取特征统计摘要"""
        summary = pd.DataFrame({
            'feature': self.features.columns,
            'dtype': self.features.dtypes.values,
            'missing_pct': (self.features.isnull().sum() / len(self.features) * 100).values,
            'unique_values': [self.features[col].nunique() for col in self.features.columns],
            'mean': self.features.mean().values,
            'std': self.features.std().values
        })
        
        return summary.sort_values('missing_pct')

# 使用示例
if __name__ == "__main__":
    # 读取数据
    try:
        df = pd.read_csv('/LocalSSD/p9056/TestTools_ANALY/data/b_1h.csv')
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
    
    # 计算所有特征
    features_df = feature_engineer.calculate_all_features()
    
    # 查看特征摘要
    summary = feature_engineer.get_feature_summary()
    print("\n特征统计摘要 (前20个特征):")
    print(summary.head(20))
    
    # 保存特征到CSV
    features_df.to_csv('/LocalSSD/p9056/TestTools_ANALY/data/crypto_features.csv', index=True)
    
    # 查看前几行数据
    print("\n前5行特征数据:")
    print(features_df.head())
    
    # 查看特征相关性（可选）
    correlation_matrix = features_df.corr()
    print(f"\n特征相关性矩阵形状: {correlation_matrix.shape}")
    
    # 可以查看与收益相关性最高的特征
    if 'returns' in features_df.columns:
        # 计算相关性，忽略NaN
        corr_with_returns = features_df.corrwith(features_df['returns'])
        corr_with_returns = corr_with_returns.dropna().sort_values(ascending=False)
        
        print("\n与收益相关性最高的10个特征:")
        print(corr_with_returns.head(10))
        print("\n与收益相关性最低的10个特征:")
        print(corr_with_returns.tail(10))
    
    print(f"\n最终特征数据形状: {features_df.shape}")
    print("特征工程完成!")
