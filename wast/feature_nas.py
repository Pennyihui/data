import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import warnings
warnings.filterwarnings('ignore')

# 如果原始数据量少，先扩展数据以便特征计算
def generate_sample_data(base_data, periods=200):
    """
    基于基础数据生成更多样本数据（仅用于演示）
    """
    dates = pd.date_range(start='2017-08-17', periods=periods, freq='D')
    
    # 使用随机游走生成更多价格数据
    np.random.seed(42)
    returns = np.random.randn(periods) * 0.02  # 2% daily volatility
    
    # 从第一个收盘价开始
    prices = [base_data.iloc[0]['Close']]
    for i in range(1, periods):
        new_price = prices[-1] * (1 + returns[i])
        prices.append(new_price)
    
    # 创建DataFrame
    df = pd.DataFrame({
        'Open Time': dates,
        'Open': [p * (1 + np.random.uniform(-0.005, 0.005)) for p in prices],
        'High': [p * (1 + np.random.uniform(0, 0.01)) for p in prices],
        'Low': [p * (1 + np.random.uniform(-0.01, 0)) for p in prices],
        'Close': prices,
        'Volume': np.random.uniform(100, 2000, periods),
        'Close Time': dates + pd.Timedelta(days=1) - pd.Timedelta(seconds=1),
        'Quote Asset Volume': np.random.uniform(100000, 5000000, periods),
        'Number of Trades': np.random.randint(1000, 10000, periods),
        'Taker Buy Base Asset Volume': np.random.uniform(50, 1000, periods),
        'Taker Buy Quote Asset Volume': np.random.uniform(50000, 2500000, periods),
        'Ignore': [0] * periods
    })
    
    # 保留原始数据的开头部分
    for i in range(min(len(base_data), len(df))):
        df.iloc[i] = base_data.iloc[i]
    
    return df

# 读取原始数据
df = pd.read_csv('/LocalSSD/p9056/TestTools_ANALY/data/b_1d.csv')

# 如果数据太少，生成更多样本数据
if len(df) < 100:
    print(f"原始数据只有 {len(df)} 行，生成样本数据以便特征计算...")
    df = generate_sample_data(df, periods=200)

print(f"数据形状: {df.shape}")
print(f"数据时间范围: {df['Open Time'].min()} 到 {df['Open Time'].max()}")

# 数据预处理
def preprocess_data(df):
    """
    基础数据预处理
    """
    # 确保时间列是datetime类型
    df['Open Time'] = pd.to_datetime(df['Open Time'])
    df['Close Time'] = pd.to_datetime(df['Close Time'])
    
    # 按时间排序
    df = df.sort_values('Open Time').reset_index(drop=True)
    
    # 检查缺失值
    print(f"缺失值检查:")
    print(df.isnull().sum())
    
    # 如果有缺失值，用前向填充
    df = df.fillna(method='ffill').fillna(method='bfill')
    
    return df

df = preprocess_data(df)

# ==================== 特征工程 ====================

class FinancialFeatureEngineering:
    """金融时间序列特征工程类"""
    
    def __init__(self, df):
        self.df = df.copy()
        self.features = pd.DataFrame(index=df.index)
        
    def add_price_features(self):
        """价格相关特征"""
        df = self.df
        
        # 1. 基础价格变换
        # 对数收益率
        self.features['log_return'] = np.log(df['Close'] / df['Close'].shift(1))
        
        # OHLC变换
        self.features['hl_range'] = (df['High'] - df['Low']) / df['Close']  # 价格范围
        self.features['oc_range'] = (df['Close'] - df['Open']) / df['Open']  # 开盘到收盘变化
        self.features['body_ratio'] = np.abs(df['Close'] - df['Open']) / (df['High'] - df['Low'] + 1e-8)
        
        # 价格位置特征
        self.features['price_position'] = (df['Close'] - df['Low']) / (df['High'] - df['Low'] + 1e-8)
        
        # 2. 多时间尺度收益率
        windows = [2, 3, 5, 10, 20, 30, 60]  # 多个时间窗口
        
        for window in windows:
            if window < len(df):
                # 对数收益率
                self.features[f'return_{window}d'] = np.log(df['Close'] / df['Close'].shift(window))
                
                # 滚动波动率
                self.features[f'volatility_{window}d'] = df['Close'].pct_change().rolling(window=window).std()
                
                # 滚动最大回撤
                rolling_max = df['Close'].rolling(window=window).max()
                self.features[f'drawdown_{window}d'] = (df['Close'] - rolling_max) / rolling_max
        
        return self
    
    def add_volume_features(self):
        """成交量相关特征"""
        df = self.df
        
        # 成交量变化率
        self.features['volume_change'] = df['Volume'].pct_change()
        
        # 成交量加权价格
        self.features['vwap'] = (df['Volume'] * (df['High'] + df['Low'] + df['Close']) / 3).cumsum() / df['Volume'].cumsum()
        
        # 成交量与价格的关系
        self.features['volume_price_corr_5d'] = df['Volume'].rolling(5).corr(df['Close'])
        self.features['volume_price_corr_20d'] = df['Volume'].rolling(20).corr(df['Close'])
        
        # 成交量比率
        self.features['volume_ratio_5d'] = df['Volume'] / df['Volume'].rolling(5).mean()
        self.features['volume_ratio_20d'] = df['Volume'] / df['Volume'].rolling(20).mean()
        
        # 大单指标（简化）
        if 'Taker Buy Base Asset Volume' in df.columns:
            self.features['buy_volume_ratio'] = df['Taker Buy Base Asset Volume'] / df['Volume']
        
        return self
    
    def add_technical_indicators(self):
        """技术指标"""
        df = self.df
        
        # ====== 趋势类指标 ======
        
        # 移动平均线（多个时间尺度）
        ma_windows = [5, 10, 20, 30, 50, 100, 200]
        
        for window in ma_windows:
            if window < len(df):
                # 简单移动平均
                self.features[f'SMA_{window}'] = df['Close'].rolling(window=window).mean()
                
                # 指数移动平均
                self.features[f'EMA_{window}'] = df['Close'].ewm(span=window, adjust=False).mean()
                
                # 价格与移动平均的关系
                self.features[f'price_sma_ratio_{window}'] = df['Close'] / self.features[f'SMA_{window}']
                self.features[f'price_ema_ratio_{window}'] = df['Close'] / self.features[f'EMA_{window}']
                
                # 移动平均斜率
                self.features[f'sma_slope_{window}'] = self.features[f'SMA_{window}'].diff() / self.features[f'SMA_{window}'].shift(1)
                self.features[f'ema_slope_{window}'] = self.features[f'EMA_{window}'].diff() / self.features[f'EMA_{window}'].shift(1)
        
        # MACD
        exp1 = df['Close'].ewm(span=12, adjust=False).mean()
        exp2 = df['Close'].ewm(span=26, adjust=False).mean()
        self.features['MACD'] = exp1 - exp2
        self.features['MACD_signal'] = self.features['MACD'].ewm(span=9, adjust=False).mean()
        self.features['MACD_hist'] = self.features['MACD'] - self.features['MACD_signal']
        
        # ====== 动量类指标 ======
        
        # RSI (相对强弱指数)
        def calculate_rsi(prices, window=14):
            delta = prices.diff()
            gain = (delta.where(delta > 0, 0)).rolling(window=window).mean()
            loss = (-delta.where(delta < 0, 0)).rolling(window=window).mean()
            rs = gain / loss
            rsi = 100 - (100 / (1 + rs))
            return rsi
        
        self.features['RSI_14'] = calculate_rsi(df['Close'], window=14)
        self.features['RSI_7'] = calculate_rsi(df['Close'], window=7)
        self.features['RSI_28'] = calculate_rsi(df['Close'], window=28)
        
        # 随机指标 Stochastic
        low_min = df['Low'].rolling(window=14).min()
        high_max = df['High'].rolling(window=14).max()
        self.features['STOCH_K'] = 100 * ((df['Close'] - low_min) / (high_max - low_min + 1e-8))
        self.features['STOCH_D'] = self.features['STOCH_K'].rolling(window=3).mean()
        
        # CCI (商品通道指数)
        typical_price = (df['High'] + df['Low'] + df['Close']) / 3
        sma_tp = typical_price.rolling(window=20).mean()
        mad = typical_price.rolling(window=20).apply(lambda x: np.abs(x - x.mean()).mean())
        self.features['CCI'] = (typical_price - sma_tp) / (0.015 * mad + 1e-8)
        
        # ====== 波动率类指标 ======
        
        # ATR (平均真实波幅)
        high_low = df['High'] - df['Low']
        high_close = np.abs(df['High'] - df['Close'].shift())
        low_close = np.abs(df['Low'] - df['Close'].shift())
        tr = pd.concat([high_low, high_close, low_close], axis=1).max(axis=1)
        self.features['ATR'] = tr.rolling(window=14).mean()
        self.features['ATR_ratio'] = self.features['ATR'] / df['Close']
        
        # 布林带
        bb_window = 20
        bb_std = df['Close'].rolling(window=bb_window).std()
        self.features['BB_middle'] = df['Close'].rolling(window=bb_window).mean()
        self.features['BB_upper'] = self.features['BB_middle'] + (bb_std * 2)
        self.features['BB_lower'] = self.features['BB_middle'] - (bb_std * 2)
        self.features['BB_width'] = (self.features['BB_upper'] - self.features['BB_lower']) / self.features['BB_middle']
        self.features['BB_position'] = (df['Close'] - self.features['BB_lower']) / (self.features['BB_upper'] - self.features['BB_lower'] + 1e-8)
        
        # 历史波动率
        for window in [5, 10, 20, 30, 60]:
            if window < len(df):
                self.features[f'historical_vol_{window}d'] = df['Close'].pct_change().rolling(window=window).std() * np.sqrt(252)  # 年化
        
        return self
    
    def add_market_regime_features(self):
        """市场状态特征"""
        df = self.df
        
        # 1. 趋势强度指标
        # ADX (简化版)
        high = df['High']
        low = df['Low']
        close = df['Close']
        
        # 方向运动
        up_move = high - high.shift(1)
        down_move = low.shift(1) - low
        
        pos_dm = np.where((up_move > down_move) & (up_move > 0), up_move, 0)
        neg_dm = np.where((down_move > up_move) & (down_move > 0), down_move, 0)
        
        # 计算14日平均
        atr = self.features['ATR']
        
        pos_di = 100 * pd.Series(pos_dm, index=df.index).rolling(14).mean() / atr
        neg_di = 100 * pd.Series(neg_dm, index=df.index).rolling(14).mean() / atr
        
        self.features['ADX'] = 100 * np.abs(pos_di - neg_di) / (pos_di + neg_di + 1e-8)
        
        # 2. 市场状态分类（简化）
        # 基于波动率和趋势的简单分类
        volatility = self.features['historical_vol_20d']
        trend_strength = self.features['ADX']
        
        # 分类规则
        self.features['regime_trend'] = (trend_strength > 25).astype(int)
        self.features['regime_volatile'] = (volatility > volatility.rolling(50).mean() * 1.5).astype(int)
        self.features['regime_range'] = ((trend_strength < 20) & (volatility <= volatility.rolling(50).mean() * 1.5)).astype(int)
        
        # 3. 成交量特征
        volume_ma = df['Volume'].rolling(20).mean()
        self.features['volume_regime_high'] = (df['Volume'] > volume_ma * 1.5).astype(int)
        self.features['volume_regime_low'] = (df['Volume'] < volume_ma * 0.7).astype(int)
        
        return self
    
    def add_time_features(self):
        """时间相关特征"""
        df = self.df
        
        # 日期特征
        dates = pd.to_datetime(df['Open Time'])
        
        # 周期性特征
        self.features['day_of_week'] = dates.dt.dayofweek  # 0=周一, 6=周日
        self.features['day_of_month'] = dates.dt.day
        self.features['month'] = dates.dt.month
        self.features['week_of_year'] = dates.dt.isocalendar().week
        self.features['quarter'] = dates.dt.quarter
        
        # 季节性编码
        self.features['sin_month'] = np.sin(2 * np.pi * dates.dt.month / 12)
        self.features['cos_month'] = np.cos(2 * np.pi * dates.dt.month / 12)
        
        # 周内模式编码
        self.features['sin_dayofweek'] = np.sin(2 * np.pi * dates.dt.dayofweek / 7)
        self.features['cos_dayofweek'] = np.cos(2 * np.pi * dates.dt.dayofweek / 7)
        
        # 是否月初/月末/季初/季末
        self.features['is_month_start'] = dates.dt.is_month_start.astype(int)
        self.features['is_month_end'] = dates.dt.is_month_end.astype(int)
        self.features['is_quarter_start'] = dates.dt.is_quarter_start.astype(int)
        self.features['is_quarter_end'] = dates.dt.is_quarter_end.astype(int)
        
        return self
    
    def add_advanced_features(self):
        """高级特征"""
        df = self.df
        
        # 1. 价格加速/减速
        returns = self.features['log_return']
        self.features['price_acceleration'] = returns.diff()  # 收益率的变化率
        
        # 2. 流动性指标
        if 'Quote Asset Volume' in df.columns:
            self.features['liquidity'] = df['Quote Asset Volume'] / self.features['ATR']
        
        # 3. 市场效率指标（简化）
        # 基于收益率自相关的市场效率
        autocorr_1 = returns.rolling(20).apply(lambda x: x.autocorr(lag=1), raw=False)
        autocorr_5 = returns.rolling(20).apply(lambda x: x.autocorr(lag=5), raw=False)
        self.features['market_efficiency_1'] = np.abs(autocorr_1)
        self.features['market_efficiency_5'] = np.abs(autocorr_5)
        
        # 4. 支撑阻力特征
        close = df['Close']
        
        # 近期高低点
        rolling_high_20 = close.rolling(20).max()
        rolling_low_20 = close.rolling(20).min()
        
        self.features['distance_to_high_20'] = (close - rolling_high_20) / rolling_high_20
        self.features['distance_to_low_20'] = (close - rolling_low_20) / rolling_low_20
        
        # 5. 价格动量特征
        # 短期 vs 长期动量
        momentum_5 = close / close.shift(5) - 1
        momentum_20 = close / close.shift(20) - 1
        self.features['momentum_ratio'] = momentum_5 / (momentum_20 + 1e-8)
        
        return self
    
    def add_statistical_features(self):
        """统计特征"""
        df = self.df
        close = df['Close']
        
        # 滚动统计量
        windows = [5, 10, 20, 30, 60]
        
        for window in windows:
            if window < len(df):
                # 偏度（收益率分布不对称性）
                self.features[f'skewness_{window}d'] = close.pct_change().rolling(window).skew()
                
                # 峰度（极端值概率）
                self.features[f'kurtosis_{window}d'] = close.pct_change().rolling(window).kurt()
                
                # 分位数
                self.features[f'quantile_25_{window}d'] = close.rolling(window).quantile(0.25)
                self.features[f'quantile_75_{window}d'] = close.rolling(window).quantile(0.75)
                
                # 价格在分布中的位置
                rolling_median = close.rolling(window).median()
                rolling_std = close.rolling(window).std()
                self.features[f'price_zscore_{window}d'] = (close - rolling_median) / (rolling_std + 1e-8)
        
        # 夏普比率（简化版）
        returns = close.pct_change()
        for window in [20, 60, 120]:
            if window < len(df):
                rolling_return = returns.rolling(window).mean() * 252  # 年化收益率
                rolling_vol = returns.rolling(window).std() * np.sqrt(252)  # 年化波动率
                self.features[f'sharpe_ratio_{window}d'] = rolling_return / (rolling_vol + 1e-8)
        
        return self
    
    def adaptive_normalization(self):
        """自适应归一化"""
        print("执行自适应归一化...")
        
        # 分组特征类型，应用不同的归一化策略
        normalized_features = self.features.copy()
        
        # 识别需要归一化的数值型特征
        numeric_cols = normalized_features.select_dtypes(include=[np.number]).columns
        
        # 排除已经是比例或分类的特征
        exclude_patterns = ['_ratio', '_position', '_regime', '_score', 'day_of_', 'month', 
                           'quarter', 'week_', 'is_', 'sin_', 'cos_']
        
        cols_to_normalize = []
        for col in numeric_cols:
            if not any(pattern in col for pattern in exclude_patterns):
                cols_to_normalize.append(col)
        
        print(f"将对 {len(cols_to_normalize)} 个特征进行归一化")
        
        # 自适应归一化：基于滚动窗口的Z-score
        window_size = 60  # 使用60天的滚动窗口
        
        for col in cols_to_normalize:
            # 计算滚动均值和标准差
            rolling_mean = normalized_features[col].rolling(window=window_size, min_periods=1).mean()
            rolling_std = normalized_features[col].rolling(window=window_size, min_periods=1).std()
            
            # 避免除零
            rolling_std = rolling_std.replace(0, 1)
            
            # Z-score归一化
            normalized_features[col] = (normalized_features[col] - rolling_mean) / rolling_std
            
            # 可选：对极端值进行缩尾处理
            # normalized_features[col] = np.clip(normalized_features[col], -3, 3)
        
        # 对比例特征进行特殊处理（保持在0-1或-1到1之间）
        ratio_cols = [col for col in normalized_features.columns if '_ratio' in col or '_position' in col]
        for col in ratio_cols:
            # 缩放到[-1, 1]或[0, 1]
            if normalized_features[col].min() < 0:
                # 对于有负值的，缩放到[-1, 1]
                max_val = normalized_features[col].abs().max()
                if max_val > 0:
                    normalized_features[col] = normalized_features[col] / max_val
            else:
                # 对于非负的，缩放到[0, 1]
                max_val = normalized_features[col].max()
                min_val = normalized_features[col].min()
                if max_val > min_val:
                    normalized_features[col] = (normalized_features[col] - min_val) / (max_val - min_val)
        
        self.normalized_features = normalized_features
        
        return self
    
    def get_feature_groups(self):
        """获取特征分组"""
        feature_groups = {
            'price_features': [],
            'return_features': [],
            'volume_features': [],
            'trend_features': [],
            'momentum_features': [],
            'volatility_features': [],
            'regime_features': [],
            'time_features': [],
            'statistical_features': [],
            'advanced_features': []
        }
        
        # 根据特征名分类
        for col in self.normalized_features.columns:
            col_lower = col.lower()
            
            if any(keyword in col_lower for keyword in ['price', 'open', 'high', 'low', 'close', 'hl_', 'oc_']):
                feature_groups['price_features'].append(col)
            elif 'return' in col_lower or 'momentum' in col_lower:
                feature_groups['return_features'].append(col)
            elif 'volume' in col_lower or 'vwap' in col_lower:
                feature_groups['volume_features'].append(col)
            elif any(keyword in col_lower for keyword in ['sma', 'ema', 'ma', 'macd', 'bb_', 'adx', 'trend']):
                feature_groups['trend_features'].append(col)
            elif any(keyword in col_lower for keyword in ['rsi', 'stoch', 'cci']):
                feature_groups['momentum_features'].append(col)
            elif any(keyword in col_lower for keyword in ['volatility', 'atr', 'drawdown', 'std']):
                feature_groups['volatility_features'].append(col)
            elif 'regime' in col_lower:
                feature_groups['regime_features'].append(col)
            elif any(keyword in col_lower for keyword in ['day', 'month', 'week', 'quarter', 'sin', 'cos', 'is_']):
                feature_groups['time_features'].append(col)
            elif any(keyword in col_lower for keyword in ['skewness', 'kurtosis', 'quantile', 'zscore', 'sharpe']):
                feature_groups['statistical_features'].append(col)
            else:
                feature_groups['advanced_features'].append(col)
        
        return feature_groups
    
    def build_all_features(self, normalize=True):
        """构建所有特征"""
        print("开始特征工程...")
        
        # 按顺序添加特征
        (self.add_price_features()
           .add_volume_features()
           .add_technical_indicators()
           .add_market_regime_features()
           .add_time_features()
           .add_advanced_features()
           .add_statistical_features())
        
        # 处理缺失值（由于滚动计算，前几行会有NaN）
        self.features = self.features.fillna(method='bfill').fillna(method='ffill').fillna(0)
        
        if normalize:
            self.adaptive_normalization()
        else:
            self.normalized_features = self.features.copy()
        
        # 获取特征分组
        self.feature_groups = self.get_feature_groups()
        
        print(f"特征工程完成！共生成 {self.normalized_features.shape[1]} 个特征")
        
        # 打印特征分组信息
        print("\n特征分组统计:")
        for group_name, features in self.feature_groups.items():
            print(f"  {group_name}: {len(features)} 个特征")
        
        return self
    
    def save_features_to_csv(self, output_path):
        """保存特征到CSV文件"""
        # 合并原始数据和特征
        result_df = pd.concat([self.df, self.normalized_features], axis=1)
        
        # 保存到CSV
        result_df.to_csv(output_path, index=False)
        print(f"特征已保存到: {output_path}")
        
        # 保存特征描述
        feature_desc_path = output_path.replace('.csv', '_description.txt')
        with open(feature_desc_path, 'w', encoding='utf-8') as f:
            f.write("特征描述文件\n")
            f.write("="*50 + "\n")
            f.write(f"总特征数: {self.normalized_features.shape[1]}\n")
            f.write(f"数据行数: {self.normalized_features.shape[0]}\n")
            f.write(f"生成时间: {datetime.now()}\n\n")
            
            f.write("特征分组详情:\n")
            for group_name, features in self.feature_groups.items():
                f.write(f"\n{group_name} ({len(features)}个):\n")
                for feature in sorted(features):
                    f.write(f"  - {feature}\n")
        
        print(f"特征描述已保存到: {feature_desc_path}")
        
        return result_df

# ==================== 执行特征工程 ====================

# 初始化特征工程
feature_engineer = FinancialFeatureEngineering(df)

# 构建所有特征
feature_engineer.build_all_features(normalize=True)

# 保存特征到文件
output_path = '/LocalSSD/p9056/TestTools_ANALY/data/b_1d_features.csv'
result_df = feature_engineer.save_features_to_csv(output_path)

# 显示前几行数据
print("\n前5行特征数据示例:")
print(result_df.iloc[:5, -20:])  # 显示最后20个特征的前5行

print("\n数据统计信息:")
print(f"总行数: {result_df.shape[0]}")
print(f"总列数: {result_df.shape[1]}")
print(f"特征列数: {feature_engineer.normalized_features.shape[1]}")

# 保存特征分组信息为独立文件
feature_groups_df = pd.DataFrame({
    'feature_name': feature_engineer.normalized_features.columns.tolist(),
    'feature_group': [''] * len(feature_engineer.normalized_features.columns)
})

# 标记特征分组
for group_name, features in feature_engineer.feature_groups.items():
    for feature in features:
        idx = feature_groups_df[feature_groups_df['feature_name'] == feature].index
        if len(idx) > 0:
            feature_groups_df.loc[idx[0], 'feature_group'] = group_name

feature_groups_path = output_path.replace('.csv', '_groups.csv')
feature_groups_df.to_csv(feature_groups_path, index=False)
print(f"特征分组信息已保存到: {feature_groups_path}")
