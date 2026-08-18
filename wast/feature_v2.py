import pandas as pd
import numpy as np
from sklearn.decomposition import PCA
from sklearn.feature_selection import VarianceThreshold, SelectKBest, f_regression, mutual_info_regression
from sklearn.preprocessing import StandardScaler
import warnings
warnings.filterwarnings('ignore')

class FeatureOptimizer:
    def __init__(self, features_df: pd.DataFrame, target_col: str = 'future_1_return'):
        """
        特征优化器
        
        Args:
            features_df: 原始特征DataFrame，包含未来收益率特征
            target_col: 目标变量列名，默认使用未来1期收益率
                      可选: 'future_1_return', 'future_10_return', 'future_10d_cum_return'等
        """
        self.original_features = features_df.copy()
        self.target_col = target_col
        self.optimized_features = None
        self.selected_features = []
        self.scaler = StandardScaler()
        
        # 检查目标列是否存在
        if self.target_col not in self.original_features.columns:
            available_targets = [col for col in self.original_features.columns if 'future_' in col]
            if available_targets:
                print(f"警告: 目标列 '{target_col}' 不存在!")
                print(f"可用的未来收益率列: {available_targets[:10]}...")
                # 使用第一个未来收益率作为默认
                self.target_col = available_targets[0]
                print(f"已自动选择 '{self.target_col}' 作为目标变量")
            else:
                raise ValueError(f"目标列 '{target_col}' 不存在，且没有找到未来收益率特征")
        
        print(f"使用目标变量: {self.target_col}")
        print(f"目标变量统计:")
        print(f"  均值: {self.original_features[self.target_col].mean():.6f}")
        print(f"  标准差: {self.original_features[self.target_col].std():.6f}")
        print(f"  非空值数量: {self.original_features[self.target_col].notnull().sum()}")
    
    def get_available_targets(self):
        """获取可用的目标变量列表"""
        future_cols = [col for col in self.original_features.columns if 'future_' in col]
        
        # 分类目标变量
        classification_targets = [col for col in future_cols if any(x in col for x in ['_up', '_class', 'breakout'])]
        
        # 回归目标变量
        regression_targets = [col for col in future_cols if col not in classification_targets]
        
        # 按类型和周期分组
        targets_info = {
            '短期收益率 (1-3期)': [col for col in regression_targets if any(x in col for x in ['_1_', '_3_', '1d_', '3d_'])],
            '中期收益率 (5-10期)': [col for col in regression_targets if any(x in col for x in ['_5_', '_10_', '5d_', '10d_'])],
            '长期收益率 (20期+)': [col for col in regression_targets if any(x in col for x in ['_20_', '_50_', '20d_', '50d_'])],
            '分类目标': classification_targets,
            '风险调整目标': [col for col in regression_targets if any(x in col for x in ['sharpe', 'sortino', 'volatility', 'drawdown'])],
            '价格极值目标': [col for col in regression_targets if any(x in col for x in ['high', 'low', 'upside', 'downside'])]
        }
        
        return targets_info
    
    def analyze_target_correlations(self):
        """分析不同未来收益率之间的相关性"""
        print("\n分析不同未来收益率之间的相关性...")
        
        # 获取所有未来收益率列
        future_cols = [col for col in self.original_features.columns if 'future_' in col]
        
        if len(future_cols) > 1:
            # 计算相关性矩阵
            corr_matrix = self.original_features[future_cols].corr()
            
            # 计算与当前目标的相关性
            target_correlations = corr_matrix[self.target_col].sort_values(ascending=False)
            
            print(f"\n与目标变量 '{self.target_col}' 相关性最高的未来收益率:")
            for col, corr in target_correlations.head(10).items():
                if col != self.target_col:
                    print(f"  {col}: {corr:.4f}")
            
            print(f"\n与目标变量 '{self.target_col}' 相关性最低的未来收益率:")
            for col, corr in target_correlations.tail(10).items():
                if col != self.target_col:
                    print(f"  {col}: {corr:.4f}")
            
            return target_correlations
        
        return None
    
    def remove_early_unstable_data(self, periods_to_remove: int = 1000):
        """移除早期不稳定数据"""
        print(f"\n移除前{periods_to_remove}期不稳定数据...")
        
        # 计算需要移除的行数
        rows_to_remove = min(periods_to_remove, len(self.original_features))
        
        # 保存移除前的数据信息
        print(f"移除前数据形状: {self.original_features.shape}")
        print(f"目标变量非空值数量: {self.original_features[self.target_col].notnull().sum()}")
        
        # 移除早期数据
        self.optimized_features = self.original_features.iloc[rows_to_remove:].copy()
        
        print(f"移除后数据形状: {self.optimized_features.shape}")
        print(f"目标变量非空值数量: {self.optimized_features[self.target_col].notnull().sum()}")
        
        return self.optimized_features
    
    def create_advanced_feature_combinations(self):
        """创建高级特征组合"""
        print("\n创建高级特征组合...")
        
        if self.optimized_features is None:
            self.optimized_features = self.original_features.copy()
        
        # 1. 价格动量组合特征
        print("1. 创建价格动量组合特征...")
        
        # 多时间尺度动量组合
        for short_period in [5, 10]:
            for long_period in [20, 50]:
                if f'price_dist_to_SMA_{short_period}' in self.optimized_features.columns and \
                   f'price_dist_to_SMA_{long_period}' in self.optimized_features.columns:
                    self.optimized_features[f'momentum_ratio_{short_period}_{long_period}'] = (
                        self.optimized_features[f'price_dist_to_SMA_{short_period}'] / 
                        (self.optimized_features[f'price_dist_to_SMA_{long_period}'] + 1e-8)
                    )
        
        # 2. 波动率调整特征
        print("2. 创建波动率调整特征...")
        
        if 'volatility_10' in self.optimized_features.columns:
            # 波动率调整的收益率
            self.optimized_features['vol_adjusted_returns'] = (
                self.optimized_features['returns'] / 
                (self.optimized_features['volatility_10'] + 1e-8)
            )
            
            # 波动率调整的价格偏离
            for period in [5, 10, 20]:
                col_name = f'price_dist_to_SMA_{period}'
                if col_name in self.optimized_features.columns:
                    self.optimized_features[f'vol_adjusted_{col_name}'] = (
                        self.optimized_features[col_name] / 
                        (self.optimized_features['volatility_10'] + 1e-8)
                    )
        
        # 3. 成交量确认特征
        print("3. 创建成交量确认特征...")
        
        if 'volume_ratio' in self.optimized_features.columns:
            # 量价确认指标
            self.optimized_features['volume_price_confirmation'] = (
                np.sign(self.optimized_features['returns']) * 
                self.optimized_features['volume_ratio']
            )
            
            # 成交量放大时的价格动量
            for period in [5, 10]:
                col_name = f'price_dist_to_SMA_{period}'
                if col_name in self.optimized_features.columns:
                    self.optimized_features[f'volume_boosted_{col_name}'] = (
                        self.optimized_features[col_name] * 
                        self.optimized_features['volume_ratio']
                    )
        
        # 4. 技术指标组合特征
        print("4. 创建技术指标组合特征...")
        
        # RSI与随机指标组合
        if 'RSI' in self.optimized_features.columns and 'STOCH_K' in self.optimized_features.columns:
            self.optimized_features['RSI_STOCH_combo'] = (
                0.6 * self.optimized_features['RSI'] + 
                0.4 * self.optimized_features['STOCH_K']
            )
            
            # 超买超卖组合信号
            self.optimized_features['overbought_signal'] = (
                (self.optimized_features['RSI'] > 70).astype(int) + 
                (self.optimized_features['STOCH_K'] > 80).astype(int)
            )
            self.optimized_features['oversold_signal'] = (
                (self.optimized_features['RSI'] < 30).astype(int) + 
                (self.optimized_features['STOCH_K'] < 20).astype(int)
            )
        
        # 5. 布林带高级特征
        print("5. 创建布林带高级特征...")
        
        if 'BB_percent' in self.optimized_features.columns and 'BB_width' in self.optimized_features.columns:
            # 布林带挤压突破信号
            self.optimized_features['bb_squeeze_breakout'] = (
                (self.optimized_features['BB_width'] < self.optimized_features['BB_width'].rolling(20).quantile(0.2)) &
                (self.optimized_features['BB_percent'] > 0.8)
            ).astype(int)
            
            # 布林带位置变化率
            self.optimized_features['bb_percent_change'] = self.optimized_features['BB_percent'].pct_change()
        
        # 6. 市场状态特征
        print("6. 创建市场状态特征...")
        
        # 趋势强度
        if 'ADX' in self.optimized_features.columns:
            self.optimized_features['trend_strength'] = np.where(
                self.optimized_features['ADX'] > 25,  # 强趋势
                np.where(
                    self.optimized_features['PLUS_DI'] > self.optimized_features['MINUS_DI'],
                    2,  # 强上升趋势
                    0   # 强下降趋势
                ),
                1  # 震荡市场
            )
        
        # 7. 时间周期特征组合
        print("7. 创建时间周期特征组合...")
        
        if 'hour' in self.optimized_features.columns:
            # 亚洲-伦敦-纽约时段动量差异
            asian_mask = self.optimized_features['is_asian_session'] == 1
            london_mask = self.optimized_features['is_london_session'] == 1
            ny_mask = self.optimized_features['is_ny_session'] == 1
            
            for period in [5, 10]:
                col_name = f'returns_{period}'
                if col_name in self.optimized_features.columns:
                    self.optimized_features[f'{col_name}_asian_session'] = np.where(
                        asian_mask, self.optimized_features[col_name], 0
                    )
                    self.optimized_features[f'{col_name}_london_session'] = np.where(
                        london_mask, self.optimized_features[col_name], 0
                    )
                    self.optimized_features[f'{col_name}_ny_session'] = np.where(
                        ny_mask, self.optimized_features[col_name], 0
                    )
        
        # 8. 相关性增强特征
        print("8. 创建相关性增强特征...")
        
        # MACD与价格偏离的交互
        if 'MACD' in self.optimized_features.columns and 'price_dist_to_SMA_20' in self.optimized_features.columns:
            self.optimized_features['macd_price_synergy'] = (
                self.optimized_features['MACD'] * 
                self.optimized_features['price_dist_to_SMA_20']
            )
        
        # 9. 形态确认特征
        print("9. 创建形态确认特征...")
        
        # 多形态确认
        if all(col in self.optimized_features.columns for col in ['is_hammer', 'bullish_engulfing']):
            self.optimized_features['strong_bullish_confirmation'] = (
                self.optimized_features['is_hammer'] + 
                self.optimized_features['bullish_engulfing']
            )
        
        if all(col in self.optimized_features.columns for col in ['is_shooting_star', 'bearish_engulfing']):
            self.optimized_features['strong_bearish_confirmation'] = (
                self.optimized_features['is_shooting_star'] + 
                self.optimized_features['bearish_engulfing']
            )
        
        # 10. 机器学习友好特征
        print("10. 创建机器学习友好特征...")
        
        # 分箱特征
        if 'RSI' in self.optimized_features.columns:
            self.optimized_features['RSI_binned'] = pd.cut(
                self.optimized_features['RSI'],
                bins=[0, 30, 50, 70, 100],
                labels=[0, 1, 2, 3]
            ).astype(int)
        
        if 'volume_ratio' in self.optimized_features.columns:
            self.optimized_features['volume_binned'] = pd.qcut(
                self.optimized_features['volume_ratio'],
                q=4,
                labels=[0, 1, 2, 3]
            ).astype(int)
        
        # 11. 针对特定目标变量的特征
        print("11. 创建针对目标变量的特征...")
        
        # 根据目标变量类型创建特征
        if 'future' in self.target_col:
            # 判断目标变量是短期、中期还是长期
            if any(x in self.target_col for x in ['_1_', '_3_', '1d_', '3d_']):
                # 短期目标：关注即时技术指标
                if 'RSI' in self.optimized_features.columns:
                    self.optimized_features[f'{self.target_col}_RSI_interaction'] = (
                        self.optimized_features['RSI'] * self.optimized_features['returns']
                    )
            
            elif any(x in self.target_col for x in ['_5_', '_10_', '5d_', '10d_']):
                # 中期目标：关注趋势和动量
                for period in [5, 10, 20]:
                    col_name = f'price_dist_to_SMA_{period}'
                    if col_name in self.optimized_features.columns:
                        self.optimized_features[f'{self.target_col}_trend_alignment'] = (
                            np.sign(self.optimized_features[col_name]) * 
                            np.sign(self.optimized_features['returns'])
                        )
            
            elif any(x in self.target_col for x in ['_20_', '_50_', '20d_', '50d_']):
                # 长期目标：关注基本趋势
                if 'trend_strength' in self.optimized_features.columns:
                    self.optimized_features[f'{self.target_col}_trend_strength'] = (
                        self.optimized_features['trend_strength']
                    )
        
        print(f"高级特征组合创建完成！新增{len(self.optimized_features.columns) - len(self.original_features.columns)}个特征")
        print(f"总特征数量: {len(self.optimized_features.columns)}")
        
        return self.optimized_features
    
    def remove_low_variance_features(self, threshold: float = 0.01):
        """移除低方差特征"""
        print(f"\n移除方差低于{threshold}的特征...")
        
        # 分离特征和目标
        exclude_cols = [self.target_col, 'log_returns', 'returns']
        exclude_cols = [col for col in exclude_cols if col in self.optimized_features.columns]
        
        X = self.optimized_features.drop(columns=exclude_cols, errors='ignore')
        
        # 计算方差
        variances = X.var()
        low_variance_features = variances[variances < threshold].index.tolist()
        
        print(f"发现{len(low_variance_features)}个低方差特征")
        if low_variance_features:
            print(f"示例: {low_variance_features[:10]}...")
        
        # 移除低方差特征
        self.optimized_features = self.optimized_features.drop(columns=low_variance_features)
        print(f"移除后特征数量: {len(self.optimized_features.columns)}")
        
        return low_variance_features
    
    def remove_highly_correlated_features(self, correlation_threshold: float = 1.01):
        """移除高度相关的特征"""
        print(f"\n移除相关性高于{correlation_threshold}的特征...")
        
        # 分离特征（排除目标变量和基础收益变量）
        exclude_cols = [self.target_col, 'log_returns', 'returns']
        exclude_cols = [col for col in exclude_cols if col in self.optimized_features.columns]
        
        feature_cols = [col for col in self.optimized_features.columns if col not in exclude_cols]
        X = self.optimized_features[feature_cols]
        
        # 计算相关性矩阵
        corr_matrix = X.corr().abs()
        
        # 找到高度相关的特征对
        upper = corr_matrix.where(np.triu(np.ones(corr_matrix.shape), k=1).astype(bool))
        to_drop = [column for column in upper.columns if any(upper[column] > correlation_threshold)]
        
        print(f"发现{len(to_drop)}个高度相关特征")
        if to_drop:
            print(f"示例: {to_drop[:10]}...")
        
        # 移除高度相关特征
        self.optimized_features = self.optimized_features.drop(columns=to_drop)
        print(f"移除后特征数量: {len(self.optimized_features.columns)}")
        
        return to_drop
    
    def select_features_by_importance(self, method: str = 'mutual_info', top_k: int = 50):
        """基于重要性选择特征"""
        print(f"\n使用{method}方法选择前{top_k}个重要特征...")
        
        # 准备数据
        exclude_cols = [self.target_col, 'log_returns', 'returns']
        exclude_cols = [col for col in exclude_cols if col in self.optimized_features.columns]
        
        X = self.optimized_features.drop(columns=exclude_cols, errors='ignore')
        y = self.optimized_features[self.target_col]
        
        # 检查是否有足够的非空值
        valid_idx = y.notnull() & X.notnull().all(axis=1)
        X = X[valid_idx]
        y = y[valid_idx]
        
        if len(X) == 0:
            print("错误: 没有有效的数据进行特征选择")
            return pd.DataFrame()
        
        # 处理缺失值
        X = X.fillna(X.mean())
        
        # 根据目标变量类型选择特征选择方法
        is_classification = any(x in self.target_col for x in ['_up', '_class', 'breakout'])
        
        if is_classification:
            # 分类任务
            from sklearn.feature_selection import f_classif, mutual_info_classif
            if method == 'mutual_info':
                selector = SelectKBest(score_func=mutual_info_classif, k=min(top_k, X.shape[1]))
            else:
                selector = SelectKBest(score_func=f_classif, k=min(top_k, X.shape[1]))
        else:
            # 回归任务
            if method == 'mutual_info':
                selector = SelectKBest(score_func=mutual_info_regression, k=min(top_k, X.shape[1]))
            elif method == 'f_regression':
                selector = SelectKBest(score_func=f_regression, k=min(top_k, X.shape[1]))
            else:
                raise ValueError(f"未知的特征选择方法: {method}")
        
        # 拟合选择器
        selector.fit(X, y)
        
        # 获取选择的特征
        selected_mask = selector.get_support()
        self.selected_features = X.columns[selected_mask].tolist()
        
        # 获取特征得分
        feature_scores = pd.DataFrame({
            'feature': X.columns,
            'score': selector.scores_,
            'selected': selected_mask
        }).sort_values('score', ascending=False)
        
        print(f"\n目标变量类型: {'分类' if is_classification else '回归'}")
        print("\n特征重要性排名(前20):")
        print(feature_scores.head(20))
        
        # 只保留选择的特征和目标变量
        cols_to_keep = self.selected_features + exclude_cols
        cols_to_keep = [col for col in cols_to_keep if col in self.optimized_features.columns]
        
        self.optimized_features = self.optimized_features[cols_to_keep]
        
        print(f"特征选择完成！保留{len(self.selected_features)}个特征")
        
        return feature_scores
    
    def apply_pca_for_dimensionality_reduction(self, n_components: int = 20, variance_threshold: float = 0.95):
        """应用PCA进行降维"""
        print(f"\n应用PCA降维，目标维度: {n_components}，保留{variance_threshold*100}%方差...")
        
        # 准备数据
        exclude_cols = [self.target_col, 'log_returns', 'returns']
        exclude_cols = [col for col in exclude_cols if col in self.optimized_features.columns]
        
        X = self.optimized_features.drop(columns=exclude_cols, errors='ignore')
        
        # 标准化
        X_scaled = self.scaler.fit_transform(X)
        
        # 应用PCA
        pca = PCA(n_components=min(n_components, X.shape[1]))
        X_pca = pca.fit_transform(X_scaled)
        
        # 计算累计方差解释率
        cumulative_variance = np.cumsum(pca.explained_variance_ratio_)
        
        # 找到解释指定方差所需的主成分数量
        n_components_needed = np.argmax(cumulative_variance >= variance_threshold) + 1
        print(f"解释{variance_threshold*100}%方差需要{n_components_needed}个主成分")
        
        # 创建PCA特征
        pca_features = pd.DataFrame(
            X_pca[:, :n_components],
            columns=[f'PCA_{i+1}' for i in range(n_components)],
            index=self.optimized_features.index
        )
        
        # 合并PCA特征和原始特征
        result_df = pd.concat([
            self.optimized_features[exclude_cols],
            pca_features
        ], axis=1)
        
        # 添加最重要的原始特征
        if len(self.selected_features) > 0:
            # 选择前10个最重要的原始特征
            important_features = self.selected_features[:10]
            for feat in important_features:
                if feat in X.columns:
                    result_df[feat] = X[feat]
        
        print(f"PCA完成！前{n_components}个主成分解释方差: {cumulative_variance[n_components-1]*100:.2f}%")
        print(f"降维后特征数量: {len(result_df.columns)}")
        
        return result_df, pca
    
    def create_lag_features_for_selected(self, lags: list = [1, 2, 3, 5, 10, 20]):
        """为选择的特征创建滞后特征"""
        print(f"\n为选择的特征创建滞后特征: {lags}...")
        
        # 排除目标变量和基础收益变量
        exclude_cols = [self.target_col, 'log_returns', 'returns']
        exclude_cols = [col for col in exclude_cols if col in self.optimized_features.columns]
        
        # 只对连续型数值特征创建滞后
        numeric_cols = self.optimized_features.select_dtypes(include=[np.number]).columns
        numeric_cols = [col for col in numeric_cols if col not in exclude_cols]
        
        # 选择部分重要特征创建滞后（排除未来收益率特征）
        features_to_lag = [col for col in numeric_cols if 'future_' not in col][:20]  # 只对前20个非未来特征创建滞后
        
        for col in features_to_lag:
            for lag in lags:
                lag_col_name = f'{col}_lag_{lag}'
                self.optimized_features[lag_col_name] = self.optimized_features[col].shift(lag)
        
        # 填充NaN值
        self.optimized_features = self.optimized_features.ffill().bfill()
        
        print(f"创建了{len(features_to_lag) * len(lags)}个滞后特征")
        print(f"总特征数量: {len(self.optimized_features.columns)}")
        
        return self.optimized_features
    
    def analyze_feature_correlations_with_target(self):
        """分析特征与目标变量的相关性"""
        print(f"\n分析特征与目标变量 '{self.target_col}' 的相关性...")
        
        if self.target_col not in self.optimized_features.columns:
            print(f"目标列 {self.target_col} 不存在")
            return None
        
        # 计算相关性
        correlations = self.optimized_features.corr()[self.target_col].sort_values(ascending=False)
        
        print(f"\n与目标变量相关性最高的20个特征:")
        for i, (feature, corr) in enumerate(correlations.head(21).items(), 1):
            if feature != self.target_col:
                print(f"{i:2d}. {feature:30s}: {corr:.6f}")
        
        print(f"\n与目标变量相关性最低的20个特征:")
        for i, (feature, corr) in enumerate(correlations.tail(20).items(), 1):
            print(f"{i:2d}. {feature:30s}: {corr:.6f}")
        
        # 绘制相关性热图
        try:
            import matplotlib.pyplot as plt
            import seaborn as sns
            
            # 选择相关性最高的特征
            top_features = correlations.index[1:21]  # 排除目标变量自身
            correlation_matrix = self.optimized_features[top_features].corr()
            
            plt.figure(figsize=(12, 10))
            sns.heatmap(correlation_matrix, annot=True, cmap='coolwarm', center=0, 
                       fmt='.2f', square=True, cbar_kws={"shrink": .8})
            plt.title(f'Top 20 Features Correlation Matrix (Target: {self.target_col})')
            plt.tight_layout()
            plt.savefig(f'feature_correlation_{self.target_col}.png', dpi=100)
            print(f"\n相关性热图已保存为 feature_correlation_{self.target_col}.png")
            plt.close()
            
        except ImportError:
            print("Matplotlib或Seaborn未安装，跳过可视化")
        
        return correlations
    
    def get_feature_clusters(self, n_clusters: int = 10):
        """基于相关性对特征进行聚类"""
        print(f"\n基于相关性对特征进行聚类 (n_clusters={n_clusters})...")
        
        try:
            from scipy.cluster.hierarchy import dendrogram, linkage, fcluster
            from scipy.spatial.distance import squareform
            
            # 排除目标变量和基础收益变量
            exclude_cols = [self.target_col, 'log_returns', 'returns']
            exclude_cols = [col for col in exclude_cols if col in self.optimized_features.columns]
            
            feature_cols = [col for col in self.optimized_features.columns if col not in exclude_cols]
            
            if len(feature_cols) < 2:
                print("特征数量不足，无法进行聚类")
                return None
            
            # 计算特征间的距离（1 - 相关性绝对值）
            corr_matrix = self.optimized_features[feature_cols].corr().abs()
            distance_matrix = 1 - corr_matrix
            
            # 层次聚类
            linked = linkage(squareform(distance_matrix), 'average')
            
            # 获取聚类标签
            clusters = fcluster(linked, n_clusters, criterion='maxclust')
            
            # 创建特征聚类映射
            feature_clusters = pd.DataFrame({
                'feature': feature_cols,
                'cluster': clusters
            })
            
            # 统计每个聚类的特征数量
            cluster_counts = feature_clusters['cluster'].value_counts().sort_index()
            
            print(f"\n特征聚类分布:")
            for cluster_id, count in cluster_counts.items():
                features_in_cluster = feature_clusters[feature_clusters['cluster'] == cluster_id]['feature'].tolist()
                print(f"聚类 {cluster_id}: {count}个特征")
                if features_in_cluster:
                    print(f"  代表性特征: {features_in_cluster[:5]}...")
            
            return feature_clusters
            
        except Exception as e:
            print(f"聚类分析出错: {e}")
            return None
    
    def prepare_train_test_split(self, test_size: float = 0.2, gap_periods: int = 5):
        """准备训练集和测试集分割"""
        print(f"\n准备训练集和测试集分割 (测试集比例: {test_size}, Gap: {gap_periods})...")
        
        # 确保目标变量存在
        if self.target_col not in self.optimized_features.columns:
            raise ValueError(f"目标列 '{self.target_col}' 不存在")
        
        # 分离特征和目标
        exclude_cols = [self.target_col, 'log_returns', 'returns']
        exclude_cols = [col for col in exclude_cols if col in self.optimized_features.columns]
        
        X = self.optimized_features.drop(columns=exclude_cols, errors='ignore')
        y = self.optimized_features[self.target_col]
        
        # 处理缺失值
        X = X.fillna(X.mean())
        y = y.fillna(y.mean())
        
        # 划分训练集和测试集（时间序列，不能随机分割）
        split_idx = int(len(X) * (1 - test_size))
        
        X_train = X.iloc[:split_idx - gap_periods]
        X_test = X.iloc[split_idx:]
        
        y_train = y.iloc[gap_periods:split_idx]
        y_test = y.iloc[split_idx + gap_periods:]
        
        print(f"训练集形状: X={X_train.shape}, y={y_train.shape}")
        print(f"测试集形状: X={X_test.shape}, y={y_test.shape}")
        
        return X_train, X_test, y_train, y_test
    
    def optimize_all(self, 
                    remove_periods: int = 1000,
                    variance_threshold: float = 0.01,
                    correlation_threshold: float = 0.95,
                    top_k_features: int = 50,
                    use_pca: bool = True,
                    n_pca_components: int = 30):
        """执行完整的特征优化流程"""
        print("=" * 60)
        print(f"开始完整的特征优化流程")
        print(f"目标变量: {self.target_col}")
        print("=" * 60)
        
        # 0. 分析目标变量相关性
        self.analyze_target_correlations()
        
        # 1. 移除早期不稳定数据
        self.remove_early_unstable_data(remove_periods)
        
        # 2. 创建高级特征组合
        self.create_advanced_feature_combinations()
        
        # 3. 移除低方差特征
        self.remove_low_variance_features(variance_threshold)
        
        # 4. 移除高度相关特征
        self.remove_highly_correlated_features(correlation_threshold)
        
        # 5. 基于重要性选择特征
        feature_scores = self.select_features_by_importance(method='mutual_info', top_k=top_k_features)
        
        # 6. 创建滞后特征
        self.create_lag_features_for_selected()
        
        # 7. 应用PCA降维（可选）
        if use_pca and len(self.optimized_features.columns) > n_pca_components + 10:
            result_df, pca_model = self.apply_pca_for_dimensionality_reduction(
                n_components=n_pca_components
            )
            self.optimized_features = result_df
        else:
            pca_model = None
        
        # 8. 分析特征相关性
        correlations = self.analyze_feature_correlations_with_target()
        
        # 9. 特征聚类分析
        feature_clusters = self.get_feature_clusters()
        
        # 10. 准备训练测试数据
        try:
            X_train, X_test, y_train, y_test = self.prepare_train_test_split()
        except Exception as e:
            print(f"准备训练测试数据时出错: {e}")
            X_train, X_test, y_train, y_test = None, None, None, None
        
        print("\n" + "=" * 60)
        print("特征优化完成!")
        print(f"原始特征数量: {len(self.original_features.columns)}")
        print(f"优化后特征数量: {len(self.optimized_features.columns)}")
        print(f"目标变量: {self.target_col}")
        
        if y_train is not None and y_test is not None:
            print(f"训练集样本数: {len(y_train)}")
            print(f"测试集样本数: {len(y_test)}")
        
        print("=" * 60)
        
        return {
            'optimized_features': self.optimized_features,
            'feature_scores': feature_scores,
            'correlations': correlations,
            'feature_clusters': feature_clusters,
            'pca_model': pca_model,
            'train_test_split': (X_train, X_test, y_train, y_test) if X_train is not None else None
        }


# 使用示例
if __name__ == "__main__":
    # 加载之前生成的特征数据
    try:
        features_df = pd.read_csv('full_data_with_features_and_targets.csv', index_col=0, parse_dates=True)
        print(f"成功加载特征数据，形状: {features_df.shape}")
    except FileNotFoundError:
        print("特征文件未找到，请先运行特征工程代码")
        exit(1)
    
    # 显示可用的目标变量
    print("\n" + "=" * 60)
    print("可用的未来收益率目标变量:")
    print("=" * 60)
    
    # 获取所有未来收益率列
    future_cols = [col for col in features_df.columns if 'future_' in col]
    print(f"总共找到 {len(future_cols)} 个未来收益率特征")
    
    # 按类型显示
    targets_by_type = {
        '短期收益率 (1-3期)': [col for col in future_cols if any(x in col for x in ['_1_', '_3_', '1d_', '3d_'])],
        '中期收益率 (5-10期)': [col for col in future_cols if any(x in col for x in ['_5_', '_10_', '5d_', '10d_'])],
        '长期收益率 (20期+)': [col for col in future_cols if any(x in col for x in ['_20_', '_50_', '20d_', '50d_'])],
        '分类目标': [col for col in future_cols if any(x in col for x in ['_up', '_class', 'breakout'])],
        '风险调整目标': [col for col in future_cols if any(x in col for x in ['sharpe', 'sortino', 'volatility', 'drawdown'])],
        '价格极值目标': [col for col in future_cols if any(x in col for x in ['high', 'low', 'upside', 'downside'])]
    }
    
    for target_type, cols in targets_by_type.items():
        if cols:
            print(f"\n{target_type} ({len(cols)}个):")
            for col in cols[:5]:  # 只显示前5个
                print(f"  - {col}")
            if len(cols) > 5:
                print(f"  ... 还有{len(cols)-5}个")
    
    print("\n" + "=" * 60)
    
    # 选择目标变量
    # 可以根据需求修改这里的target_col
    target_choices = {
        '1': 'future_1_return',      # 1期收益率
        '2': 'future_10_return',     # 10期收益率
        '3': 'future_10d_cum_return', # 10天累计收益率
        '4': 'future_1_up',          # 1期是否上涨
        '5': 'future_10d_sharpe',    # 10天夏普比率
        '6': 'future_10d_upside_potential'  # 10天上行潜力
    }
    
    print("请选择目标变量:")
    for key, value in target_choices.items():
        print(f"  {key}. {value}")
    
    choice = input("\n输入选择编号 (默认1): ").strip()
    
    if choice == '':
        target_col = target_choices['1']
    elif choice in target_choices:
        target_col = target_choices[choice]
    else:
        # 使用用户输入的直接列名
        target_col = choice
        if target_col not in features_df.columns:
            print(f"警告: 列 '{target_col}' 不存在，使用默认目标 'future_1_return'")
            target_col = 'future_1_return'
    
    print(f"\n选择的目标变量: {target_col}")
    
    # 初始化特征优化器
    optimizer = FeatureOptimizer(features_df, target_col=target_col)
    
    # 执行完整的特征优化流程
    results = optimizer.optimize_all(
        remove_periods=1000,
        variance_threshold=0.01,
        correlation_threshold=0.95,
        top_k_features=60,
        use_pca=True,
        n_pca_components=25
    )
    
    # 获取优化后的特征
    optimized_features = results['optimized_features']
    
    # 保存优化后的特征
    safe_target_name = target_col.replace('future_', '').replace('_', '-')
    output_file = f'/LocalSSD/p9056/TestTools_ANALY/data/optimized_features_target_{safe_target_name}.csv'
    optimized_features.to_csv(output_file)
    print(f"\n优化后的特征已保存为 '{output_file}'")
    
    # 保存特征重要性
    if 'feature_scores' in results and results['feature_scores'] is not None:
        importance_file = f'/LocalSSD/p9056/TestTools_ANALY/data/feature_importance_target_{safe_target_name}.csv'
        results['feature_scores'].to_csv(importance_file)
        print(f"特征重要性分数已保存为 '{importance_file}'")
    
    # 显示优化后的特征信息
    print("\n优化后的特征概览:")
    print(f"数据形状: {optimized_features.shape}")
    print(f"目标变量: {target_col}")
    
    print(f"\n目标变量统计:")
    print(f"  均值: {optimized_features[target_col].mean():.6f}")
    print(f"  标准差: {optimized_features[target_col].std():.6f}")
    print(f"  最小值: {optimized_features[target_col].min():.6f}")
    print(f"  最大值: {optimized_features[target_col].max():.6f}")
    print(f"  非空值数量: {optimized_features[target_col].notnull().sum()}")
    
    # 创建特征重要性可视化
    try:
        import matplotlib.pyplot as plt
        
        # 绘制特征重要性
        if 'feature_scores' in results and results['feature_scores'] is not None:
            feature_scores = results['feature_scores']
            if len(feature_scores) > 0:
                top_30 = feature_scores.head(30)
                
                plt.figure(figsize=(12, 8))
                bars = plt.barh(range(len(top_30)), top_30['score'].values)
                plt.yticks(range(len(top_30)), top_30['feature'].values)
                plt.xlabel('Feature Importance Score')
                plt.title(f'Top 30 Feature Importance Scores (Target: {target_col})')
                plt.gca().invert_yaxis()  # 反转y轴使最高分数在顶部
                
                # 添加数值标签
                for i, (bar, score) in enumerate(zip(bars, top_30['score'].values)):
                    plt.text(score, i, f' {score:.2f}', va='center')
                
                plt.tight_layout()
                importance_plot_file = f'feature_importance_target_{safe_target_name}.png'
                plt.savefig(importance_plot_file, dpi=100, bbox_inches='tight')
                print(f"\n特征重要性图已保存为 '{importance_plot_file}'")
                plt.close()
        
    except ImportError:
        print("\nMatplotlib未安装，跳过可视化")
    
    # 批量优化不同目标变量（可选）
    print("\n" + "=" * 60)
    print("是否要批量优化多个目标变量？")
    batch_choice = input("输入 'y' 开始批量优化，其他键跳过: ").strip().lower()
    
    if batch_choice == 'y':
        # 选择要优化的目标变量列表
        batch_targets = [
            'future_1_return',           # 短期收益
            'future_10_return',          # 中期收益
            'future_10d_cum_return',     # 中期累计收益
            'future_1_up',               # 短期涨跌
            'future_10d_sharpe',         # 风险调整收益
            'future_10d_upside_potential' # 上行潜力
        
        
        # 只保留数据中存在的目标变量
        batch_targets = [t for t in batch_targets if t in features_df.columns]
        
        print(f"\n开始批量优化 {len(batch_targets)} 个目标变量...")
        
        for i, batch_target in enumerate(batch_targets, 1):
            print(f"\n[{i}/{len(batch_targets)}] 优化目标变量: {batch_target}")
            
            try:
                # 为每个目标变量创建新的优化器
                batch_optimizer = FeatureOptimizer(features_df, target_col=batch_target)
                
                # 执行优化（简化版，避免重复计算）
                batch_results = batch_optimizer.optimize_all(
                    remove_periods=1000,
                    variance_threshold=0.01,
                    correlation_threshold=0.95,
                    top_k_features=40,
                    use_pca=False,  # 批量时不使用PCA
                    n_pca_components=20
                )
                
                # 保存结果
                batch_safe_name = batch_target.replace('future_', '').replace('_', '-')
                batch_output_file = f'/LocalSSD/p9056/TestTools_ANALY/data/batch_optimized_target_{batch_safe_name}.csv'
                batch_optimizer.optimized_features.to_csv(batch_output_file)
                
                print(f"  已保存: {batch_output_file}")
                
            except Exception as e:
                print(f"  优化失败: {e}")
                continue
        
        print("\n批量优化完成！")
    
    print("\n特征优化流程全部完成!")
