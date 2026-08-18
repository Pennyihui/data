# produce_csv.py
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import os
import warnings
import json
warnings.filterwarnings('ignore')

# 导入重构后的指标计算模块
# 导入重构后的指标计算模块
from calculate_indicator_v2 import (
    WeeklyIndicators,  # 新增
    DailyIndicators,
    Hourly4HIndicators,
    Hourly1HIndicators,
    FeatureValidator
)

CANONICAL_OUTPUT_DIR = r"D:/Documents/z_python_data_analy/Quent/workspace_0503/data_new/with_features_0601"


def classify_feature_category(column_name: str) -> str:
    if column_name in {'Open', 'High', 'Low', 'Close', 'Volume', 'quote_volume', 'trades', 'taker_buy_volume', 'taker_buy_quote_volume'}:
        return 'raw'
    if column_name.startswith('label_'):
        return 'label'
    if any(x in column_name for x in ['filter_pass', 'trend_bias', 'trend_strength', 'vol_regime', 'above_long_ma']):
        return 'context'
    if 'Div' in column_name or 'Divergence' in column_name:
        return 'divergence'
    if any(x in column_name for x in ['MACD', 'EMA', 'ADX', 'Trend', 'Kijun', 'Tenkan', 'Pivot', 'Resistance', 'Support']):
        return 'trend'
    if any(x in column_name for x in ['RSI', 'STOCH', 'CCI', 'WILLR', 'MOM', 'ROC']):
        return 'momentum'
    if any(x in column_name for x in ['BB', 'ATR', 'Volatility', 'Keltner', 'Donchian']):
        return 'volatility'
    if any(x in column_name for x in ['Volume', 'OBV', 'MFI', 'Buy_', 'Sell_', 'Trade_Size']):
        return 'volume'
    if any(x in column_name for x in ['Body', 'Shadow', 'Doji', 'Hammer', 'Candle']):
        return 'candle'
    if any(x in column_name for x in ['Hour', 'DayOfWeek', 'Month', 'Quarter', 'Seasonal']):
        return 'time'
    return 'other'


def build_feature_schema(dataset_name: str, timeframe: str, df: pd.DataFrame) -> list[dict]:
    rows = []
    for col in df.columns:
        rows.append({
            'dataset': dataset_name,
            'feature_name': col,
            'timeframe': timeframe,
            'category': classify_feature_category(col),
            'source_table': timeframe,
            'dtype': str(df[col].dtype),
            'non_null_count': int(df[col].notnull().sum()),
            'null_count': int(df[col].isnull().sum()),
            'is_lagged': False,
        })
    return rows


def add_multi_timeframe_divergence(data_dict: dict) -> dict:
    """
    为各时间框架数据添加多周期背离协同特征
    data_dict: 包含各时间框架DataFrame的字典，键为 '1h','4h','daily','weekly'
    返回更新后的data_dict
    """
    print("\n计算多周期背离协同特征...")
    timeframes = ['1h', '4h', 'daily', 'weekly']
    for tf in timeframes:
        if tf not in data_dict:
            print(f"警告: {tf} 数据不存在，跳过")
            return data_dict
        df = data_dict[tf]
        prefix = f'{tf}_' if tf != '1h' else '1h_'
        col = f'{prefix}Div_Type'
        if col not in df.columns:
            print(f"警告: {tf} 数据缺少 {col} 列，将创建全0列")
            df[col] = 0

    # 确保索引有序
    for tf in timeframes:
        data_dict[tf] = data_dict[tf].sort_index()

    # 获取所有时间戳并集
    all_timestamps = data_dict['1h'].index.union(data_dict['4h'].index).union(
        data_dict['daily'].index).union(data_dict['weekly'].index)
    all_timestamps = pd.DatetimeIndex(sorted(all_timestamps))

    # 将各周期Div_Type对齐到最高频率（前向填充）
    div_aligned = {}
    for tf in timeframes:
        prefix = f'{tf}_' if tf != '1h' else '1h_'
        col = f'{prefix}Div_Type'
        div_aligned[tf] = data_dict[tf][col].reindex(all_timestamps, method='ffill')

    # 为每个时间框架添加协同特征
    for target_tf in timeframes:
        df = data_dict[target_tf]
        target_prefix = f'{target_tf}_' if target_tf != '1h' else '1h_'
        target_idx = df.index
        aligned_data = {tf: div_aligned[tf].loc[target_idx] for tf in timeframes}

        align_counts = np.zeros(len(df))
        align_details = {tf: np.zeros(len(df)) for tf in timeframes if tf != target_tf}
  
        for i, idx in enumerate(target_idx):
            current_div = aligned_data[target_tf].iloc[i]
            if current_div == 0:
                continue
            count = 0
            for other_tf in timeframes:
                if other_tf == target_tf:
                    continue
                other_div = aligned_data[other_tf].iloc[i]
                # 判断背离类型是否一致（1/3看涨，2/4看跌）
                if (current_div in [1,3] and other_div in [1,3]) or (current_div in [2,4] and other_div in [2,4]):
                    align_details[other_tf][i] = 1
                    count += 1
            align_counts[i] = count

        df[f'{target_prefix}Div_MultiTimeframe_Count'] = align_counts
        for other_tf in timeframes:
            if other_tf != target_tf:
                df[f'{target_prefix}Div_Align_{other_tf}'] = align_details[other_tf]
        df[f'{target_prefix}Div_MultiTimeframe_Align'] = (align_counts >= 1).astype(int)

        data_dict[target_tf] = df

    print("多周期背离协同特征添加完成")
    return data_dict

def load_btc_data(filepath: str) -> pd.DataFrame:
    """
    加载BTC原始数据
    
    Args:
        filepath: CSV文件路径
        
    Returns:
        处理后的DataFrame
    """
    print(f"正在加载数据: {filepath}")
    
    # 读取CSV文件
    df = pd.read_csv(filepath)
    
    print(f"原始数据形状: {df.shape}")
    print(f"数据列: {df.columns.tolist()}")
    print(f"数据时间范围: {df['Open Time'].min()} 到 {df['Open Time'].max()}")
    
    # 重命名列以符合标准格式
    column_mapping = {
        'Open Time': 'timestamp',
        'Open': 'Open',
        'High': 'High',
        'Low': 'Low',
        'Close': 'Close',
        'Volume': 'Volume',
        'Close Time': 'close_timestamp',
        'Quote Asset Volume': 'quote_volume',
        'Number of Trades': 'trades',
        'Taker Buy Base Asset Volume': 'taker_buy_volume',
        'Taker Buy Quote Asset Volume': 'taker_buy_quote_volume'
    }
    
    df = df.rename(columns=column_mapping)
    
    # 将时间戳转换为datetime类型
    df['timestamp'] = pd.to_datetime(df['timestamp'])
    
    # 设置时间戳为索引
    df.set_index('timestamp', inplace=True)

    # 确保时间顺序
    df = df.sort_index()
    df.attrs['source_path'] = filepath
    
    # 基本数据清理
    print(f"清理前数据形状: {df.shape}")
    
    # 检查缺失值
    missing_values = df[['Open', 'High', 'Low', 'Close', 'Volume']].isnull().sum()
    print(f"缺失值统计:\n{missing_values}")
    
    # 填充缺失值（如果有）
    df = df.fillna(method='ffill').fillna(method='bfill')
    
    # 移除重复的时间戳
    df = df[~df.index.duplicated(keep='first')]
    
    print(f"清理后数据形状: {df.shape}")
    print(f"最终数据时间范围: {df.index.min()} 到 {df.index.max()}")
    
    return df

def resample_to_daily(df_1h: pd.DataFrame, agg_dict: dict = None) -> pd.DataFrame:
    """
    将1小时数据重采样为日线数据
    """
    print("重采样为日线数据...")
    if not isinstance(df_1h.index, pd.DatetimeIndex):
        df_1h.index = pd.to_datetime(df_1h.index)

    # 默认聚合规则：OHLCV及额外字段
    default_agg = {
        'Open': 'first',
        'High': 'max',
        'Low': 'min',
        'Close': 'last',
        'Volume': 'sum',
        'quote_volume': 'sum',
        'trades': 'sum',
        'taker_buy_volume': 'sum',
        'taker_buy_quote_volume': 'sum'
    }
    if agg_dict:
        default_agg.update(agg_dict)

    daily_data = df_1h.resample('D').agg(default_agg)
    daily_data = daily_data.dropna(subset=['Close'])
    print(f"日线数据形状: {daily_data.shape}")
    return daily_data

def resample_to_4h(df_1h: pd.DataFrame, agg_dict: dict = None) -> pd.DataFrame:
    """
    将1小时数据重采样为4小时数据
    """
    print("重采样为4小时数据...")
    if not isinstance(df_1h.index, pd.DatetimeIndex):
        df_1h.index = pd.to_datetime(df_1h.index)

    default_agg = {
        'Open': 'first',
        'High': 'max',
        'Low': 'min',
        'Close': 'last',
        'Volume': 'sum',
        'quote_volume': 'sum',
        'trades': 'sum',
        'taker_buy_volume': 'sum',
        'taker_buy_quote_volume': 'sum'
    }
    if agg_dict:
        default_agg.update(agg_dict)

    four_hour_data = df_1h.resample('4H').agg(default_agg)
    four_hour_data = four_hour_data.dropna(subset=['Close'])
    print(f"4小时数据形状: {four_hour_data.shape}")
    return four_hour_data

def resample_to_weekly(df_1h: pd.DataFrame, agg_dict: dict = None) -> pd.DataFrame:
    """
    将1小时数据重采样为周线数据（周一为开始）
    """
    print("重采样为周线数据...")
    if not isinstance(df_1h.index, pd.DatetimeIndex):
        df_1h.index = pd.to_datetime(df_1h.index)

    default_agg = {
        'Open': 'first',
        'High': 'max',
        'Low': 'min',
        'Close': 'last',
        'Volume': 'sum',
        'quote_volume': 'sum',
        'trades': 'sum',
        'taker_buy_volume': 'sum',
        'taker_buy_quote_volume': 'sum'
    }
    if agg_dict:
        default_agg.update(agg_dict)

    weekly_data = df_1h.resample('W-MON').agg(default_agg)
    weekly_data = weekly_data.dropna(subset=['Close'])
    print(f"周线数据形状: {weekly_data.shape}")
    return weekly_data

def calculate_features_for_all_timeframes(df_1h: pd.DataFrame, 
                                         save_dir: str = None) -> dict:
    """
    计算所有时间框架的特征
    
    Args:
        df_1h: 原始1小时数据
        save_dir: 保存目录
        
    Returns:
        包含所有时间框架数据的字典
    """
    # 创建保存目录
    if save_dir and not os.path.exists(save_dir):
        os.makedirs(save_dir)
    
    print("=" * 80)
    print("开始计算技术指标特征")
    print("=" * 80)
    
    # 1. 重采样数据
    print("\n1. 数据重采样")
    print("-" * 40)
    
    # 获取周线数据
    weekly_data = resample_to_weekly(df_1h)
    
    # 获取日线数据
    daily_data = resample_to_daily(df_1h)
    
    # 获取4小时数据
    four_hour_data = resample_to_4h(df_1h)
    
    # 原始1小时数据 - 保留后续特征计算所需的全部关键原始字段
    hourly_required_columns = [
        'Open', 'High', 'Low', 'Close', 'Volume',
        'quote_volume', 'trades', 'taker_buy_volume', 'taker_buy_quote_volume'
    ]
    hourly_available_columns = [col for col in hourly_required_columns if col in df_1h.columns]
    hourly_data = df_1h[hourly_available_columns].copy()
    
    print(f"\n数据统计:")
    print(f"周线数据: {weekly_data.shape[0]} 行")
    print(f"日线数据: {daily_data.shape[0]} 行")
    print(f"4小时数据: {four_hour_data.shape[0]} 行")
    print(f"1小时数据: {hourly_data.shape[0]} 行")
    
    # 2. 计算周线特征
    print("\n2. 计算周线特征")
    print("-" * 40)
    
    weekly_indicator = WeeklyIndicators()
    weekly_with_features = weekly_indicator.calculate(weekly_data)
    
    print(f"周线特征计算完成，总列数: {weekly_with_features.shape[1]}")
    
    # 3. 计算日线特征
    print("\n3. 计算日线特征")
    print("-" * 40)
    
    daily_indicator = DailyIndicators()
    daily_with_features = daily_indicator.calculate(daily_data)
    
    print(f"日线特征计算完成，总列数: {daily_with_features.shape[1]}")
    
    # 4. 计算4小时特征
    print("\n4. 计算4小时特征")
    print("-" * 40)
    
    hourly4h_indicator = Hourly4HIndicators()
    four_hour_with_features = hourly4h_indicator.calculate(four_hour_data)
    
    print(f"4小时特征计算完成，总列数: {four_hour_with_features.shape[1]}")
    
    # 5. 计算1小时特征
    print("\n5. 计算1小时特征")
    print("-" * 40)
    
    hourly1h_indicator = Hourly1HIndicators()
    hourly_with_features = hourly1h_indicator.calculate(hourly_data)
    
    print(f"1小时特征计算完成，总列数: {hourly_with_features.shape[1]}")
    
    # 6. 验证特征（检查未来数据泄露）
    print("\n6. 特征验证")
    print("-" * 40)
    
    validator = FeatureValidator()
    
    # 验证周线特征
    print("\n验证周线特征...")
    weekly_validation = validator.validate_no_future_data(weekly_with_features)
    print(f"周线特征验证结果: {len(weekly_validation['safe_columns'])}个安全列，"
          f"{len(weekly_validation['suspicious_columns'])}个可疑列")
    
    # 验证日线特征
    print("\n验证日线特征...")
    daily_validation = validator.validate_no_future_data(daily_with_features)
    print(f"日线特征验证结果: {len(daily_validation['safe_columns'])}个安全列，"
          f"{len(daily_validation['suspicious_columns'])}个可疑列")
    
    # 验证4小时特征
    print("\n验证4小时特征...")
    four_hour_validation = validator.validate_no_future_data(four_hour_with_features)
    print(f"4小时特征验证结果: {len(four_hour_validation['safe_columns'])}个安全列，"
          f"{len(four_hour_validation['suspicious_columns'])}个可疑列")
    
    # 验证1小时特征
    print("\n验证1小时特征...")
    hourly_validation = validator.validate_no_future_data(hourly_with_features)
    print(f"1小时特征验证结果: {len(hourly_validation['safe_columns'])}个安全列，"
          f"{len(hourly_validation['suspicious_columns'])}个可疑列")

    # 7. 添加多周期背离协同特征
    print("\n7. 计算多周期背离协同特征")
    print("-" * 40)
    
    # 收集结果字典
    results = {
        'weekly': weekly_with_features,
        'daily': daily_with_features,
        '4h': four_hour_with_features,
        '1h': hourly_with_features,
        'weekly_validation': weekly_validation,
        'daily_validation': daily_validation,
        '4h_validation': four_hour_validation,
        '1h_validation': hourly_validation
    }
    
    # 添加多周期背离协同特征
    results = add_multi_timeframe_divergence(results)
    
    # 更新变量引用
    weekly_with_features = results['weekly']
    daily_with_features = results['daily']
    four_hour_with_features = results['4h']
    hourly_with_features = results['1h']

    # 8. 保存数据
    if save_dir:
        print("\n8. 保存数据")
        print("-" * 40)
        
        # 保存周线数据
        weekly_output_path = os.path.join(save_dir, "B_weekly_with_features.csv")
        weekly_with_features.to_csv(weekly_output_path)
        print(f"周线数据保存到: {weekly_output_path}")
        print(f"周线数据形状: {weekly_with_features.shape}")
        
        # 保存日线数据
        daily_output_path = os.path.join(save_dir, "B_daily_with_features.csv")
        daily_with_features.to_csv(daily_output_path)
        print(f"日线数据保存到: {daily_output_path}")
        print(f"日线数据形状: {daily_with_features.shape}")
        
        # 保存4小时数据
        four_hour_output_path = os.path.join(save_dir, "B_4h_with_features.csv")
        four_hour_with_features.to_csv(four_hour_output_path)
        print(f"4小时数据保存到: {four_hour_output_path}")
        print(f"4小时数据形状: {four_hour_with_features.shape}")
        
        # 保存1小时数据
        hourly_output_path = os.path.join(save_dir, "B_1h_with_features.csv")
        hourly_with_features.to_csv(hourly_output_path)
        print(f"1小时数据保存到: {hourly_output_path}")
        print(f"1小时数据形状: {hourly_with_features.shape}")
        
        # 保存数据统计信息
        stats_output_path = os.path.join(save_dir, "data_statistics.txt")
        with open(stats_output_path, 'w', encoding='utf-8') as f:
            f.write("BTC数据特征计算统计\n")
            f.write("=" * 60 + "\n\n")
            
            f.write(f"原始1小时数据行数: {df_1h.shape[0]}\n")
            f.write(f"原始数据时间范围: {df_1h.index.min()} 到 {df_1h.index.max()}\n\n")
            
            f.write("周线数据:\n")
            f.write(f"  行数: {weekly_with_features.shape[0]}\n")
            f.write(f"  列数: {weekly_with_features.shape[1]}\n")
            f.write(f"  时间范围: {weekly_with_features.index.min()} 到 {weekly_with_features.index.max()}\n")
            f.write(f"  特征列数: {len([c for c in weekly_with_features.columns if 'weekly_' in c])}\n\n")
            
            f.write("日线数据:\n")
            f.write(f"  行数: {daily_with_features.shape[0]}\n")
            f.write(f"  列数: {daily_with_features.shape[1]}\n")
            f.write(f"  时间范围: {daily_with_features.index.min()} 到 {daily_with_features.index.max()}\n")
            f.write(f"  特征列数: {len([c for c in daily_with_features.columns if 'daily_' in c])}\n\n")
            
            f.write("4小时数据:\n")
            f.write(f"  行数: {four_hour_with_features.shape[0]}\n")
            f.write(f"  列数: {four_hour_with_features.shape[1]}\n")
            f.write(f"  时间范围: {four_hour_with_features.index.min()} 到 {four_hour_with_features.index.max()}\n")
            f.write(f"  特征列数: {len([c for c in four_hour_with_features.columns if '4h_' in c])}\n\n")
            
            f.write("1小时数据:\n")
            f.write(f"  行数: {hourly_with_features.shape[0]}\n")
            f.write(f"  列数: {hourly_with_features.shape[1]}\n")
            f.write(f"  时间范围: {hourly_with_features.index.min()} 到 {hourly_with_features.index.max()}\n")
            f.write(f"  特征列数: {len([c for c in hourly_with_features.columns if '1h_' in c])}\n\n")
            
            f.write("特征验证结果:\n")
            f.write(f"  周线安全特征: {len(weekly_validation['safe_columns'])}/{weekly_with_features.shape[1]}\n")
            f.write(f"  日线安全特征: {len(daily_validation['safe_columns'])}/{daily_with_features.shape[1]}\n")
            f.write(f"  4小时安全特征: {len(four_hour_validation['safe_columns'])}/{four_hour_with_features.shape[1]}\n")
            f.write(f"  1小时安全特征: {len(hourly_validation['safe_columns'])}/{hourly_with_features.shape[1]}\n")
        
        print(f"数据统计保存到: {stats_output_path}")
        
        # 保存特征列表
        feature_list_path = os.path.join(save_dir, "feature_columns.csv")
        feature_info = []
        
        # 收集所有特征信息
        for col in weekly_with_features.columns:
            if 'weekly_' in col:
                feature_info.append({
                    'timeframe': 'weekly',
                    'feature_name': col,
                    'feature_type': 'technical' if 'weekly_' in col else 'price',
                    'first_value': weekly_with_features[col].iloc[0] if len(weekly_with_features) > 0 else None,
                    'non_null_count': weekly_with_features[col].notnull().sum(),
                    'mean': weekly_with_features[col].mean() if weekly_with_features[col].notnull().any() else None
                })
        
        for col in daily_with_features.columns:
            if 'daily_' in col:
                feature_info.append({
                    'timeframe': 'daily',
                    'feature_name': col,
                    'feature_type': 'technical' if 'daily_' in col else 'price',
                    'first_value': daily_with_features[col].iloc[0] if len(daily_with_features) > 0 else None,
                    'non_null_count': daily_with_features[col].notnull().sum(),
                    'mean': daily_with_features[col].mean() if daily_with_features[col].notnull().any() else None
                })
        
        for col in four_hour_with_features.columns:
            if '4h_' in col:
                feature_info.append({
                    'timeframe': '4h',
                    'feature_name': col,
                    'feature_type': 'technical' if '4h_' in col else 'price',
                    'first_value': four_hour_with_features[col].iloc[0] if len(four_hour_with_features) > 0 else None,
                    'non_null_count': four_hour_with_features[col].notnull().sum(),
                    'mean': four_hour_with_features[col].mean() if four_hour_with_features[col].notnull().any() else None
                })
        
        for col in hourly_with_features.columns:
            if '1h_' in col:
                feature_info.append({
                    'timeframe': '1h',
                    'feature_name': col,
                    'feature_type': 'technical' if '1h_' in col else 'price',
                    'first_value': hourly_with_features[col].iloc[0] if len(hourly_with_features) > 0 else None,
                    'non_null_count': hourly_with_features[col].notnull().sum(),
                    'mean': hourly_with_features[col].mean() if hourly_with_features[col].notnull().any() else None
                })
        
        feature_df = pd.DataFrame(feature_info)
        feature_df.to_csv(feature_list_path, index=False)
        print(f"特征列表保存到: {feature_list_path}")

        schema_rows = []
        schema_rows.extend(build_feature_schema('B_weekly_with_features.csv', 'weekly', weekly_with_features))
        schema_rows.extend(build_feature_schema('B_daily_with_features.csv', 'daily', daily_with_features))
        schema_rows.extend(build_feature_schema('B_4h_with_features.csv', '4h', four_hour_with_features))
        schema_rows.extend(build_feature_schema('B_1h_with_features.csv', '1h', hourly_with_features))

        schema_df = pd.DataFrame(schema_rows)
        schema_csv_path = os.path.join(save_dir, 'feature_schema.csv')
        schema_json_path = os.path.join(save_dir, 'feature_schema.json')
        schema_df.to_csv(schema_csv_path, index=False)
        schema_df.to_json(schema_json_path, orient='records', force_ascii=False, indent=2)
        print(f"特征schema保存到: {schema_csv_path}")
        print(f"特征schema JSON保存到: {schema_json_path}")

        manifest = {
            'input_file': df_1h.attrs.get('source_path'),
            'generated_at': datetime.now().isoformat(),
            'datasets': {
                'weekly': {'rows': int(weekly_with_features.shape[0]), 'cols': int(weekly_with_features.shape[1])},
                'daily': {'rows': int(daily_with_features.shape[0]), 'cols': int(daily_with_features.shape[1])},
                '4h': {'rows': int(four_hour_with_features.shape[0]), 'cols': int(four_hour_with_features.shape[1])},
                '1h': {'rows': int(hourly_with_features.shape[0]), 'cols': int(hourly_with_features.shape[1])},
            }
        }
        manifest_path = os.path.join(save_dir, 'dataset_manifest.json')
        with open(manifest_path, 'w', encoding='utf-8') as f:
            json.dump(manifest, f, ensure_ascii=False, indent=2)
        print(f"数据清单保存到: {manifest_path}")

    print("\n" + "=" * 80)
    print("特征计算完成!")
    print("=" * 80)
    
    return results

def analyze_features(data_dict: dict):
    """
    分析特征数据
    
    Args:
        data_dict: 包含所有时间框架数据的字典
    """
    print("\n" + "=" * 80)
    print("特征分析")
    print("=" * 80)
    
    for timeframe, df in [(k, v) for k, v in data_dict.items() if k in ['weekly', 'daily', '4h', '1h']]:
        print(f"\n{timeframe.upper()}时间框架:")
        print("-" * 40)
        
        # 基本信息
        print(f"数据形状: {df.shape}")
        print(f"时间范围: {df.index.min()} 到 {df.index.max()}")
        
        # 特征分类统计
        price_cols = [col for col in df.columns if col in ['Open', 'High', 'Low', 'Close', 'Volume']]
        feature_cols = [col for col in df.columns if col not in price_cols]
        
        print(f"原始价格列数: {len(price_cols)}")
        print(f"技术特征列数: {len(feature_cols)}")
        
        # 按特征类型统计
        if timeframe == 'weekly':
            prefix = 'weekly_'
        elif timeframe == 'daily':
            prefix = 'daily_'
        elif timeframe == '4h':
            prefix = '4h_'
        else:
            prefix = '1h_'
        
        trend_cols = [col for col in df.columns if prefix in col and any(x in col for x in ['MACD', 'EMA', 'ADX', 'Trend'])]
        momentum_cols = [col for col in df.columns if prefix in col and any(x in col for x in ['RSI', 'STOCH', 'CCI', 'MOM', 'ROC'])]
        volatility_cols = [col for col in df.columns if prefix in col and any(x in col for x in ['BB', 'ATR', 'Volatility'])]
        volume_cols = [col for col in df.columns if prefix in col and any(x in col for x in ['Volume', 'OBV', 'MFI'])]
        divergence_cols = [col for col in df.columns if prefix in col and 'Div' in col]
        
        print(f"趋势特征: {len(trend_cols)}")
        print(f"动量特征: {len(momentum_cols)}")
        print(f"波动率特征: {len(volatility_cols)}")
        print(f"成交量特征: {len(volume_cols)}")
        print(f"背离特征: {len(divergence_cols)}")
        
        # 缺失值统计
        missing_stats = df.isnull().sum()
        missing_features = missing_stats[missing_stats > 0]
        if len(missing_features) > 0:
            print(f"\n有缺失值的特征: {len(missing_features)}个")
            print("缺失值最多的10个特征:")
            print(missing_features.sort_values(ascending=False).head(10))
        else:
            print("\n所有特征都没有缺失值")
        
        # 特征相关性（样本）
        if len(df) > 50:
            # 随机选择10个特征查看相关性
            sample_features = np.random.choice(feature_cols, min(10, len(feature_cols)), replace=False)
            sample_df = df[sample_features].iloc[-50:]  # 最近50个数据点
            
            corr_matrix = sample_df.corr()
            high_corr_pairs = []
            
            for i in range(len(sample_features)):
                for j in range(i+1, len(sample_features)):
                    corr = abs(corr_matrix.iloc[i, j])
                    if corr > 0.8:
                        high_corr_pairs.append((sample_features[i], sample_features[j], corr))
            
            if high_corr_pairs:
                print(f"\n高相关性特征对(相关系数>0.8): {len(high_corr_pairs)}对")
                for pair in high_corr_pairs[:5]:  # 显示前5对
                    print(f"  {pair[0]} 与 {pair[1]}: {pair[2]:.3f}")
            else:
                print("\n未发现高相关性特征对(相关系数>0.8)")

def create_sample_dataset(data_dict: dict, save_dir: str, sample_size: int = 1000):
    """
    创建样本数据集用于测试
    
    Args:
        data_dict: 包含所有时间框架数据的字典
        save_dir: 保存目录
        sample_size: 样本大小
    """
    print(f"\n创建样本数据集(最近{sample_size}行)...")
    
    for timeframe in ['weekly', 'daily', '4h', '1h']:
        if timeframe in data_dict:
            df = data_dict[timeframe]
            # 对于周线数据，样本大小适当减少
            actual_sample_size = min(sample_size, len(df))
            sample_df = df.tail(actual_sample_size)
            
            sample_path = os.path.join(save_dir, f"B_{timeframe}_sample_{actual_sample_size}.csv")
            sample_df.to_csv(sample_path)
            print(f"{timeframe}样本数据保存到: {sample_path}")

def main():
    """主函数"""
    # 配置路径
    input_file = r"D:/Documents/z_python_data_analy/Quent/workspace_0503/data_new/b_1h.csv"
    output_dir = CANONICAL_OUTPUT_DIR
    
    # 检查输入文件是否存在
    if not os.path.exists(input_file):
        print(f"错误: 输入文件不存在: {input_file}")
        return
    
    print("=" * 80)
    print("BTC数据特征计算脚本")
    print("=" * 80)
    print(f"输入文件: {input_file}")
    print(f"输出目录: {output_dir}")
    
    try:
        # 1. 加载数据
        btc_data = load_btc_data(input_file)
        
        # 2. 计算特征
        results = calculate_features_for_all_timeframes(btc_data, output_dir)
        
        # 3. 分析特征
        analyze_features(results)
        
        # 4. 创建样本数据集
        create_sample_dataset(results, output_dir, sample_size=1000)
        
        print("\n" + "=" * 80)
        print("处理完成!")
        print("=" * 80)
        
        # 显示结果摘要
        # 显示结果摘要
        print("\n结果摘要:")
        print(f"周线数据文件: {os.path.join(output_dir, 'B_weekly_with_features.csv')}")
        print(f"日线数据文件: {os.path.join(output_dir, 'B_daily_with_features.csv')}")
        print(f"4小时数据文件: {os.path.join(output_dir, 'B_4h_with_features.csv')}")
        print(f"1小时数据文件: {os.path.join(output_dir, 'B_1h_with_features.csv')}")
        print(f"数据统计文件: {os.path.join(output_dir, 'data_statistics.txt')}")
        print(f"特征列表文件: {os.path.join(output_dir, 'feature_columns.csv')}")
        
    except Exception as e:
        print(f"\n处理过程中发生错误: {str(e)}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    # 记录开始时间
    start_time = datetime.now()
    print(f"脚本开始时间: {start_time}")
    
    # 运行主函数
    main()
    
    # 记录结束时间
    end_time = datetime.now()
    duration = end_time - start_time
    print(f"\n脚本结束时间: {end_time}")
    print(f"总耗时: {duration}")
