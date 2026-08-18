#merge_csv.py
import pandas as pd
import numpy as np
from datetime import timedelta

CANONICAL_OUTPUT_DIR = "D:/Documents/z_python_data_analy/Quent/workspace_0503/data_new/with_features_0601"


def prepare_multi_timeframe_data(
    path_4h: str,
    path_daily: str,
    path_weekly: str
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    将4小时、日线、周线数据合并为4小时研究宽表，并产出轻量上下文表。

    参数:
        path_4h   : 4小时数据CSV文件路径
        path_daily: 日线数据CSV文件路径
        path_weekly: 周线数据CSV文件路径

    返回:
        (研究版宽表, 上下文版宽表)
    """
    # 1. 读取数据
    df_4h = pd.read_csv(path_4h)
    df_daily = pd.read_csv(path_daily)
    df_weekly = pd.read_csv(path_weekly)

    # 2. 统一时间戳格式
    df_4h['timestamp'] = pd.to_datetime(df_4h['timestamp'])
    df_daily['timestamp'] = pd.to_datetime(df_daily['timestamp'])
    df_weekly['timestamp'] = pd.to_datetime(df_weekly['timestamp'])

    # 3. 对4小时数据添加日期和周起始列（用于匹配）
    df_4h['date'] = df_4h['timestamp'].dt.normalize()
    df_4h['week_start'] = df_4h['timestamp'].dt.to_period('W-MON').dt.start_time

    # 4. 处理日线数据：滞后一期，使其适用于下一个交易日
    df_daily['date'] = df_daily['timestamp'].dt.normalize()
    df_daily['apply_date'] = df_daily['date'] + pd.Timedelta(days=1)
    df_daily['daily_source_timestamp'] = df_daily['timestamp']

    daily_rename = {
        col: f'daily_{col}' for col in df_daily.columns
        if col not in ['timestamp', 'date', 'apply_date', 'daily_source_timestamp']
    }
    df_daily = df_daily.rename(columns=daily_rename)

    daily_feature_columns = [
        c for c in df_daily.columns
        if c.startswith('daily_') and c != 'daily_source_timestamp'
    ]

    # 5. 处理周线数据：滞后一期，使其适用于下一周
    df_weekly['week_start'] = df_weekly['timestamp'].dt.to_period('W-MON').dt.start_time
    df_weekly['apply_week_start'] = df_weekly['week_start'] + pd.Timedelta(days=7)
    df_weekly['weekly_source_timestamp'] = df_weekly['timestamp']

    weekly_rename = {
        col: f'weekly_{col}' for col in df_weekly.columns
        if col not in ['timestamp', 'week_start', 'apply_week_start', 'weekly_source_timestamp']
    }
    df_weekly = df_weekly.rename(columns=weekly_rename)

    weekly_feature_columns = [
        c for c in df_weekly.columns
        if c.startswith('weekly_') and c != 'weekly_source_timestamp'
    ]

    # 6. 合并数据（左连接）
    df_merged = df_4h.merge(
        df_daily[['apply_date', 'daily_source_timestamp'] + daily_feature_columns],
        left_on='date',
        right_on='apply_date',
        how='left'
    )
    df_merged.drop(columns=['apply_date'], inplace=True)

    df_merged = df_merged.merge(
        df_weekly[['apply_week_start', 'weekly_source_timestamp'] + weekly_feature_columns],
        left_on='week_start',
        right_on='apply_week_start',
        how='left'
    )
    df_merged.drop(columns=['apply_week_start'], inplace=True)

    # 7. 按时间戳排序，确保时序正确
    df_merged.sort_values('timestamp', inplace=True)
    df_merged.reset_index(drop=True, inplace=True)

    # 8. 规范研究表中的轻量上下文列命名，避免双前缀
    rename_context_columns = {
        'daily_daily_trend_bias': 'daily_trend_bias',
        'daily_daily_trend_strength': 'daily_trend_strength',
        'daily_daily_filter_pass_long': 'daily_filter_pass_long',
        'daily_daily_filter_pass_short': 'daily_filter_pass_short',
        'weekly_weekly_trend_bias': 'weekly_trend_bias',
        'weekly_weekly_trend_strength': 'weekly_trend_strength',
        'weekly_weekly_vol_regime': 'weekly_vol_regime',
        'weekly_weekly_above_long_ma': 'weekly_above_long_ma',
    }
    existing_rename_context_columns = {
        old: new for old, new in rename_context_columns.items() if old in df_merged.columns
    }
    if existing_rename_context_columns:
        df_merged.rename(columns=existing_rename_context_columns, inplace=True)

    # 9. 构建轻量上下文表
    context_columns = [
        'timestamp', 'Open', 'High', 'Low', 'Close', 'Volume',
        'daily_source_timestamp', 'weekly_source_timestamp',
        'daily_trend_bias', 'daily_trend_strength', 'daily_filter_pass_long', 'daily_filter_pass_short',
        'weekly_trend_bias', 'weekly_trend_strength', 'weekly_vol_regime', 'weekly_above_long_ma'
    ]
    existing_context_columns = [c for c in context_columns if c in df_merged.columns]
    df_context = df_merged[existing_context_columns].copy()

    # 10. 删除研究表中的临时辅助列
    df_merged.drop(columns=['date', 'week_start'], inplace=True)

    return df_merged, df_context


# 使用示例
if __name__ == "__main__":
    path_4h = f"{CANONICAL_OUTPUT_DIR}/B_4h_with_features.csv"
    path_daily = f"{CANONICAL_OUTPUT_DIR}/B_daily_with_features.csv"
    path_weekly = f"{CANONICAL_OUTPUT_DIR}/B_weekly_with_features.csv"

    df_research, df_context = prepare_multi_timeframe_data(path_4h, path_daily, path_weekly)
    print("研究版宽表形状:", df_research.shape)
    print("上下文宽表形状:", df_context.shape)
    print("研究版列名示例:\n", df_research.columns[:10].tolist())
    print("\n研究版数据预览:\n", df_research.head())

    df_research.to_csv(f"{CANONICAL_OUTPUT_DIR}/merged_4h_research.csv", index=False)
    df_context.to_csv(f"{CANONICAL_OUTPUT_DIR}/merged_4h_context.csv", index=False)
    print(f"研究版宽表已保存到: {CANONICAL_OUTPUT_DIR}/merged_4h_research.csv")
    print(f"上下文宽表已保存到: {CANONICAL_OUTPUT_DIR}/merged_4h_context.csv")
