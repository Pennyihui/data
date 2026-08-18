import pandas as pd

def resample_ohlcv_data(input_file, output_file, interval='D'):
    """
    将高频K线数据重采样为低频数据
    
    参数:
    input_file: 输入文件路径
    output_file: 输出文件路径
    interval: 重采样间隔，可以是：
              'D' - 日线
              'W' - 周线
              'M' - 月线
              '4H' - 4小时线等
    """
    
    # 读取数据
    df = pd.read_csv(input_file)
    
    # 确保Open Time是datetime类型
    df['Open Time'] = pd.to_datetime(df['Open Time'])
    
    # 设置索引
    df.set_index('Open Time', inplace=True)
    
    # 定义聚合规则
    agg_dict = {
        'Open': 'first',
        'High': 'max',
        'Low': 'min',
        'Close': 'last',
        'Volume': 'sum',
        'Close Time': 'last',
        'Quote Asset Volume': 'sum',
        'Number of Trades': 'sum',
        'Taker Buy Base Asset Volume': 'sum',
        'Taker Buy Quote Asset Volume': 'sum',
        'Ignore': 'last'
    }
    
    # 执行重采样
    resampled_df = df.resample(interval).agg(agg_dict)
    
    # 清理数据：删除没有完整数据的日期
    resampled_df = resampled_df.dropna()
    
    # 重置索引
    resampled_df.reset_index(inplace=True)
    
    # 保存结果
    resampled_df.to_csv(output_file, index=False)
    
    print(f"数据已从1小时重采样为{interval}间隔")
    print(f"原始数据条数: {len(df)}")
    print(f"重采样后条数: {len(resampled_df)}")
    print(f"结果已保存到: {output_file}")
    
    return resampled_df

# 使用示例
daily_data = resample_ohlcv_data(
    input_file='/LocalSSD/p9056/TestTools_ANALY/data/b_1h.csv',
    output_file='/LocalSSD/p9056/TestTools_ANALY/data/b_1d.csv',
    interval='D'
)

# # 如果需要其他时间间隔，比如4小时线
# four_hour_data = resample_ohlcv_data(
#     input_file='/LocalSSD/p9056/TestTools_ANALY/data/b_1h.csv',
#     output_file='/LocalSSD/p9056/TestTools_ANALY/data/b_4h.csv',
#     interval='4H'
# )
