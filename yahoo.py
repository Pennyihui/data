import requests
import pandas as pd
import numpy as np
import os

def get_binance_4h_data(symbol='BTCUSDT', limit=2000):
    url = "https://api.binance.com/api/v3/klines"
    params = {
        'symbol': symbol,
        'interval': '4h',  # 直接指定4小时间隔
        'limit': limit       # 最多1000条数据
    }
    response = requests.get(url, params=params)
    data = response.json()
    
    # 数据清洗
    # columns = ['Open Time', 'Open', 'High', 'Low', 'Close', 'Volume']
    columns = [
        'Open Time', 'Open', 'High', 'Low', 'Close', 'Volume',
        'Close Time', 'Quote Asset Volume', 'Number of Trades',
        'Taker Buy Base Asset Volume', 'Taker Buy Quote Asset Volume', 'Ignore'
    ]
    df = pd.DataFrame(data, columns=columns)#[:6]
    df['Open Time'] = pd.to_datetime(df['Open Time'], unit='ms')
    return df

def get_full_4h_data(symbol='ETHUSDT', start_time='2015-01-01', end_time='2025-05-26'):
    url = "https://api.binance.com/api/v3/klines"
    all_data = []
    
    # 时间转换为毫秒级时间戳
    start_ts = int(pd.to_datetime(start_time).timestamp() * 1000)
    end_ts = int(pd.to_datetime(end_time).timestamp() * 1000)
    
    while start_ts < end_ts:
        params = {
            'symbol': symbol,
            'interval': '1h',
            'startTime': start_ts,
            'limit': 1000  # 每次最多取1000条
        }
        response = requests.get(url, params=params)
        data = response.json()
        if not data:
            break
        all_data.extend(data)
        start_ts = data[-1][0] + 1  # 更新起始时间为最后一条的结束时间+1ms
    
    # 数据清洗（与用户原代码一致）
    # columns = ['Open Time', 'Open', 'High', 'Low', 'Close', 'Volume']
    columns = [
        'Open Time', 'Open', 'High', 'Low', 'Close', 'Volume',
        'Close Time', 'Quote Asset Volume', 'Number of Trades',
        'Taker Buy Base Asset Volume', 'Taker Buy Quote Asset Volume', 'Ignore'
    ]
    df = pd.DataFrame(all_data, columns=columns)
    df['Open Time'] = pd.to_datetime(df['Open Time'], unit='ms')
    return df


def enhanced_save(df, filename, chunk_size=None, compression=None):
    """增强版CSV保存函数"""
    # 参数校验
    if not filename.endswith('.csv'):
        filename += '.csv'
    
    # 分块保存逻辑
    if chunk_size and len(df) > chunk_size:
        for i, chunk in enumerate(np.array_split(df, len(df)//chunk_size + 1)):
            chunk.to_csv(
                f"{filename[:-4]}_part{i+1}.csv",
                index=False,
                encoding='utf-8-sig',
                float_format='%.8f'
            )
        return
    
    # 压缩保存
    if compression:
        df.to_csv(filename, compression=compression, index=False)
        return
    
    # 常规保存
    df.to_csv(
        filename,
        index=False,
        encoding='utf-8-sig',
        float_format='%.8f',
        date_format='%Y-%m-%d %H:%M:%S'
    )


# # 示例：获取BTC/USDT最近1000条4小时K线
# btc_4h = get_binance_4h_data()
# print(btc_4h.head())
# btc_4h.to_csv('binance_btc_4h_data_2000.csv', index=False)
# print("数据已保存为 binance_btc_4h_data_2000.csv")

# 使用示例
df = get_full_4h_data()
enhanced_save(df, 'ETH_1h.csv', chunk_size=500000)