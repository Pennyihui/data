import pandas as pd
import numpy as np
import json
from pathlib import Path

# ============================================================
# 1. 数据读取与预处理
# ============================================================
DATA_PATH = r'/LocalSSD/p9056/TestTools_ANALY/data_new/with_features_0601/merged_4h_research.csv'
df = pd.read_csv(DATA_PATH)

# 解析时间戳并排序
df['timestamp'] = pd.to_datetime(df['timestamp'])
df = df.sort_values('timestamp').reset_index(drop=True)

# 创建辅助列用于分组，避免列名冲突
df['date'] = df['timestamp'].dt.date
df['year_week'] = df['timestamp'].dt.isocalendar().apply(lambda x: f"{x.year}_{x.week}", axis=1)

# 提取不同频率的价格序列
price_4h = df['Close'].astype(float).values

# 日线价格：取每天的最后一个4h记录
df_daily = df.groupby('date').last().reset_index()
daily_close_raw = df_daily['daily_Close'].dropna()
price_daily = daily_close_raw.astype(float).values
ts_daily = df_daily.loc[daily_close_raw.index, 'timestamp'].values

# 周线价格：取每周的最后一个4h记录
df_weekly = df.groupby('year_week').last().reset_index()
weekly_close_raw = df_weekly['weekly_Close'].dropna()
price_weekly = weekly_close_raw.astype(float).values
ts_weekly = df_weekly.loc[weekly_close_raw.index, 'timestamp'].values

print(f"数据尺寸: {df.shape}")
print(f"4h价格长度: {len(price_4h)}")
print(f"日线价格长度: {len(price_daily)}")
print(f"周线价格长度: {len(price_weekly)}")

# ============================================================
# 2. 趋势标注算法
# ============================================================
def label_fth(price, H=1, tau_mult=0.0, vol_window=30):
    """
    Future Trend Horizon with volatility-adaptive threshold.
    tau = tau_mult * rolling_vol * sqrt(H)
    tau_mult=0.0 → original behavior (tau=0)
    """
    n = len(price)
    if n <= H:
        return np.array([], dtype=int)

    returns = np.diff(price) / price[:-1]
    rolling_vol = np.zeros(n)
    for t in range(1, n):
        start = max(0, t - vol_window)
        seg = returns[start:t]
        rolling_vol[t] = np.nanstd(seg) if len(seg) > 2 else 0.0

    y = np.zeros(n - H, dtype=int)
    for t in range(n - H):
        ret = (price[t + H] - price[t]) / price[t]
        tau = tau_mult * rolling_vol[t] * np.sqrt(H)
        y[t] = 1 if ret >= tau else 0
    return y

def label_ct(price, omega=0.01):
    """
    Continuous Trend labeling algorithm (corrected).
    omega: fluctuation threshold (e.g. 0.01 for 1%)
    """
    n = len(price)
    if n == 0:
        return np.array([], dtype=int)

    y = np.full(n, -1, dtype=int)
    FP = price[0]
    pH, HT = price[0], 0
    pL, LT = price[0], 0
    Cid = 0
    FP_N = 0

    # First pass: find the **first** valid turning point
    for i in range(n):
        if price[i] > FP * (1 + omega):
            pH, HT, Cid = price[i], i, 1
            FP_N = i
            break
        elif price[i] < FP * (1 - omega):
            pL, LT, Cid = price[i], i, 0
            FP_N = i
            break

    # If no turning point ever found, return empty
    if FP_N == 0:
        return np.array([], dtype=int)

    # Second pass: assign labels from the first turning point onwards
    for i in range(FP_N + 1, n):
        if Cid == 1:                     # currently in uptrend
            if price[i] > pH:
                pH, HT = price[i], i
            elif price[i] < pH * (1 - omega) and LT <= HT:
                # uptrend ends, label the segment
                for j in range(LT + 1, HT + 1):
                    if j < n:
                        y[j] = 1
                pL, LT, Cid = price[i], i, 0
        else:                            # currently in downtrend
            if price[i] < pL:
                pL, LT = price[i], i
            elif price[i] > pL * (1 + omega) and HT <= LT:
                # downtrend ends, label the segment
                for j in range(HT + 1, LT + 1):
                    if j < n:
                        y[j] = 0
                pH, HT, Cid = price[i], i, 1

    # Handle the last unclosed segment
    if Cid == 1 and HT > LT:
        y[LT+1:HT+1] = 1
    elif Cid == 0 and LT > HT:
        y[HT+1:LT+1] = 0

    # Remove leading unlabeled part
    valid = np.where(y != -1)[0]
    if len(valid) > 0:
        return y[valid[0]:].astype(int)
    else:
        return np.array([], dtype=int)

def label_oracle(price, theta=0.001, final_label=0):
    """
    Oracle DP labeling with percentage returns.
    Maximizes risk-adjusted cumulative return with transaction cost theta.
    """
    n = len(price)
    if n < 2:
        return np.array([])

    returns = np.diff(price) / price[:-1]  # percentage returns

    S = np.zeros((n, 2))
    B = np.zeros((n, 2), dtype=int)
    S[0, 0] = 0.0
    S[0, 1] = -1e10  # cannot start in position

    for t in range(1, n):
        ret = returns[t - 1]

        # → position 0 (flat)
        from0 = S[t - 1, 0]
        from1 = S[t - 1, 1] + ret - theta  # close position: collect ret, pay fee
        if from0 >= from1:
            S[t, 0] = from0
            B[t, 0] = 0
        else:
            S[t, 0] = from1
            B[t, 0] = 1

        # → position 1 (long)
        from0 = S[t - 1, 0] - theta  # open position: pay fee
        from1 = S[t - 1, 1] + ret   # hold position: collect ret
        if from0 >= from1:
            S[t, 1] = from0
            B[t, 1] = 0
        else:
            S[t, 1] = from1
            B[t, 1] = 1

    y = np.zeros(n, dtype=int)
    state = final_label
    y[-1] = state
    for t in range(n-1, 0, -1):
        state = B[t, state]
        y[t-1] = state
    return y

# ============================================================
# 3. 收益计算函数
# ============================================================
def cumulative_return_from_labels(y, price, fee=0.001):
    n = min(len(y), len(price))
    if n == 0:
        return 0.0
    cum_return = 1.0
    position = 0
    entry_price = 0.0
    for t in range(n):
        if np.isnan(price[t]):
            continue
        if y[t] == 1 and position == 0:
            entry_price = price[t]
            if entry_price <= 0:
                continue
            position = 1
            cum_return *= (1 - fee)
        elif y[t] == 0 and position == 1:
            if entry_price <= 0:
                continue
            trade_return = (price[t] - entry_price) / entry_price
            cum_return *= (1 + trade_return)
            cum_return *= (1 - fee)
            position = 0
    if position == 1 and entry_price > 0 and not np.isnan(price[-1]):
        trade_return = (price[-1] - entry_price) / entry_price
        cum_return *= (1 + trade_return)
        cum_return *= (1 - fee)
    return cum_return - 1.0

# ============================================================
# 4. 为每个频率生成三种标签
# ============================================================
FEE_RATE = 0.001

labels_config = {
    '4h': {
        'fee': FEE_RATE,
        'H': 1, 'tau_mult': 0.0,
        'omega_mult': 1.0, 'vol_window': 30,
    },
    'daily': {
        'fee': FEE_RATE,
        'H': 5, 'tau_mult': 0.3,
        'omega_mult': 1.5, 'vol_window': 20,
    },
    'weekly': {
        'fee': FEE_RATE,
        'H': 2, 'tau_mult': 0.3,
        'omega_mult': 1.5, 'vol_window': 12,
    },
}

labels_config['4h']['price'] = price_4h
labels_config['daily']['price'] = price_daily
labels_config['weekly']['price'] = price_weekly

labels_dict = {}

for freq, config in labels_config.items():
    p = config['price']
    fee = config['fee']

    # CT: compute omega from rolling volatility (data-driven)
    rets = np.diff(p) / p[:-1]
    if len(rets) > config['vol_window']:
        rv = pd.Series(rets).rolling(config['vol_window']).std().dropna().values
        omega = config['omega_mult'] * np.median(rv)
    else:
        omega = 0.02  # fallback

    print(f"\n生成 {freq} 标签... (omega={omega*100:.2f}%, H={config['H']}, tau_mult={config['tau_mult']})")
    labels_fth = label_fth(p, H=config['H'], tau_mult=config['tau_mult'])
    labels_ct = label_ct(p, omega=omega)
    labels_oracle = label_oracle(p, theta=fee, final_label=0)

    labels_dict[freq] = {
        'FTH': labels_fth,
        'CT': labels_ct,
        'Oracle': labels_oracle
    }
    print(f"  FTH: {len(labels_fth)}, CT: {len(labels_ct)}, Oracle: {len(labels_oracle)}")

# ============================================================
# 5. 将标签对齐并附加到4h主DataFrame
# ============================================================
# 4h标签（直接附加，长度截断）
min_len_4h = min(len(price_4h), len(labels_dict['4h']['FTH']), len(labels_dict['4h']['CT']), len(labels_dict['4h']['Oracle']))
df_4h_labeled = df.iloc[:min_len_4h].copy()
df_4h_labeled['label_4h_FTH'] = labels_dict['4h']['FTH'][:min_len_4h]
df_4h_labeled['label_4h_CT'] = labels_dict['4h']['CT'][:min_len_4h]
df_4h_labeled['label_4h_Oracle'] = labels_dict['4h']['Oracle'][:min_len_4h]

# 日线标签（通过date列合并）
min_len_d = min(len(price_daily), len(labels_dict['daily']['FTH']), len(labels_dict['daily']['CT']), len(labels_dict['daily']['Oracle']))
df_daily_labels = df_daily[['date']].iloc[:min_len_d].copy()
df_daily_labels['label_daily_FTH'] = labels_dict['daily']['FTH'][:min_len_d]
df_daily_labels['label_daily_CT'] = labels_dict['daily']['CT'][:min_len_d]
df_daily_labels['label_daily_Oracle'] = labels_dict['daily']['Oracle'][:min_len_d]
df_4h_labeled = df_4h_labeled.merge(df_daily_labels, on='date', how='left')

# 周线标签（通过year_week列合并）
min_len_w = min(len(price_weekly), len(labels_dict['weekly']['FTH']), len(labels_dict['weekly']['CT']), len(labels_dict['weekly']['Oracle']))
df_weekly_labels = df_weekly[['year_week']].iloc[:min_len_w].copy()
df_weekly_labels['label_weekly_FTH'] = labels_dict['weekly']['FTH'][:min_len_w]
df_weekly_labels['label_weekly_CT'] = labels_dict['weekly']['CT'][:min_len_w]
df_weekly_labels['label_weekly_Oracle'] = labels_dict['weekly']['Oracle'][:min_len_w]
df_4h_labeled = df_4h_labeled.merge(df_weekly_labels, on='year_week', how='left')

# 保存最终数据集
OUTPUT_PATH = r'/LocalSSD/p9056/TestTools_ANALY/data_new/with_features_0601/B_multilabeled_of_feature_trend_v2.csv'
df_4h_labeled.to_csv(OUTPUT_PATH, index=False)
print(f"\n带多时间频率标签的数据已保存至: {OUTPUT_PATH}")

label_config_output = {
    'input_dataset': DATA_PATH,
    'fee_rate': FEE_RATE,
    'labels_config': {
        key: {k: v for k, v in value.items() if k != 'price'}
        for key, value in labels_config.items()
    }
}

output_path = Path(OUTPUT_PATH)
config_path = output_path.with_name('label_config.json')
with open(config_path, 'w', encoding='utf-8') as f:
    json.dump(label_config_output, f, ensure_ascii=False, indent=2)
print(f'label config saved to: {config_path}')

# ============================================================
# 6. 自测
# ============================================================
print("\n========== 自测 ==========")
for freq in ['4h', 'daily', 'weekly']:
    p = labels_config[freq]['price']
    for method in ['FTH', 'CT', 'Oracle']:
        y = labels_dict[freq][method]
        if len(y) < 10:
            print(f"{freq} {method}: 标签不足，跳过")
            continue
        # CT从有效起始点截取，与价格对不齐：用最后len(y)个价格匹配
        if method == 'CT':
            ret = cumulative_return_from_labels(y, p[-len(y):], FEE_RATE)
        else:
            ret = cumulative_return_from_labels(y, p[:len(y)], FEE_RATE)
        print(f"{freq} {method}: 前{len(y)}标签收益={ret*100:.2f}%, 标签1占比={np.mean(y):.2%}")

# ============================================================
# 7. 鲁棒性评估 (论文Section III-IV方法)
# ============================================================
def apply_noise(y, desired_acc):
    """论文Eq.9: ŷ = ¬(y ⊕ m), m~Bernoulli(ψ)"""
    n = len(y)
    flip_mask = np.random.binomial(1, 1 - desired_acc, size=n)
    return np.where(flip_mask, 1 - y, y)

def simulate_expected_return(y_true, price, desired_acc, fee=0.001, n_sim=200):
    returns = []
    for _ in range(n_sim):
        y_noisy = apply_noise(y_true, desired_acc)
        ret = cumulative_return_from_labels(y_noisy, price, fee)
        returns.append(ret)
    return np.mean(returns), np.std(returns)

def compute_robustness_profile(y_true, price, fee=0.001, n_sim=200):
    """
    论文方法: 13个ψ水平(1.00→0.54, 每步相对-5%), 在每个区间计算ρ
    ρ(ψ_i) = (R(ψ_{i+1})/R(ψ_i)-1) / (ψ_{i+1}/ψ_i-1)   论文Eq.7
    """
    n_steps = 12
    psi = 1.0
    profile = []
    np.random.seed(42)

    for step in range(n_steps + 1):
        R, R_std = simulate_expected_return(y_true, price, psi, fee, n_sim)
        entry = {'psi': psi, 'R_mean': R, 'R_std': R_std}

        if step < n_steps:
            psi_next = psi * 0.95
            R_next, _ = simulate_expected_return(y_true, price, psi_next, fee, n_sim)
            delta_R_R = (R_next - R) / R if R != 0 else 0
            delta_psi_psi = (psi_next - psi) / psi
            entry['rho'] = delta_R_R / delta_psi_psi if delta_psi_psi != 0 else 0

        profile.append(entry)

        if step < n_steps:
            psi = psi * 0.95

    return profile

N_SIM = 200

for freq in ['4h', 'daily', 'weekly']:
    price_seq = labels_config[freq]['price']
    print(f"\n--- {freq} 鲁棒性曲线 (论文方法: 相对ΔACC=-5%/步) ---")

    methods_profiles = {}
    for method in ['FTH', 'CT', 'Oracle']:
        y_true = labels_dict[freq][method]
        if len(y_true) < 10:
            print(f"{method}: 标签不足，跳过")
            continue
        if method == 'CT':
            p = price_seq[-len(y_true):]
        else:
            p = price_seq[:len(y_true)]

        profile = compute_robustness_profile(y_true, p, FEE_RATE, N_SIM)
        methods_profiles[method] = profile

    # 表头
    header = f"{'ψ':>6}"
    for m in methods_profiles:
        header += f" {m+' R':>14} {m+' ρ':>9}"
    print(header)
    print('-' * len(header))

    # 每一行: 一个ψ水平
    for step in range(12):
        row = f"{methods_profiles[list(methods_profiles.keys())[0]][step]['psi']:>5.3f}"
        for m in methods_profiles:
            p = methods_profiles[m][step]
            row += f" {p['R_mean']*100:>12.2e} {p.get('rho', 0):>9.2f}"
        print(row)

    # 最后一行: ψ=0.54
    row = f"{methods_profiles[list(methods_profiles.keys())[0]][12]['psi']:>5.3f}"
    for m in methods_profiles:
        p = methods_profiles[m][12]
        row += f" {p['R_mean']*100:>12.2e} {'':>9}"
    print(row)

    # 总结: 在实际可达到的ACC水平附近的ρ
    print(f"\n  弹性总结 (ρ=1%准确率变化→ρ%收益变化):")
    for m in methods_profiles:
        rhos = [p.get('rho', 0) for p in methods_profiles[m][:-1]]
        print(f"    {m}: ρ∈[{min(rhos):.1f}, {max(rhos):.1f}], 均值={np.mean(rhos):.1f}")

# ============================================================
# 8. 日线标签可视化
# ============================================================
import matplotlib.pyplot as plt
from matplotlib.patches import Patch
import os

VIS_DIR = r'/LocalSSD/p9056/TestTools_ANALY/pipeline/2_label/visualization'
os.makedirs(VIS_DIR, exist_ok=True)

freq = 'daily'
p = labels_config[freq]['price']
ts = ts_daily
ld = labels_dict[freq]

# 确定每类标签的起始对齐位置
segs = {}
for m in ['FTH', 'CT', 'Oracle']:
    y = ld[m]
    if m == 'CT':
        offset = len(p) - len(y)
    else:
        offset = 0
    segs[m] = {'labels': y, 'offset': offset}

window = 400
n_total = len(p)
win_start = max(0, n_total - window)
win_end = n_total

fig, axes = plt.subplots(4, 1, figsize=(18, 8), sharex=True,
                         gridspec_kw={'height_ratios': [3, 1, 1, 1]})

# --- 价格曲线 ---
ax0 = axes[0]
ax0.plot(range(win_start, win_end), p[win_start:win_end],
         color='black', linewidth=0.8)
ax0.set_ylabel('Price', fontsize=10)
ax0.set_title('Daily - Three Trend Labeling Methods', fontsize=14, fontweight='bold')
ax0.grid(True, alpha=0.3)
ax0.set_xlim(win_start, win_end)

# --- 三种标签条 ---
bar_configs = [
    ('FTH', '#2ecc71', axes[1]),
    ('CT',  '#3498db', axes[2]),
    ('Oracle', '#e74c3c', axes[3]),
]

for m_name, color, ax in bar_configs:
    s = segs[m_name]
    y = s['labels']
    offset = s['offset']

    lo = max(0, win_start - offset)
    hi = min(len(y), win_end - offset)
    x_start = offset + lo
    x_end = offset + hi
    x = np.arange(x_start, x_end)
    y_seg = y[lo:hi]

    ax.fill_between(x, 0, 1, where=(y_seg == 1),
                    color='green', alpha=0.4, step='mid')
    ax.fill_between(x, 0, 1, where=(y_seg == 0),
                    color='red', alpha=0.15, step='mid')

    if m_name == 'CT' and offset > win_start:
        ax.axvspan(win_start, min(offset, win_end),
                   color='gray', alpha=0.12)

    ax.set_ylim(-0.1, 1.2)
    ax.set_yticks([0, 1])
    ax.set_yticklabels(['0', '1'], fontsize=8)
    ax.set_ylabel(m_name, fontsize=10, fontweight='bold', color=color)
    ax.set_xlim(win_start, win_end)

# --- X 轴时间标签 ---
n_ticks = 10
step = max(1, (win_end - win_start) // n_ticks)
tick_pos = list(range(win_start, win_end, step))
if tick_pos[-1] != win_end - 1:
    tick_pos.append(win_end - 1)

ts_win = ts[win_start:win_end]
axes[-1].set_xticks(tick_pos)
axes[-1].set_xticklabels(
    [pd.Timestamp(ts_win[t - win_start]).strftime('%Y-%m-%d')
     for t in tick_pos],
    rotation=35, fontsize=8, ha='right'
)
axes[-1].set_xlabel('Date', fontsize=10)

# 图例
legend = [
    Patch(facecolor='green', alpha=0.4, label='Uptrend (label=1)'),
    Patch(facecolor='red', alpha=0.15, label='Downtrend (label=0)'),
    Patch(facecolor='gray', alpha=0.12, label='CT undefined'),
]
axes[0].legend(handles=legend, loc='upper left', fontsize=9, framealpha=0.9)

plt.tight_layout()
save_path = os.path.join(VIS_DIR, 'daily_label_comparison.png')
plt.savefig(save_path, dpi=150, bbox_inches='tight')
plt.close()
print(f"\n日线标签可视化已保存至: {save_path}")
