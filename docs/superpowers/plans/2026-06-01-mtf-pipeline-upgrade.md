# Multi-Timeframe Pipeline Upgrade Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Upgrade the BTC feature pipeline so it better supports 4h-primary, daily-filter, weekly-context trend-trading research by improving base feature quality, multi-timeframe alignment, schema/metadata outputs, and label pipeline connectivity without deleting broad indicator coverage.

**Architecture:** Keep the existing single-file pipeline structure but clarify responsibilities: `calculate_indicator_v2.py` remains the feature factory, `produce_csv.py` remains the orchestration entrypoint, `merge_csv.py` becomes the authoritative 4h merge/lineage builder, and `2_label/label_of_feature_trend.py` becomes the connected label stage for the new merged datasets. Add richer metadata outputs and a second merged dataset with lightweight context columns while preserving broad research features.

**Tech Stack:** Python, pandas, numpy, TA-Lib, CSV/JSON outputs

---

## File Structure Map

### Existing files to modify
- `produce_csv.py`
  - Keep as the pipeline entrypoint
  - Fix 1h raw field preservation
  - Normalize output behavior
  - Emit richer metadata files
- `calculate_indicator_v2.py`
  - Keep as the feature factory
  - Fix broken feature semantics
  - Remove duplicate divergence entrypoint ambiguity
  - Add lightweight weekly/daily context columns
- `merge_csv.py`
  - Keep as merged 4h dataset builder
  - Add lineage/timing columns
  - Emit research + context datasets
- `2_label/label_of_feature_trend.py`
  - Point to new merged dataset paths
  - Save label config metadata
  - Preserve FTH / CT / Oracle labels

### New files to create
- `docs/superpowers/plans/2026-06-01-mtf-pipeline-upgrade.md`
- `feature_schema.json` (written by pipeline)
- `feature_schema.csv` (written by pipeline)
- `dataset_manifest.json` (written by pipeline)
- `label_config.json` (written by label script)

### Existing outputs to preserve or continue generating
- `B_1h_with_features.csv`
- `B_4h_with_features.csv`
- `B_daily_with_features.csv`
- `B_weekly_with_features.csv`
- `feature_columns.csv`
- multilabel output CSV

### New outputs to add
- `merged_4h_research.csv`
- `merged_4h_context.csv`

---

### Task 1: Fix 1h raw-field preservation in `produce_csv.py`

**Files:**
- Modify: `produce_csv.py:258-269`
- Test: `produce_csv.py` runtime smoke output and generated `B_1h_with_features.csv`

- [ ] **Step 1: Add a narrow failing assertion script for the current 1h field loss**

Create a temporary local check script content to run in a Python one-liner:

```python
import pandas as pd
from produce_csv import load_btc_data

path = "/LocalSSD/p9056/TestTools_ANALY/data_new/b_1h.csv"
df = load_btc_data(path)
required = {"Open", "High", "Low", "Close", "Volume", "quote_volume", "trades", "taker_buy_volume", "taker_buy_quote_volume"}
missing = required - set(df.columns)
assert not missing, missing
print("raw 1h input columns ok")
```

- [ ] **Step 2: Run the check to establish current orchestration drops fields later**

Run:

```bash
python - <<'PY'
from produce_csv import load_btc_data
path = "/LocalSSD/p9056/TestTools_ANALY/data_new/b_1h.csv"
df = load_btc_data(path)
hourly_data = df[['Open', 'High', 'Low', 'Close', 'Volume']].copy()
required = ['quote_volume', 'trades', 'taker_buy_volume', 'taker_buy_quote_volume']
missing = [c for c in required if c not in hourly_data.columns]
print('missing_after_slice=', missing)
assert missing == required
PY
```

Expected: PASS with all four columns listed as missing after the slice.

- [ ] **Step 3: Replace the 1h slicing code with a preserve-all-required-fields block**

Change the 1h selection logic in `produce_csv.py` to:

```python
    # 原始1小时数据 - 保留后续特征计算所需的全部关键原始字段
    hourly_required_columns = [
        'Open', 'High', 'Low', 'Close', 'Volume',
        'quote_volume', 'trades', 'taker_buy_volume', 'taker_buy_quote_volume'
    ]
    hourly_available_columns = [col for col in hourly_required_columns if col in df_1h.columns]
    hourly_data = df_1h[hourly_available_columns].copy()
```

- [ ] **Step 4: Run a direct code-path check to verify the 1h working frame now preserves the fields**

Run:

```bash
python - <<'PY'
from produce_csv import load_btc_data
path = "/LocalSSD/p9056/TestTools_ANALY/data_new/b_1h.csv"
df = load_btc_data(path)
hourly_required_columns = [
    'Open', 'High', 'Low', 'Close', 'Volume',
    'quote_volume', 'trades', 'taker_buy_volume', 'taker_buy_quote_volume'
]
hourly_available_columns = [col for col in hourly_required_columns if col in df.columns]
hourly_data = df[hourly_available_columns].copy()
required = set(hourly_required_columns)
missing = required - set(hourly_data.columns)
assert not missing, missing
print('1h fields preserved')
PY
```

Expected: `1h fields preserved`

- [ ] **Step 5: Commit the field-preservation fix**

```bash
git add produce_csv.py
git commit -m "fix: preserve raw 1h fields for feature generation"
```

---

### Task 2: Fix broken candle-direction semantics in `calculate_indicator_v2.py`

**Files:**
- Modify: `calculate_indicator_v2.py:248-256`, `calculate_indicator_v2.py:2439-2445`
- Test: local Python assertions against body-size fields

- [ ] **Step 1: Write a failing semantic check for signed candle direction**

Run this local check script:

```python
import pandas as pd

sample = pd.DataFrame({
    'Open': [100, 100],
    'Close': [110, 90],
})
body_size = abs(sample['Close'] - sample['Open'])
body_size_pct = body_size / sample['Open'] * 100
assert body_size_pct.iloc[0] > 0
assert body_size_pct.iloc[1] > 0
assert not (body_size_pct.iloc[1] < 0)
print('absolute body size cannot classify red candles')
```

- [ ] **Step 2: Run the semantic check to confirm current limitation**

Run:

```bash
python - <<'PY'
import pandas as pd
sample = pd.DataFrame({'Open': [100, 100], 'Close': [110, 90]})
body_size = abs(sample['Close'] - sample['Open'])
body_size_pct = body_size / sample['Open'] * 100
print(body_size_pct.tolist())
assert body_size_pct.tolist() == [10.0, 10.0]
PY
```

Expected: PASS with `[10.0, 10.0]`

- [ ] **Step 3: Add a signed body feature and update weekly big-green/big-red logic to use it**

In `calculate_indicator_v2.py`, update the candle section to include:

```python
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
```

Then update weekly candle classification to:

```python
        if 'weekly_Signed_Body_Pct' in df.columns:
            signed_body_pct = df['weekly_Signed_Body_Pct']
            df['weekly_Big_Green_Candle'] = (signed_body_pct > 5).astype(int)
            df['weekly_Big_Red_Candle'] = (signed_body_pct < -5).astype(int)
```

- [ ] **Step 4: Run a direct semantic check for the new signed interpretation**

Run:

```bash
python - <<'PY'
import pandas as pd
sample = pd.DataFrame({'Open': [100, 100], 'Close': [110, 90]})
signed_body = sample['Close'] - sample['Open']
signed_body_pct = signed_body / sample['Open'] * 100
assert signed_body_pct.tolist() == [10.0, -10.0]
print('signed candle semantics ok')
PY
```

Expected: `signed candle semantics ok`

- [ ] **Step 5: Commit the candle-semantics fix**

```bash
git add calculate_indicator_v2.py
git commit -m "fix: add signed candle body semantics"
```

---

### Task 3: Remove duplicate divergence entrypoint ambiguity in `calculate_indicator_v2.py`

**Files:**
- Modify: `calculate_indicator_v2.py:822-884`, `calculate_indicator_v2.py:1132-1234`
- Test: file-level grep + import smoke check

- [ ] **Step 1: Confirm the file currently contains duplicate `detect_divergences` definitions**

Run:

```bash
python - <<'PY'
from pathlib import Path
text = Path('calculate_indicator_v2.py').read_text(encoding='utf-8')
count = text.count('def detect_divergences(')
print('detect_divergences_count=', count)
assert count == 2
PY
```

Expected: `detect_divergences_count= 2`

- [ ] **Step 2: Decide the single authoritative implementation path**

Keep the later, richer implementation that includes:
- weighted bullish/bearish scores
- net score
- consecutive divergence features
- `Div_Type`

Delete the earlier duplicate `detect_divergences` block entirely.

- [ ] **Step 3: Remove the earlier duplicate method block**

Delete the entire earlier block beginning with:

```python
    def detect_divergences(self, df: pd.DataFrame, prefix: str = "") -> pd.DataFrame:
```

and ending before:

```python
    def _detect_macd_hist_divergence(self, df: pd.DataFrame, price: np.ndarray,
```

Keep `_detect_macd_hist_divergence`, `_detect_rsi_divergence`, `_detect_volume_divergence`, and the later `detect_divergences`.

- [ ] **Step 4: Run the duplicate-count smoke check again**

Run:

```bash
python - <<'PY'
from pathlib import Path
text = Path('calculate_indicator_v2.py').read_text(encoding='utf-8')
count = text.count('def detect_divergences(')
print('detect_divergences_count=', count)
assert count == 1
PY
```

Expected: `detect_divergences_count= 1`

- [ ] **Step 5: Run an import smoke test**

Run:

```bash
python - <<'PY'
from calculate_indicator_v2 import TechnicalIndicators, DailyIndicators, Hourly4HIndicators, Hourly1HIndicators, WeeklyIndicators
print('import ok')
PY
```

Expected: `import ok`

- [ ] **Step 6: Commit the divergence cleanup**

```bash
git add calculate_indicator_v2.py
git commit -m "refactor: remove duplicate divergence entrypoint"
```

---

### Task 4: Add lightweight weekly and daily context-state columns

**Files:**
- Modify: `calculate_indicator_v2.py:1555-1733`, `calculate_indicator_v2.py:2322-2534`
- Test: small synthetic dataframe checks via the indicator classes

- [ ] **Step 1: Define concrete context columns to add without hard-coding strategy signals**

Use these exact columns:

Weekly:
- `weekly_trend_bias`
- `weekly_trend_strength`
- `weekly_vol_regime`
- `weekly_above_long_ma`

Daily:
- `daily_trend_bias`
- `daily_trend_strength`
- `daily_filter_pass_long`
- `daily_filter_pass_short`

- [ ] **Step 2: Add weekly context formulas based on existing feature families**

Append logic near the end of `calculate_weekly_specific_features()`:

```python
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
```

- [ ] **Step 3: Add daily context formulas based on existing feature families**

Append logic near the end of `calculate_daily_specific_features()`:

```python
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
```

- [ ] **Step 4: Run a synthetic smoke test for daily and weekly context columns**

Run:

```bash
python - <<'PY'
import numpy as np
import pandas as pd
from calculate_indicator_v2 import DailyIndicators, WeeklyIndicators

idx_d = pd.date_range('2024-01-01', periods=260, freq='D')
df_d = pd.DataFrame({
    'Open': np.linspace(100, 200, 260),
    'High': np.linspace(101, 201, 260),
    'Low': np.linspace(99, 199, 260),
    'Close': np.linspace(100, 210, 260),
    'Volume': np.linspace(1000, 2000, 260),
    'quote_volume': np.linspace(10000, 20000, 260),
    'trades': np.linspace(100, 200, 260),
    'taker_buy_volume': np.linspace(500, 1000, 260),
}, index=idx_d)

idx_w = pd.date_range('2020-01-06', periods=160, freq='W-MON')
df_w = pd.DataFrame({
    'Open': np.linspace(100, 200, 160),
    'High': np.linspace(101, 201, 160),
    'Low': np.linspace(99, 199, 160),
    'Close': np.linspace(100, 210, 160),
    'Volume': np.linspace(1000, 2000, 160),
    'quote_volume': np.linspace(10000, 20000, 160),
    'trades': np.linspace(100, 200, 160),
    'taker_buy_volume': np.linspace(500, 1000, 160),
}, index=idx_w)

daily = DailyIndicators().calculate(df_d)
weekly = WeeklyIndicators().calculate(df_w)
for col in ['daily_trend_bias', 'daily_trend_strength', 'daily_filter_pass_long', 'daily_filter_pass_short']:
    assert col in daily.columns, col
for col in ['weekly_trend_bias', 'weekly_trend_strength', 'weekly_vol_regime', 'weekly_above_long_ma']:
    assert col in weekly.columns, col
print('context columns present')
PY
```

Expected: `context columns present`

- [ ] **Step 5: Commit the context-state additions**

```bash
git add calculate_indicator_v2.py
git commit -m "feat: add lightweight daily and weekly context states"
```

---

### Task 5: Add richer schema and manifest outputs in `produce_csv.py`

**Files:**
- Modify: `produce_csv.py:396-489`
- Test: output JSON/CSV metadata files existence and shape

- [ ] **Step 1: Define exact metadata outputs and minimum schema fields**

Emit these files in the output directory:
- `feature_schema.csv`
- `feature_schema.json`
- `dataset_manifest.json`

Use schema fields:
- `dataset`
- `feature_name`
- `timeframe`
- `category`
- `source_table`
- `dtype`
- `non_null_count`
- `null_count`
- `is_lagged`

- [ ] **Step 2: Add a helper to classify columns into categories**

Add this helper near the top of `produce_csv.py`:

```python
def classify_feature_category(column_name: str) -> str:
    if column_name in {'Open', 'High', 'Low', 'Close', 'Volume', 'quote_volume', 'trades', 'taker_buy_volume', 'taker_buy_quote_volume'}:
        return 'raw'
    if column_name.startswith('label_'):
        return 'label'
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
    if any(x in column_name for x in ['filter_pass', 'trend_bias', 'trend_strength', 'vol_regime', 'above_long_ma']):
        return 'context'
    return 'other'
```

- [ ] **Step 3: Add schema/manifest writers after per-timeframe CSV generation**

Add a helper block in `produce_csv.py`:

```python
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
```

Then write outputs:

```python
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

        manifest = {
            'input_file': input_file if 'input_file' in locals() else None,
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
```

Also ensure `datetime`/`json` are imported where needed.

- [ ] **Step 4: Run the full feature pipeline once to generate metadata outputs**

Run:

```bash
python produce_csv.py
```

Expected: output includes saved per-timeframe CSV files plus `feature_schema.csv`, `feature_schema.json`, and `dataset_manifest.json` in the configured output directory.

- [ ] **Step 5: Validate the metadata files exist and are non-empty**

Run:

```bash
python - <<'PY'
from pathlib import Path
base = Path('/LocalSSD/p9056/TestTools_ANALY/data_new/with_features_0601')
for name in ['feature_schema.csv', 'feature_schema.json', 'dataset_manifest.json']:
    path = base / name
    assert path.exists(), path
    assert path.stat().st_size > 0, path
    print(name, 'ok')
PY
```

Expected: all three files print `ok`

- [ ] **Step 6: Commit the metadata-output work**

```bash
git add produce_csv.py
git commit -m "feat: add feature schema and dataset manifest outputs"
```

---

### Task 6: Upgrade `merge_csv.py` to emit research and context datasets with lineage columns

**Files:**
- Modify: `merge_csv.py:5-97`
- Test: generated merged outputs and lineage columns

- [ ] **Step 1: Define the merged output contract**

The script must produce:
- `merged_4h_research.csv`
- `merged_4h_context.csv`

The merged datasets must include lineage fields:
- `daily_source_timestamp`
- `weekly_source_timestamp`

- [ ] **Step 2: Preserve source timestamps during rename/join preparation**

Update daily/weekly prep blocks to retain source timestamps:

```python
    df_daily['date'] = df_daily['timestamp'].dt.normalize()
    df_daily['apply_date'] = df_daily['date'] + pd.Timedelta(days=1)
    df_daily['daily_source_timestamp'] = df_daily['timestamp']
```

```python
    df_weekly['week_start'] = df_weekly['timestamp'].dt.to_period('W-MON').dt.start_time
    df_weekly['apply_week_start'] = df_weekly['week_start'] + pd.Timedelta(days=7)
    df_weekly['weekly_source_timestamp'] = df_weekly['timestamp']
```

When renaming, exclude these lineage fields from prefixed renaming.

- [ ] **Step 3: Merge lineage fields into the 4h table alongside prefixed features**

Update the daily merge slice to:

```python
        df_daily[['apply_date', 'daily_source_timestamp'] + [c for c in df_daily.columns if c.startswith('daily_')]],
```

Update the weekly merge slice to:

```python
        df_weekly[['apply_week_start', 'weekly_source_timestamp'] + [c for c in df_weekly.columns if c.startswith('weekly_')]],
```

- [ ] **Step 4: Build a lightweight context view from the research dataframe**

After the merged research dataframe is built, define context columns:

```python
    context_columns = [
        'timestamp', 'Open', 'High', 'Low', 'Close', 'Volume',
        'daily_source_timestamp', 'weekly_source_timestamp',
        'daily_trend_bias', 'daily_trend_strength', 'daily_filter_pass_long', 'daily_filter_pass_short',
        'weekly_trend_bias', 'weekly_trend_strength', 'weekly_vol_regime', 'weekly_above_long_ma'
    ]
    existing_context_columns = [c for c in context_columns if c in df_merged.columns]
    df_context = df_merged[existing_context_columns].copy()
```

Return both dataframes or return research and save context in the main block.

- [ ] **Step 5: Update the main block to save both merged outputs**

Use:

```python
    df_research, df_context = prepare_multi_timeframe_data(path_4h, path_daily, path_weekly)
    df_research.to_csv('/LocalSSD/p9056/TestTools_ANALY/data_new/with_features/merged_4h_research.csv', index=False)
    df_context.to_csv('/LocalSSD/p9056/TestTools_ANALY/data_new/with_features/merged_4h_context.csv', index=False)
```

Adjust the function return accordingly:

```python
    return df_merged, df_context
```

- [ ] **Step 6: Run the merge script and verify the new files exist**

Run:

```bash
python merge_csv.py
```

Expected: script prints shapes/previews and writes both merged CSV files.

- [ ] **Step 7: Verify lineage columns appear in the research output**

Run:

```bash
python - <<'PY'
import pandas as pd
path = '/LocalSSD/p9056/TestTools_ANALY/data_new/with_features/merged_4h_research.csv'
df = pd.read_csv(path, nrows=5)
for col in ['daily_source_timestamp', 'weekly_source_timestamp']:
    assert col in df.columns, col
print('merge lineage ok')
PY
```

Expected: `merge lineage ok`

- [ ] **Step 8: Commit the merge-output upgrade**

```bash
git add merge_csv.py
git commit -m "feat: add merged research and context datasets"
```

---

### Task 7: Reconnect and parameterize the label pipeline

**Files:**
- Modify: `2_label/label_of_feature_trend.py:6-8`, `213-232`, `286-289`
- Test: generated label config and labeled dataset

- [ ] **Step 1: Point the label script to the new merged research dataset path**

Replace the top-level dataset path with:

```python
DATA_PATH = r'/LocalSSD/p9056/TestTools_ANALY/data_new/with_features/merged_4h_research.csv'
```

- [ ] **Step 2: Centralize label configuration into a serializable object**

Use this exact config block:

```python
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
```

Then populate `price` fields immediately after extracting the price arrays:

```python
labels_config['4h']['price'] = price_4h
labels_config['daily']['price'] = price_daily
labels_config['weekly']['price'] = price_weekly
```

- [ ] **Step 3: Save label metadata next to the labeled dataset**

Add after writing the labeled CSV:

```python
import json
from pathlib import Path

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
```

- [ ] **Step 4: Run the label script**

Run:

```bash
cd 2_label && python label_of_feature_trend.py
```

Expected: labeled dataset is written successfully and `label_config.json` is saved alongside it.

- [ ] **Step 5: Verify both the labeled output and config file exist**

Run:

```bash
python - <<'PY'
from pathlib import Path
base = Path('/LocalSSD/p9056/TestTools_ANALY/data/with_features_0303')
# If OUTPUT_PATH is changed during implementation, update the base path in this check accordingly.
config_candidates = list(base.glob('label_config.json'))
label_candidates = list(base.glob('B_multilabeled_of_feature_trend_v2.csv'))
assert label_candidates, 'missing labeled csv'
assert config_candidates, 'missing label config'
print('label outputs ok')
PY
```

Expected: `label outputs ok`

- [ ] **Step 6: Commit the label-pipeline reconnection**

```bash
git add 2_label/label_of_feature_trend.py
git commit -m "feat: reconnect label pipeline to merged research dataset"
```

---

### Task 8: Run end-to-end verification and update output paths if needed

**Files:**
- Modify: `produce_csv.py`, `merge_csv.py`, `2_label/label_of_feature_trend.py` as needed for path consistency
- Test: end-to-end pipeline run

- [ ] **Step 1: Audit path consistency across feature, merge, and label stages**

Check these expectations:
- `produce_csv.py` writes to one canonical feature directory
- `merge_csv.py` reads from that same feature directory
- `label_of_feature_trend.py` reads the new merged research dataset from the same branch of outputs

Use a short audit script:

```bash
python - <<'PY'
from pathlib import Path
for p in [
    '/LocalSSD/p9056/TestTools_ANALY/data_new/with_features_0601',
    '/LocalSSD/p9056/TestTools_ANALY/data_new/with_features',
    '/LocalSSD/p9056/TestTools_ANALY/data/with_features_0303',
]:
    print(p, Path(p).exists())
PY
```

Expected: identify whether outputs are still fragmented and standardize them in code before final verification.

- [ ] **Step 2: Normalize the final canonical output directory in code**

If fragmentation remains, update scripts so all stages use the same canonical output base, for example:

```python
FEATURE_OUTPUT_DIR = r'/LocalSSD/p9056/TestTools_ANALY/data_new/with_features_0601'
```

Then consistently derive:
- per-timeframe feature files
- merged research/context files
- labeled outputs or label input path

Do not leave produce/merge/label pointing at three unrelated directories.

- [ ] **Step 3: Run the full pipeline in order**

Run:

```bash
python produce_csv.py
python merge_csv.py
cd 2_label && python label_of_feature_trend.py
```

Expected: all three stages complete without path-related failures.

- [ ] **Step 4: Run a final output audit**

Run:

```bash
python - <<'PY'
from pathlib import Path
base_candidates = [
    Path('/LocalSSD/p9056/TestTools_ANALY/data_new/with_features_0601'),
    Path('/LocalSSD/p9056/TestTools_ANALY/data_new/with_features'),
]
for base in base_candidates:
    if base.exists():
        print('checking', base)
        for name in [
            'B_1h_with_features.csv',
            'B_4h_with_features.csv',
            'B_daily_with_features.csv',
            'B_weekly_with_features.csv',
            'feature_schema.csv',
            'feature_schema.json',
            'dataset_manifest.json',
            'merged_4h_research.csv',
            'merged_4h_context.csv',
        ]:
            path = base / name
            print(name, path.exists())
PY
```

Expected: all required core outputs appear under the canonical output directory.

- [ ] **Step 5: Commit the path-consistency and end-to-end verification updates**

```bash
git add produce_csv.py merge_csv.py 2_label/label_of_feature_trend.py
git commit -m "chore: align pipeline paths and verify end-to-end outputs"
```

---

### Task 9: Final verification of research intent and non-regression

**Files:**
- Modify: none unless verification reveals issues
- Test: direct output inspection

- [ ] **Step 1: Verify broad indicator families were preserved**

Run:

```bash
python - <<'PY'
import pandas as pd
path = '/LocalSSD/p9056/TestTools_ANALY/data_new/with_features_0601/B_4h_with_features.csv'
df = pd.read_csv(path, nrows=5)
checks = [
    '4h_MACD', '4h_RSI', '4h_ATR', '4h_OBV', '4h_BB_Upper',
    '4h_Divergence_Score', '4h_Div_Type'
]
missing = [c for c in checks if c not in df.columns]
assert not missing, missing
print('broad indicator families preserved')
PY
```

Expected: `broad indicator families preserved`

- [ ] **Step 2: Verify the new context columns are present in the context dataset**

Run:

```bash
python - <<'PY'
import pandas as pd
path = '/LocalSSD/p9056/TestTools_ANALY/data_new/with_features/merged_4h_context.csv'
df = pd.read_csv(path, nrows=5)
checks = [
    'daily_trend_bias', 'daily_trend_strength', 'daily_filter_pass_long', 'daily_filter_pass_short',
    'weekly_trend_bias', 'weekly_trend_strength', 'weekly_vol_regime', 'weekly_above_long_ma'
]
missing = [c for c in checks if c not in df.columns]
assert not missing, missing
print('context dataset columns present')
PY
```

Expected: `context dataset columns present`

- [ ] **Step 3: Verify label metadata excludes raw price arrays and is JSON-serializable**

Run:

```bash
python - <<'PY'
import json
from pathlib import Path
for candidate in [
    Path('/LocalSSD/p9056/TestTools_ANALY/data/with_features_0303/label_config.json'),
    Path('/LocalSSD/p9056/TestTools_ANALY/data_new/with_features/label_config.json'),
    Path('/LocalSSD/p9056/TestTools_ANALY/data_new/with_features_0601/label_config.json'),
]:
    if candidate.exists():
        payload = json.loads(candidate.read_text(encoding='utf-8'))
        assert 'labels_config' in payload
        for cfg in payload['labels_config'].values():
            assert 'price' not in cfg
        print('label metadata ok:', candidate)
        break
else:
    raise AssertionError('no label_config.json found')
PY
```

Expected: one `label metadata ok:` line.

- [ ] **Step 4: Commit final verification adjustments if any were needed**

```bash
git status
```

Expected: clean working tree, or only intentional verification-related edits.

If code changed during final verification:

```bash
git add produce_csv.py calculate_indicator_v2.py merge_csv.py 2_label/label_of_feature_trend.py
git commit -m "test: finalize pipeline verification fixes"
```

---

## Spec Coverage Check

This plan covers:
- base feature/data quality improvements
- 1h raw-field preservation
- broken feature semantic correction
- duplicate divergence cleanup
- lightweight weekly/daily context-state additions
- richer schema/manifest outputs
- merged research/context outputs
- label pipeline reconnection and label metadata
- end-to-end path consistency
- preservation of broad indicator coverage

## Placeholder Scan

No `TODO`, `TBD`, or “similar to above” references are intentionally left in this plan. Every task names exact files, commands, and code blocks.

## Type / Name Consistency Check

Key names used consistently across tasks:
- `merged_4h_research.csv`
- `merged_4h_context.csv`
- `daily_source_timestamp`
- `weekly_source_timestamp`
- `daily_trend_bias`
- `daily_trend_strength`
- `daily_filter_pass_long`
- `daily_filter_pass_short`
- `weekly_trend_bias`
- `weekly_trend_strength`
- `weekly_vol_regime`
- `weekly_above_long_ma`
