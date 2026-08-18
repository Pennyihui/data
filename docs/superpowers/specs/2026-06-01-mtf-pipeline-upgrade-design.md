# Multi-timeframe Pipeline Upgrade Design

Date: 2026-06-01

## Goal
Upgrade the existing BTC data pipeline to better support a multi-timeframe trend-trading research workflow while keeping broad feature coverage intact for later evaluation.

Primary trading hierarchy:
- 4h = primary decision timeframe
- daily = trend filter
- weekly = larger market/cycle context filter

Current scope is intentionally limited to:
- improving base feature/data quality
- improving multi-timeframe alignment
- improving schema/metadata/lineage
- fixing label pipeline connectivity and label metadata
- adding only a small amount of weekly/daily context-state columns

Out of scope for this iteration:
- full macro regime discovery / clustering
- full layered prediction framework (macro / first-layer / second-layer)
- full support/resistance system
- direct executable trading signal generation / meta-labeling framework
- large-scale deletion of generic indicators

## Why this scope
The user wants to preserve generic indicators for later evaluation, not prematurely collapse the dataset into one fixed strategy view. The pipeline should become a better research data foundation first, while exposing enough weekly/daily context to support later multi-stage forecasting and filtering work.

## Design Summary
The pipeline will be refactored conceptually into three output layers:

1. **Base feature layer**
   - Keep existing broad technical features across 1h/4h/daily/weekly
   - Fix data-loss and naming/logic inconsistencies
   - Improve consistency of raw columns and metadata

2. **Context layer**
   - Add a small set of weekly/daily state columns
   - Preserve them as research context, not final trading decisions
   - Expose multi-timeframe lineage (which daily/weekly bar each 4h row is using)

3. **Label layer**
   - Keep FTH / CT / Oracle labels
   - Connect label input paths to the new merged datasets
   - Save label parameter metadata alongside outputs

## Architectural Direction
### 1. Single-timeframe outputs remain
Continue producing:
- `B_1h_with_features.csv`
- `B_4h_with_features.csv`
- `B_daily_with_features.csv`
- `B_weekly_with_features.csv`

These remain the authoritative per-timeframe research outputs.

### 2. Two merged 4h-wide outputs
Instead of a single merged wide table as the only downstream artifact, produce two merged 4h datasets:

- `merged_4h_research.csv`
  - broad 4h + lagged daily + lagged weekly feature matrix
  - intended for feature analysis, model training, ablation, and exploratory research

- `merged_4h_context.csv`
  - all research columns plus a small number of context-state columns
  - intended for later forecasting/filtering experiments

The context dataset is not yet a strategy signal table. It is still a research dataset.

### 3. Labels become path-connected and parameterized
The label script should read from the new merged dataset path, not a stale legacy dataset path. It should also write a label-config metadata file so labels are reproducible from the dataset alone.

## Weekly / Daily / 4h Responsibility Boundaries (for this iteration)
This iteration does not implement the user’s full layered prediction framework, but it should establish compatible boundaries.

### Weekly
Weekly provides only lightweight high-level context columns now, such as:
- long-horizon trend bias
- trend strength proxy
- volatility regime proxy
- above/below long moving-average state

It does **not** yet implement full BTC four-phase cycle detection.

### Daily
Daily provides lightweight trend-filter context columns, such as:
- trend bias
- trend strength
- pass-long / pass-short style filter flags

It does **not** yet implement full directional forecasting.

### 4h
4h remains the execution-resolution research table, but this iteration does not yet hard-code 4h setup logic into explicit strategy signals.

## File-by-file Design

### A. `produce_csv.py`
Responsibilities after upgrade:
- load and clean raw 1h data
- resample to 4h/daily/weekly
- preserve raw fields consistently across timeframes where possible
- compute base features for each timeframe
- generate metadata/schema summaries
- save per-timeframe outputs

Planned changes:
1. **Fix 1h raw-field preservation**
   - Stop truncating 1h input to only OHLCV before feature generation.
   - Preserve `quote_volume`, `trades`, `taker_buy_volume`, and `taker_buy_quote_volume` so 1h volume-pressure and average-trade-size features are meaningful.

2. **Normalize output consistency**
   - Ensure timestamp/index handling is explicit and stable when writing CSVs.
   - Keep raw/base columns consistent enough that downstream merge and schema tools can reason about them.

3. **Upgrade metadata outputs**
   - Keep `feature_columns.csv` if useful, but add richer outputs (see Metadata section below).

4. **Keep broad feature coverage intact**
   - Do not delete broad indicator families in this pass.

### B. `calculate_indicator_v2.py`
Responsibilities after upgrade:
- remain the core feature factory
- cleanly support base features, advanced features, and a small context-state layer
- expose more internally consistent feature semantics

Planned changes:
1. **Fix clearly broken or misleading logic**
   - Correct features whose implementation and naming disagree.
   - Example already identified: candle-direction logic using absolute body size where signed semantics are required.

2. **Remove/resolve duplicate method definitions**
   - Consolidate duplicated `detect_divergences` definitions so there is only one authoritative implementation path.

3. **Improve timeframe semantic consistency**
   - Review volatility scaling and similar features whose meaning changes by timeframe.
   - Prefer definitions that are explicit and consistent rather than silently mixed.

4. **Add only lightweight context columns**
   - Weekly context examples:
     - `weekly_trend_bias`
     - `weekly_trend_strength`
     - `weekly_vol_regime`
     - `weekly_above_long_ma`
   - Daily context examples:
     - `daily_trend_bias`
     - `daily_trend_strength`
     - `daily_filter_pass_long`
     - `daily_filter_pass_short`

These should be derived from existing indicator families and remain interpretable.

### C. `merge_csv.py`
Responsibilities after upgrade:
- build authoritative 4h-granularity merged datasets
- apply lagged daily/weekly joins safely
- preserve lineage and timing transparency

Planned changes:
1. **Keep lagged-join design**
   - Daily applies to the next trading day
   - Weekly applies to the next trading week
   - Preserve no-future-data intent

2. **Add lineage/timing columns**
   Suggested additions:
   - `daily_source_timestamp`
   - `weekly_source_timestamp`
   - optional freshness/age indicators such as `daily_age_bars` and `weekly_age_bars`

3. **Emit two merged outputs**
   - `merged_4h_research.csv`
   - `merged_4h_context.csv`

4. **Clearly separate roles of merged columns**
   - 4h-native columns remain primary table features
   - daily and weekly columns remain lagged context features

### D. `2_label/label_of_feature_trend.py`
Responsibilities after upgrade:
- label the current merged dataset rather than a stale path
- keep FTH / CT / Oracle labeling available
- save label metadata for reproducibility

Planned changes:
1. **Reconnect to current merged outputs**
   - Replace old hard-coded input path with the new merged dataset path.

2. **Externalize or centralize label config**
   - Store label parameters in an explicit config structure and output file.

3. **Save label metadata**
   Suggested output:
   - `label_config.json`
   - include fee, H, tau_mult, omega settings, and dataset source path

4. **Keep current labeling families**
   - No label-family replacement in this iteration.

## Output Artifacts

### Per-timeframe outputs
- `B_1h_with_features.csv`
- `B_4h_with_features.csv`
- `B_daily_with_features.csv`
- `B_weekly_with_features.csv`

### Merged outputs
- `merged_4h_research.csv`
- `merged_4h_context.csv`

### Labeled output
- existing multilabel output name may be preserved or renamed, but it must be based on the new merged input path

### Metadata outputs
1. `feature_columns.csv` (optional to keep)
2. `feature_schema.json`
3. `feature_schema.csv`
4. `dataset_manifest.json`
5. `label_config.json`

## Metadata / Schema Design
This is one of the most important improvements in this iteration.

Each feature schema row should ideally capture:
- feature name
- timeframe
- category (raw / trend / momentum / volatility / volume / candle / divergence / context / label)
- source table (`1h`, `4h`, `daily`, `weekly`, `merged`)
- data type
- null count / non-null count
- whether it is lagged
- window/parameter hints when easily known
- short human-readable description when feasible

The dataset manifest should capture:
- input file path(s)
- output file path(s)
- generation timestamp
- date coverage
- row counts and column counts
- pipeline version string or script names

## Error Handling / Data Quality Rules
1. Preserve no-future-data intent in joins and rolling features.
2. Prefer explicit fallback values only when absolutely necessary, and make them visible in metadata if practical.
3. Avoid silent degradation of important feature families due to missing raw inputs.
4. Keep research outputs rich, but make data lineage explicit enough that later filtering/evaluation is trustworthy.

## Testing / Verification Strategy
This iteration should verify:
1. 1h outputs now contain the raw fields needed for pressure/trade-size features.
2. merged datasets use lagged daily/weekly data rather than same-bar future information.
3. duplicated or broken feature logic has been resolved.
4. metadata files correctly describe produced datasets.
5. label script reads the new merged dataset path successfully.
6. context columns are present and interpretable without hard-coding a full strategy.

## Recommended Implementation Order
1. Fix `produce_csv.py` raw-field preservation and output consistency.
2. Clean `calculate_indicator_v2.py` logic issues and duplicate definitions.
3. Add lightweight weekly/daily context-state columns.
4. Upgrade `merge_csv.py` to emit research + context outputs with lineage columns.
5. Reconnect and parameterize `label_of_feature_trend.py`.
6. Add schema/manifest outputs.
7. Run verification on generated columns and file connectivity.

## Trade-off Rationale
This design deliberately avoids premature strategy hard-coding. It keeps broad indicators for later evaluation while still moving the pipeline toward a true multi-timeframe trend-research foundation. It also creates a clean bridge to the user’s later goals: macro state discovery, dual-direction probability framing, layered forecasting, and eventual trading-signal generation.
