# Codebase Architecture

## Package Map

```
main.py                          # Repo-level CLI entry point used by make run
src/data_generator/
├── __init__.py
├── core/
│   ├── config.py                # GeneratorConfig - YAML config loader
│   ├── schema.py                # DataSchema - Parquet & Avro field definitions
│   ├── base_generator.py        # BaseGenerator - abstract root with config+utils
│   └── utils.py                 # RandomDataUtils - Zipf, Bernoulli, duplicates, late timestamps
├── offline/
│   ├── base_table_generator.py  # BaseTableGenerator - schema validation, Parquet I/O
│   ├── users.py                 # UsersGenerator       - dimension table
│   ├── videos.py                # VideoGenerator        - dimension table
│   ├── playback_history.py      # PlaybackHistoryGenerator - fact table (Zipfian video skew)
│   ├── interactions.py          # InteractionGenerator     - fact table (depends on playback)
│   ├── ad_impressions.py        # AdImpressionGenerator    - fact table (schema evolution, CTR)
│   └── offline_data_generator.py# OfflineDataGenerator     - orchestrates all offline generators
├── streaming/
│   ├── stream_data_generator.py # StreamDataGenerator        - chain-based event generation, Avro I/O
│   ├── playback_start.py        # PlaybackStartEventGenerator
│   ├── ad_impression.py         # AdImpressionEventGenerator (streaming ad_impression events)
│   ├── ad_click.py              # AdClickEventGenerator
│   ├── pause.py                 # PauseEventGenerator
│   ├── skip.py                  # SkipEventGenerator
│   └── subscription_cancel.py   # SubscriptionCancelEventGenerator
├── features/
│   ├── feature_engineer.py      # FeatureEngineer - load, compute, merge, save features
│   ├── offline_calculator.py    # OfflineFeatureCalculator    - 90-day stable features
│   └── streaming_calculator.py  # StreamingFeatureCalculator  - rolling window features
└── pipeline/
    ├── orchestrator.py          # PipelineOrchestrator - run offline + streaming + features
    └── quality_report.py        # QualityReport - profile data and generate quality reports
```

## Data Flow

```
┌────────────────────────────────────────────────────────────────────┐
│ YAML Config (config/default.yaml)                                  │
│  random_seed, n_users, n_videos, days_history, burst_windows, ...  │
└────────────────────────┬───────────────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────────────────────┐
│  PipelineOrchestrator                                               │
│  ┌─────────────────┐   ┌──────────────────┐   ┌──────────────────┐  │
│  │ run_offline()   │   │ run_streaming()  │   │ run_feature_     │  │
│  │                 │   │                  │   │ engineering()    │  │
│  │ Users           │   │ playback_start   │   │                  │  │
│  │ Videos          │   │ ad_impression    │   │ OfflineFeature   │  │
│  │ PlaybackHistory │──▶│ ad_click         │──▶│ Calculator       │  │
│  │ Interactions    │   │ pause/skip       │   │ StreamingFeature │  │
│  │ AdImpressions   │   │ subscription_    │   │ Calculator       │  │
│  │                 │   │   cancel         │   │ merge + save     │  │
│  └────────┬────────┘   └────────┬─────────┘   └─────────┬────────┘  │
└───────────┼─────────────────────┼───────────────────────┼───────────┘
            │                     │                       │
            ▼                     ▼                       ▼
      Parquet files        Avro partitions          Feature Parquet
      (data/offline/)      (data/streaming/)        (data/features/)
```

### Generation Order (Offline)

```
1. Users ───────────────► dimension
2. Videos ──────────────► dimension
3. Playback History ────► fact (no table dependency)
4. Interactions ────────► fact (samples user+video from playback_history)
5. Ad Impressions ──────► fact (samples user+video+date from playback_history)
```

### Streaming Event Chain

```
1. playback_start ──────────────────── (position=0)
2. ad_impression ───────────────────── (+5-90s, midpoint/third_quartile random)
3. ad_click (optional, ~3%) ────────── (+1-10s after ad)
4. pause (65%) / skip (35%) ───────── (+10-600s, position random)
5. subscription_cancel (optional) ──── (driven by churn_rate_baseline)
```

---

## Component Relationships

### Inheritance

```
BaseGenerator (core/)
  └── BaseTableGenerator (offline/)
       └── _GeneratorAdapter (inner class of OfflineDataGenerator)
```

### Ownership

```
PipelineOrchestrator
  ├── owns: OfflineDataGenerator
  │         └── owns: UsersGenerator, VideoGenerator, PlaybackHistoryGenerator,
  │                   InteractionGenerator, AdImpressionGenerator
  │                   (wrapped in _GeneratorAdapter → BaseTableGenerator)
  ├── owns: StreamDataGenerator
  │         └── owns: PlaybackStartEventGenerator, AdImpressionEventGenerator,
  │                   AdClickEventGenerator, PauseEventGenerator, SkipEventGenerator,
  │                   SubscriptionCancelEventGenerator
  └── owns: FeatureEngineer
            ├── owns: OfflineFeatureCalculator
            └── owns: StreamingFeatureCalculator
```

### Data Dependencies (cross-component)

```
OfflineDataGenerator → writes Parquet → consumed by:
  ├── FeatureEngineer.load_offline()
  ├── PipelineOrchestrator._build_streaming_contexts() [via FeatureEngineer.load_offline()]
  └── QualityReport.profile_offline_tables()

StreamDataGenerator → writes Avro → consumed by:
  ├── FeatureEngineer.load_stream()
  └── QualityReport.profile_stream()

FeatureEngineer → writes Parquet → consumed by:
  └── QualityReport.profile_features()
```

---

## Key Design Decisions

### 1. Deterministic Seeds
Offline table generators create private `np.random.default_rng(seed)` instances from the configured seed. Streaming generation shares a seeded `RandomDataUtils` instance across the stream orchestrator and event generators so event chains advance through one deterministic sequence. This keeps outputs reproducible while avoiding duplicate random streams inside a generator.

### 2. Chain-Based Streaming Events
Streaming follows a causal chain model (not independent event-type sampling). Each chain corresponds to a mini user session with a fixed sequence: playback_start → ad_impression → (optional ad_click) → pause/skip → (optional subscription_cancel). This produces realistic inter-event timing and correlation.

### 3. Schema Evolution via Date-Based Null Logic
The `ad_impressions` table (offline) and streaming events contain `midpoint` / `third_quartile` fields. Records with `playback_date < schema_change_date` (default `2026-04-01`) have these fields as `NULL`; newer records have them populated (~72% reach midpoint, ~68% of those reach third quartile). This simulates a real IAB VAST tracking spec update without requiring Hive directory partitions.

### 4. Referential Integrity Through Data Sampling
Fact tables (interactions, ad_impressions) re-sample `(user_id, video_id, playback_date)` tuples from the already-generated `playback_history` DataFrame rather than sampling independently from dimension tables. This guarantees all fact-table foreign keys exist without explicit validation.

### 5. Right-Skewed Distributions
- **Video popularity**: Zipf distribution with `skew_ratio_popularity` (default 0.80) - ~80% of views go to ~20% of videos.
- **Genre distribution**: Zipf with `skew_ratio_genre` (default 0.75).
- **Streaming ad clicks**: Bernoulli with `ad_click_rate` (default 0.03) - most ad impressions do not produce an `ad_click` event.
- **Subscription tiers**: Zipf-like weights - `free` is most common.

### 6. Output Formats
- **Offline**: Parquet datasets under `data/offline/{table}/`. Tables with `signup_ts` or `playback_date` are partitioned by those columns; other tables are written as a Parquet file inside their table directory. Schema evolution (midpoint/third_quartile null logic) is based on the `playback_date` column value.
- **Streaming**: Avro object container files, partitioned hourly as `hour=YYYYMMDDHH/events.avro`.
- **Features**: Parquet files at `data/features/refresh_ts=YYYYMMDDHHmm/features.parquet`. The timestamp is the refresh/write timestamp; the table also contains `feature_ts`, which is `window_end` floored to a 15-minute boundary.

### 7. Duplicate Injection Strategy
- **Offline**: Inline `_inject_duplicates` in each generator - random rows are copied and shuffled into the output (no dedup step).
- **Streaming**: Per-event `bernoulli(duplicate_rate_stream)` - duplicate is appended with `created_ts + 1ms`.
- Both use the same key (`history_id`, `interaction_id`, `event_id`) - dedup removes all but one copy.
- Quality report captures both before-dedup and after-dedup rates.

---

## Data Quality Dimensions

| Dimension | How It's Injected | How It's Measured |
|---|---|---|
| **Skew** | Zipfian video selection, right-skewed offline/streaming clicks, imbalanced churn | `QualityReport` - popularity skew %, CTR, genre distribution |
| **Cardinality** | Unique user/video/advertiser IDs | `QualityReport` - `nunique()` per table |
| **Schema Evolution** | `midpoint`/`third_quartile` NULL before `schema_change_date` | `QualityReport` - null rates before/after `schema_change_date` |
| **Duplicates** | Injected random rows (offline) or events (streaming) | `QualityReport` - before/after dedup rates |
| **Late Arrivals** | `created_ts` delayed by 1-48 hours (streaming) | `QualityReport` - late arrival % and delay hours |
| **Bursts** | Events-per-minute multiplied during burst windows | `QualityReport` - baseline vs. peak events/min |
| **Referential Integrity** | Fact tables sample from playback_history | Enforced by generation order, verified by tests |
