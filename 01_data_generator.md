# Media Data Generator Sample Solution

## 1. Domain Overview

This project simulates a medium-size media platform. The generator produces:

- Offline historical/reference data (Parquet)
- Streaming real-time events (JSON)

The goal is to support downstream ingestion, transformation, and feature engineering while intentionally injecting realistic data quality and processing challenges.

---

## 2. Offline Dataset Design

### 2.1 Offline Tables

| Table | Grain | Key Columns |
|-------|-------|------------|
| users | one per user | user_id, user_age, country, user_subscription, signup_ts |
| videos | one per video | video_id, video_title, video_genre, video_duration, upload_date |
| playback_history | one per session | history_id, user_id, video_id, playback_date, watch_hours |
| interactions | one per interaction | interaction_id, user_id, video_id, interaction_type, comments, likes |
| ad_impressions | one per impression | impression_id, user_id, video_id, advertiser_id, cost_nanos, midpoint, thirdQuartile |

### 2.2 Offline Data Problems

**Compulsory:**
- **Skew**: Severe popularity bias following a Zipfian distribution — ~80% of views concentrate on 20% of content, leaving a long tail of underrepresented media. Ad CTR is heavily right-skewed (most impressions yield zero clicks). Churn data is imbalanced at ~14–15% positive rate.
- **High cardinality**: `user_id`, `video_id`, `history_id`, and `impression_id` are unique identifiers spanning millions of daily events.
- **Schema evolution**: ~60% of historical partitions predate the schema change date and are missing ad tracking fields (`midpoint`, `thirdQuartile`) introduced in the updated IAB VAST tracking spec. These fields are NULL in older partitions and must be handled downstream.

**Optional chosen:** 2% duplicate rate in `playback_history` and `interactions`, caused by redundant log entries from distributed CDN nodes or client-side retries during network buffering.

**Output:** Parquet partitioned by `playback_date` and `signup_ts`.

---

## 3. Streaming Dataset Design

### 3.1 Event Stream Schema

Single unified Kafka topic with `event_type` to ingest high-velocity events from diverse sources.

Key columns:
- `event_id`, `event_type` (`playback_start` | `pause` | `skip` | `ad_impression` | `ad_click` | `subscription_cancel`)
- `event_timestamp`, `created_ts` (client device time vs. stream storage ingestion time)
- `user_id`, `session_id`, `device_type`, `platform` (`smart_tv` | `web` | `mobile_app`)
- `video_id` (nullable), `genre_id` (nullable), `playback_position_seconds` (nullable), `ad_campaign_id` (nullable)
- `midpoint` (nullable), `thirdQuartile` (nullable) — ad tracking fields aligned with offline schema

### 3.2 Streaming Data Problems

**Compulsory:**
- **Bursts**: 10,000 events/min baseline → 500,000 events/min during 30-minute burst windows (e.g., NFL Christmas Day Games, major series premieres).
- **Late arrivals**: 15% of events have `created_ts` significantly later than `event_timestamp` (delay range: 1–48 hours) due to offline mobile viewing that syncs telemetry only after network reconnection.

**Optional chosen:** 2% duplicate events (same `event_id`, immediate or short delay) from CDN redundancy or client-side retry on network buffering.

**Output:** Avro.

---

## 4. Feature Engineering

Compute from user viewing history, interaction, and streaming event data:

**Offline (stable, 90-day windows):**
- `f_user_total_watch_hours_90d` — total playback duration across all sessions
- `f_user_distinct_genres_90d` — genre diversity score to map content affinity
- `f_user_historical_ad_ctr_90d` — historical ad click-through rate (clicks / impressions)
- `f_user_subscription_churn_risk_90d` — derived churn risk score based on recency, frequency, and engagement decline (not a ground-truth label)

**Streaming (rolling windows):**
- `f_stream_videos_started_30m` — count of `playback_start` events in last 30 minutes
- `f_stream_ad_completion_ratio_60m` — ratio of ad impressions reaching `midpoint` (50%) or `thirdQuartile` (75%) in last 60 minutes
- `f_stream_early_skip_rate_60m` — rate of `skip` events within the first few seconds (signals poor recommendations)
- `f_stream_burst_activity_flag` — binary flag for high-frequency event spikes (e.g., live sports, premieres)

Merge offline + streaming for a unified feature table keyed by `user_id`, refreshed every 15 minutes.

---

## 5. Generator Configuration

```yaml
n_users: 120000
n_videos: 45000
days_history: 180
skew_ratio_popularity: 0.80   # 80% of views go to 20% of catalog (Zipf's Law)
skew_ratio_genre: 0.75        # Heavy skew towards mainstream genres (Action, Drama)
churn_rate_baseline: 0.145    # ~14.5% churn rate to simulate class imbalance
duplicate_rate_offline: 0.02  # 2% duplicate rate from CDN log redundancies
schema_change_date: "2025-07-01"  # Date when midpoint/thirdQuartile ad tracking was introduced
base_events_per_min: 10000
burst_multiplier: 50          # Traffic spikes for live programming or season premieres
burst_windows: ["20:00-20:30", "21:00-21:30"]  # Evening prime time peaks
late_arrival_rate: 0.15       # 15% late arrivals from offline mobile sync
late_delay_min_max: [1, 48]   # Delay range in hours (offline sessions sync after reconnect)
duplicate_rate_stream: 0.02
random_seed: 42
```

---

## 6. Deliverables

1. Generator code with configurable parameters supporting complex media behaviors (popularity bias, imbalanced churn, schema evolution, late arrivals).
2. Data outputs: Parquet (offline tables), Avro (streaming telemetry).
3. Quality report:
   - Skew distribution: verify Zipfian video popularity and right-skewed CTR.
   - Cardinality: `approx_count_distinct` by `user_id`, `video_id`, `ad_campaign_id`.
   - Schema evolution: verify NULLs in pre-`schema_change_date` partitions for `midpoint` / `thirdQuartile`.
   - Duplicate rate before/after dedup (offline and streaming).
   - Streaming burst, late arrival, and duplicate rates.
4. Write-up: explain (a) Zipf distribution for realistic media consumption simulation, (b) delayed event sync logic for offline viewing, (c) duplicate injection strategy for both offline and streaming layers, and (d) schema evolution handling for backward compatibility.

---

## 7. Implementation Tips

- Use deterministic seeds for reproducibility.
- Dedup keys: `history_id` / `interaction_id` (offline); `event_id` alone (streaming — duplicates share the same `event_id` but may differ in `created_ts`).
- Simulate popularity bias using an inverse power-law (Zipfian) distribution for `video_id` interaction frequencies.
- Enforce referential integrity: all `user_id` and `video_id` values in streaming events must exist in the offline dimension tables.
- Generate `midpoint` / `thirdQuartile` as NULL for records before `schema_change_date` in both offline and streaming layers to ensure consistent schema evolution behavior.