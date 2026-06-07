# Media Data Generator Sample Solution

## 1. Domain Overview

This project simulates a medium-size media platform. The generator produces:

- Offline historical/reference data (Parquet)
- Streaming real-time events (Avro)

The goal is to support downstream ingestion, transformation, and feature engineering while intentionally injecting realistic data quality and processing challenges.

---

## 2. Offline Dataset Design

### 2.1 Offline Tables

| Table | Grain | Key Columns |
|-------|-------|------------|
| users | one per user | user_id, user_age, country, user_subscription, signup_ts |
| videos | one per video | video_id, video_title, video_genre, video_duration, upload_date |
| playback_history | one per video watch; `session_id` can link multiple watches | session_id, history_id, user_id, video_id, playback_date, watch_hours |
| interactions | one per interaction | interaction_id, user_id, video_id, interaction_type, likes |
| ad_impressions | one per impression | impression_id, user_id, video_id, advertiser_id, cost_nanos, playback_date, midpoint, third_quartile, clicked |

### 2.2 Offline Data Problems

**Compulsory:**
- **Skew**: Severe popularity bias following a Zipfian distribution — views concentrate on the most popular content, leaving a long tail of underrepresented media. Offline and streaming ad clicks are right-skewed through `ad_click_rate`. Churn-like cancellation events are imbalanced through `churn_rate_baseline`.
- **High cardinality**: `user_id`, `video_id`, `history_id`, and `impression_id` are unique identifiers spanning millions of daily events.
- **Schema evolution**: ~60% of historical records predate the schema change date (`playback_date < schema_change_date`) and are missing ad tracking fields (`midpoint`, `third_quartile`) introduced in the updated IAB VAST tracking spec. These fields are NULL in older records and must be handled downstream.

**Optional chosen:** 2% duplicate rate in `playback_history` and `interactions`, caused by redundant log entries from distributed CDN nodes or client-side retries during network buffering.

**Output:** Parquet datasets under `data/offline/{table}/`. Tables containing `signup_ts` or `playback_date` are partitioned by those columns; other tables are written as a Parquet file inside the table directory.

---

## 3. Streaming Dataset Design

### 3.1 Event Stream Schema

Single unified Kafka topic with `event_type` field.

Key columns:
- `event_id`, `event_type` (`playback_start` | `pause` | `skip` | `ad_impression` | `ad_click` | `subscription_cancel`)
- `event_timestamp`, `created_ts` (client device time vs. stream storage ingestion time)
- `user_id`, `session_id`, `device_type`, `platform` (`smart_tv` | `web` | `mobile_app`)
- `video_id` (nullable), `genre_id` (nullable), `playback_position_seconds` (nullable), `ad_campaign_id` (nullable)
- `midpoint` (nullable), `third_quartile` (nullable) — ad tracking fields aligned with offline schema

### 3.2 Streaming Data Problems

**Compulsory:**
- **Bursts**: Configurable event-chain baseline (`base_events_per_min`) multiplied by `burst_multiplier` during configured 30-minute burst windows.
- **Late arrivals**: 15% of events have `created_ts` significantly later than `event_timestamp` (delay range: 1–48 hours) due to offline mobile viewing that syncs telemetry only after network reconnection.

**Optional chosen:** 2% duplicate events (same `event_id`, immediate or short delay) from CDN redundancy or client-side retry on network buffering.

**Output:** Avro.

---

## 4. Feature Engineering

Compute from user viewing history, interaction, and streaming event data:

**Offline (stable, 90-day windows):**
- `f_user_total_watch_hours_90d` — total playback duration across all sessions
- `f_user_distinct_genres_90d` — genre diversity score to map content affinity
- `f_user_historical_ad_ctr_90d` — historical ad click-through rate from offline `ad_impressions.clicked`
- `f_user_subscription_churn_risk_90d` — derived churn risk score based on recency, frequency, and engagement decline using fixed heuristic weights

**Streaming (rolling windows):**
- `f_stream_videos_started_30m` — count of `playback_start` events in last 30 minutes
- `f_stream_ad_completion_ratio_60m` — ratio of ad impressions reaching `midpoint` (50%) or `third_quartile` (75%) in last 60 minutes
- `f_stream_early_skip_rate_60m` — rate of `skip` events within the first few seconds (signals poor recommendations)
- `f_stream_burst_activity_flag` — per-user binary flag for high event counts in the last 60 minutes

Merge offline + streaming for a unified feature table keyed by `user_id`, refreshed every 15 minutes.

---

## 5. Generator Configuration

```yaml
n_users: 2500
n_videos: 1000
days_history: 120

n_playback_sessions: 25000
n_interactions: 15000
n_ad_impressions: 10000

advertiser_ids_pool: 50
cost_nanos_range: [500000000, 5000000000]  # $0.50 – $5.00 per impression

ad_click_rate: 0.03

skew_ratio_popularity: 0.80
skew_ratio_genre: 0.75

churn_rate_baseline: 0.145
duplicate_rate_offline: 0.02
duplicate_rate_stream: 0.02

schema_change_date: "2026-04-01"

base_events_per_min: 300
burst_multiplier: 5
burst_windows: ["20:00-20:30", "21:00-21:30"]

late_arrival_rate: 0.15
late_delay_min_max: [1, 48]

feature_early_skip_seconds: 10
feature_burst_threshold_events_60m: 20

run_duration_minutes: 15

random_seed: 42

data_dir:
  offline: data/offline
  streaming: data/streaming
  features: data/features
```

---

## 6. Deliverables

1. Generator code with configurable parameters supporting complex media behaviors (popularity bias, imbalanced churn, schema evolution, late arrivals).
2. Data outputs: Parquet (offline tables), Avro (streaming telemetry).
3. Quality report:
   - Skew distribution: verify Zipfian video popularity and right-skewed click behavior.
   - Cardinality: `approx_count_distinct` by `user_id`, `video_id`, `ad_campaign_id`.
   - Schema evolution: verify NULLs in pre-`schema_change_date` records for `midpoint` / `third_quartile`.
   - Duplicate rate before/after dedup (offline and streaming).
   - Streaming burst, late arrival, and duplicate rates.
4. Write-up: explain (a) Zipf distribution for realistic media consumption simulation, (b) delayed event sync logic for offline viewing, (c) duplicate injection strategy for both offline and streaming layers, and (d) schema evolution handling for backward compatibility.

---

## 7. Implementation Tips

- Use deterministic seeds for reproducibility.
- Dedup keys: `history_id` / `interaction_id` (offline); `event_id` alone (streaming - duplicates share the same `event_id` but may differ in `created_ts`).
- Simulate popularity bias using an inverse power-law (Zipfian) distribution for `video_id` interaction frequencies.
- Enforce referential integrity: all `user_id` and `video_id` values in streaming events must exist in the offline dimension tables.
- Generate `midpoint` / `third_quartile` as NULL for records before `schema_change_date` in both offline and streaming layers to ensure consistent schema evolution behavior.
- session_id in playback_history links multiple video watches within a single viewing session (useful for sequential recommendation model downstreams).
- advertiser_id is sampled from a configurable advertiser pool with no dimension table; sufficient for grouping and CTR analysis. 
