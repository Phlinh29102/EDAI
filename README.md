# Coursework — Media Platform Data Generator

## Quick Start

```bash
# Install dependencies
make install

# Run the full pipeline (offline → streaming → features → quality report)
make run

# Run with a different config
make run ARGS="config/test.yaml"

# Run tests
make test
```

## Commands

| Command | Action |
|---|---|
| `make install` | Install dependencies (`uv sync`) |
| `make run` | Run pipeline with `config/default.yaml` |
| `make run ARGS="config/test.yaml"` | Run pipeline with test config |
| `make test` | Run all tests |
| `make clean` | Remove generated data |

## Config

Edit `config/default.yaml` to control data volume, skew, burst windows, and feature thresholds.

## Outputs

| Stage | Format | Location |
|---|---|---|
| Offline tables | Parquet | `data/offline/{table}/` |
| Streaming events | Avro | `data/streaming/hour=YYYYMMDDHH/events.avro` |
| Features | Parquet | `data/features/refresh_ts={ts}/features.parquet` |
| Quality report | Text | `data/reports/quality_report.txt` |


**I'm so busy this week so I didn't have time to finish the phase 2 in time :)**