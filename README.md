# User Activity Batch Pipeline

This repository contains a batch data pipeline that generates synthetic user activity events, validates and transforms them, and writes cleaned and aggregated Parquet outputs for analytics.

## Setup

```bash
python3 -m pip install -r requirements.txt
```

## Run End-to-End

```bash
python3 data_generator.py --rows 500000 --days 30 --seed 42 --clean
python3 pipeline.py
```

The generator writes source JSON Lines partitions under `data/raw/date=YYYY-MM-DD/events.jsonl`.
The pipeline writes outputs under `output/{table_name}/date=YYYY-MM-DD/part-000.parquet` and writes a run manifest at `output/pipeline_manifest.json`.

## Event Schema

| Field | Type | Nullable | Rationale |
| --- | --- | --- | --- |
| `event_id` | string UUID | no | Unique event identifier used for deduplication. |
| `user_id` | string | no | Actor that triggered the event; supports user-level summaries and funnels. |
| `event_type` | string enum | no | One of `page_view`, `click`, `signup`, `purchase`; intentionally non-uniform distribution. |
| `event_category` | string | yes | Broad category such as `engagement` or `transactional`; nullable to simulate non-key data quality issues. |
| `event_timestamp` | ISO-8601 string | no | Raw event time; malformed values are quarantined. |
| `session_id` | string | no | Groups events into sessions for session counts and duration classification. |
| `country_code` | ISO country code string | yes | Geographic context; nulls are retained as `UNKNOWN` to avoid losing otherwise valid events. |
| `device_type` | string enum | yes | `mobile`, `web`, or `tablet`; null/invalid values are dropped because device is a required breakdown dimension. |
| `is_bot` | boolean | no | Identifies automated traffic that should be excluded from analytical outputs. |
| `payload` | JSON object | yes | Flexible event attributes. The pipeline flattens `page`, `referrer`, `product_id`, and purchase `value`. |

## Data Quality Strategy

The generator creates at least 500,000 base events over 30 days, then injects approximately 3% nulls in non-key fields, 2% duplicate event identifiers, and 1% malformed timestamps. Duplicate rows are added after base generation, so total ingested rows are slightly above the requested base count.

Cleaning rules:

- Malformed timestamps are written to `output/bad_records/date=unknown/part-000.parquet`.
- Duplicate `event_id` records keep the first observed row.
- Bot traffic is filtered out.
- Invalid `event_type` values are dropped.
- Null or invalid `device_type` values are dropped because `device_type` is required for an output table.
- Null `country_code` values are filled as `UNKNOWN`, preserving useful events while making the missingness visible.
- Null `event_category` values are filled as `unknown`.

Counts for each rule are logged and included in the manifest.

## Transformations

The pipeline flattens the JSON payload into typed top-level columns, derives `date` and `hour` from the parsed event timestamp, classifies each session by duration, and ranks events per user within each day.

Session duration thresholds:

- `short`: 0 to 5 minutes
- `medium`: greater than 5 and up to 30 minutes
- `long`: greater than 30 minutes

These thresholds separate quick visits, normal browsing sessions, and extended sessions in a simple way that is easy to explain and tune later.

## Aggregated Outputs

| Table | Grain | Metrics |
| --- | --- | --- |
| `daily_user_summary` | `user_id + date` | event count, session count, purchase total, first seen, last seen |
| `hourly_event_volume` | `event_type + date + hour` | event count, unique users, unique sessions |
| `country_device_breakdown` | `country_code + device_type + date` | event count, purchase count, unique users |
| `funnel_analysis` | `date` | signup -> page_view -> click -> purchase stage counts and conversion rates |

All cleaned and aggregated outputs are written as Snappy-compressed Parquet with a consistent `date=YYYY-MM-DD` partition layout. The pipeline deletes and rewrites each managed output table on every run, making repeated runs on the same input idempotent.

## Manifest

`output/pipeline_manifest.json` records:

- run timestamp
- input file count
- rows ingested
- rows dropped or flagged per quality rule
- rows written per output table
- wall-clock duration
- final status

## Design Notes

This solution uses pandas for pragmatic local batch processing. For a larger production workload, I would move the same contract to Spark, Polars lazy frames, or DuckDB depending on the expected data volume and serving pattern, while keeping the validation rules, partition contract, manifest, and table grains stable.
