# Flight Delay Prediction Dashboard (Tableau) - Gemini Review Guide

This repo:

- Crawls / calls APIs to collect flight + (optionally) weather/airport data
- Builds analytics tables (preferably in BigQuery) for Tableau dashboards
- Trains a model to predict delay (minutes) and publishes predictions for dashboard use

## Top priorities

### 1) Time correctness (critical)

- Enforce a single standard:
  - Store timestamps in UTC internally, and keep original local timezone fields if needed.
  - Clearly name columns: _\_utc_ts, _\_local_ts, \*\_tz
- Confirm "delay" definitions are consistent everywhere:
  - scheduled vs actual departure/arrival, delay in minutes
- Beware of daylight saving time and airport local time conversions.

### 2) Data ingestion reliability (crawling/API)

- Handle rate limits (429) and transient failures with retries + exponential backoff.
- Idempotency: re-running ingestion should not duplicate records.
  - Prefer upsert/merge keys (flight_id + date + airport + sched_time etc.)
- Validate schema changes:
  - Fail fast on missing expected fields
  - Log and surface breaking changes (do not silently fill with nulls)

### 3) Data quality & reproducibility

- Missing values: explicit strategy per field.
- Deduplication rules: deterministic and documented.
- Avoid “magic numbers” for thresholds (explain/centralize configs).
- Use a config file or constants module for endpoints, query params, and feature lists.

### 4) ML modeling (if present)

- Avoid leakage:
  - No features that rely on post-event information (e.g., actual arrival info).
- Splitting:
  - Prefer time-based split for forecasting-like use cases.
- Metrics:
  - Regression: MAE as primary (minutes) + RMSE as secondary.
  - Include baseline comparison (e.g., historical average delay by route/time).
- Ensure same preprocessing is used in training and inference.

### 5) SQL / BigQuery / Tableau layer

- Grain discipline:
  - Confirm the table grain matches dashboard filters (airport/day/hour/route).
- Avoid row explosion:
  - Check join keys, use DISTINCT only with justification.
- Prefer partitioning/clustering-friendly patterns if BigQuery is used.
- Provide stable schema for Tableau:
  - Avoid renaming columns without migration notes.

## What not to do

- Do not nitpick formatting-only changes.
- Avoid generic advice; point out concrete risk + fix.

## Preferred review format

For each issue:

1. Risk/bug
2. Why it matters for this project (Tableau + ingestion + delay prediction)
3. Suggested fix (specific)
