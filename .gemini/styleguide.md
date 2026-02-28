# Flight Delay Prediction Dashboard (Tableau) - Gemini Review Guide

This repo:

- Crawls / calls APIs to collect flight + (optionally) weather/airport data
- Builds analytics tables (preferably in BigQuery) for Tableau dashboards
- Trains a model to predict delay (minutes) and publishes predictions for dashboard use

## Data Architecture (Medallion Architecture)

This project follows the Medallion Architecture (Bronze, Silver, Gold).

### 1) Bronze Layer (Ingestion / Raw)

- **Goal**: Preserve raw data from source systems for lineage and re-processability.
- **Rules**:
  - **Type Safety**: In the Bronze layer, favor `STRING` (or `JSON`) for most source-originated fields. This prevents ingestion failures due to source schema changes or unexpected data formats.
  - **Minimal Transformation**: Do not apply complex business logic or type conversions here.
  - **Audit Fields**: Always include `collected_at` (UTC) and the full `raw_json` if possible.
  - **Deduplication**: Ingestion can be `WRITE_APPEND`; deduplication is handled in Silver/Gold.

### 2) Silver Layer (Cleansing / Standardized)

- **Goal**: Clean data, apply types, and normalize.
- **Rules**:
  - **Type Conversion**: Convert Bronze `STRING` fields to their appropriate analytical types (`TIMESTAMP`, `INT64`, `DATE`, etc.).
  - **Cleaning**: Handle nulls, trim strings, and normalize naming conventions.
  - **Deduplication**: Apply primary key constraints and deduplicate records.
  - **Validation**: Enforce schema constraints and data quality checks.

### 3) Gold Layer (Business / Analytics)

- **Goal**: Provide "ready-to-use" tables for Tableau and ML models.
- **Rules**:
  - **Aggregation**: Pre-calculate metrics and dimensions needed for dashboards.
  - **Join-heavy**: Tables should be flattened (denormalized) to optimize for Tableau performance.
  - **ML Features**: Store features prepared for model training and inference.

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
