CREATE SCHEMA IF NOT EXISTS `PROJECT_ID.stg`
OPTIONS (
  location = "asia-northeast3"
);

CREATE OR REPLACE VIEW `PROJECT_ID.stg.flights` AS
WITH base AS (
  SELECT
    airline_icao,
    airline_kr,
    flight_iata,

    CASE
      WHEN arr_airport_iata = 'LA' THEN 'LAX'
      ELSE arr_airport_iata
    END AS arr_airport_iata_norm,

    arr_airport_kr,

    status,
    status_remark,
    status_remark_code,

    scheduled_utc_ts,
    expected_utc_ts,
    actual_utc_ts,

    nature,
    ymd,
    collected_at
  FROM 
    `PROJECT_ID.raw.flights`
  WHERE 
    scheduled_utc_ts IS NOT NULL
),

keyed AS (
  SELECT
    *,
    CAST(ymd AS STRING)
      || '_' || flight_iata
      || '_' || arr_airport_iata_norm
      || '_' || FORMAT_TIMESTAMP('%H%M', scheduled_utc_ts)
      AS flight_leg_key,

    TIMESTAMP_ADD(scheduled_utc_ts, INTERVAL 9 HOUR) AS scheduled_kst_ts,
    IFNULL(TIMESTAMP_ADD(expected_utc_ts, INTERVAL 9 HOUR), NULL) AS expected_kst_ts,
    IFNULL(TIMESTAMP_ADD(actual_utc_ts, INTERVAL 9 HOUR), NULL) AS actual_kst_ts
  FROM 
    base
)

SELECT
  * EXCEPT(arr_airport_iata_norm),
  arr_airport_iata_norm AS arr_airport_iata
FROM 
  keyed
QUALIFY
  ROW_NUMBER() OVER (
    PARTITION BY flight_leg_key, collected_at
    ORDER BY
      (actual_utc_ts IS NOT NULL) DESC,
      (expected_utc_ts IS NOT NULL) DESC,
      (status IS NOT NULL AND TRIM(status) != '') DESC,
      COALESCE(actual_utc_ts, expected_utc_ts, scheduled_utc_ts) DESC
  ) = 1;