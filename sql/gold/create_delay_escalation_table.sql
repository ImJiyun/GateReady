-- ──────────────────────────────────────────────
-- Gold Table 2: 지연 확대 분석용
-- 갱신 주기: 매일 1회 (BigQuery Scheduled Query)
-- 항공편별 "최초 관측 지연 → 최종 지연"을 비교하여
-- "지금 X분 지연이면, 최종적으로 얼마나 지연될까?" 분석
-- 데이터 소스: silver.flights_snapshots (전체 스냅샷 이력)
-- ──────────────────────────────────────────────
DECLARE local_timezone STRING DEFAULT 'Asia/Seoul';

CREATE OR REPLACE TABLE `gold.tableau_delay_escalation` AS

WITH first_snapshot AS (
  -- 항공편별 첫 번째 관측 (최초 포착된 상태)
  SELECT * FROM (
    SELECT
      flight_key,
      current_delay_min AS initial_delay_min,
      status AS initial_status,
      collected_at AS first_collected_at,
      ROW_NUMBER() OVER(PARTITION BY flight_key ORDER BY collected_at ASC) AS rn
    FROM `silver.flights_snapshots`
  ) WHERE rn = 1
),

last_snapshot AS (
  -- 항공편별 마지막 관측 (최종 확정 상태)
  SELECT * FROM (
    SELECT
      flight_key,
      airline_icao,
      airline_kr,
      flight_iata,
      arr_airport_iata,
      arr_airport_kr,
      nature,
      scheduled_utc,
      expected_utc,
      actual_utc,
      current_delay_min AS final_delay_min,
      status AS final_status,
      collected_at AS last_collected_at,
      ymd,
      ROW_NUMBER() OVER(PARTITION BY flight_key ORDER BY collected_at DESC) AS rn
    FROM `silver.flights_snapshots`
  ) WHERE rn = 1
),

-- 타임존 변환을 위한 CTE
tz_converted_last_snapshot AS (
  SELECT
    *,
    DATETIME(scheduled_utc, local_timezone) AS scheduled_kst,
    DATETIME(expected_utc,  local_timezone) AS expected_kst,
    DATETIME(actual_utc,    local_timezone) AS actual_kst
  FROM last_snapshot
)

SELECT
  -- ── 식별자 ──
  l.flight_key,
  l.airline_icao,
  l.flight_iata,
  l.airline_kr,
  l.arr_airport_kr,
  l.nature,                         -- 운항 유형 (화물/여객/기타)
  l.ymd,
  EXTRACT(DATE FROM l.scheduled_kst) AS scheduled_date,
  l.scheduled_utc,
  l.scheduled_kst,
  l.expected_kst,
  l.actual_kst,

  -- 최초 관측 시점
  f.initial_delay_min,
  f.initial_status,
  f.first_collected_at,

  -- 최종 확정 시점
  l.final_delay_min,
  l.final_status,
  l.last_collected_at,

  -- 추가 지연
  l.final_delay_min - f.initial_delay_min AS additional_delay_min,

  -- 관측 기간 (분)
  TIMESTAMP_DIFF(l.last_collected_at, f.first_collected_at, MINUTE) AS observation_duration_min,

  -- 초기 지연 구간 (Tableau 행/색상 기준)
  CASE
    WHEN f.initial_delay_min <= 0 THEN '정시 (0분 이하)'
    WHEN f.initial_delay_min <= 15 THEN '경미 (1~15분)'
    WHEN f.initial_delay_min <= 30 THEN '보통 (16~30분)'
    WHEN f.initial_delay_min <= 60 THEN '지연 (31~60분)'
    WHEN f.initial_delay_min <= 120 THEN '심각 (1~2시간)'
    ELSE '매우 심각 (2시간 초과)'
  END AS initial_delay_bucket,

  -- 지연 변화 방향
  CASE
    WHEN l.final_delay_min - f.initial_delay_min > 15 THEN '악화 (+15분 이상)'
    WHEN l.final_delay_min - f.initial_delay_min > 0 THEN '소폭 악화'
    WHEN l.final_delay_min - f.initial_delay_min = 0 THEN '변동 없음'
    ELSE '개선 (단축됨)'
  END AS delay_trend

FROM tz_converted_last_snapshot l
JOIN first_snapshot f ON l.flight_key = f.flight_key
WHERE l.nature = '여객';
