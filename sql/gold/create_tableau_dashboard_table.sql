-- ──────────────────────────────────────────────
-- Gold Table 1: 운항 현황 대시보드용
-- 갱신 주기: 매일 1회 (BigQuery Scheduled Query)
-- 데이터 소스: silver.flights (최신 상태)
-- ──────────────────────────────────────────────

DECLARE local_timezone STRING DEFAULT 'Asia/Seoul';
DECLARE delay_threshold_15 INT64 DEFAULT 15;
DECLARE delay_threshold_30 INT64 DEFAULT 30;
DECLARE delay_threshold_60 INT64 DEFAULT 60;
DECLARE delay_threshold_120 INT64 DEFAULT 120;

CREATE OR REPLACE TABLE `gold.tableau_flights_dashboard` AS
WITH base_flights AS (
  SELECT * FROM `silver.flights`
),

-- 타임존 변환을 위한 CTE
tz_converted AS (
  SELECT
    *,
    DATETIME(scheduled_utc, local_timezone) AS scheduled_kst,
    DATETIME(expected_utc,  local_timezone) AS expected_kst,
    DATETIME(actual_utc,    local_timezone) AS actual_kst
  FROM base_flights
)

SELECT
  -- ── 기본 식별자 ──
  flight_key,
  ymd,
  airline_icao,
  flight_iata,
  airline_kr,                     -- 항공사 한글명 (ex: 대한항공)
  arr_airport_iata,               -- 도착 공항 코드 (ex: NRT)
  arr_airport_kr,                 -- 도착 공항 한글명 (ex: 나리타)
  nature,                         -- 운항 유형 (화물/여객/기타)

  -- ── 시간 관련 (KST 기준) ──
  scheduled_utc,
  expected_utc,
  actual_utc,
  scheduled_kst,
  expected_kst,
  actual_kst,
  EXTRACT(DATE FROM scheduled_kst) AS scheduled_date,        -- KST 날짜
  EXTRACT(HOUR FROM scheduled_kst) AS scheduled_hour,        -- KST 시각
  EXTRACT(DAYOFWEEK FROM scheduled_kst) AS scheduled_day_of_week,  -- 1=일, 7=토

  -- 시간대 그룹
  CASE
    WHEN EXTRACT(HOUR FROM scheduled_kst) BETWEEN 5  AND 11 THEN '오전 (05-11시)'
    WHEN EXTRACT(HOUR FROM scheduled_kst) BETWEEN 12 AND 17 THEN '오후 (12-17시)'
    WHEN EXTRACT(HOUR FROM scheduled_kst) BETWEEN 18 AND 22 THEN '저녁 (18-22시)'
    ELSE '심야/새벽 (23-04시)'
  END AS time_of_day,

  -- 운항 상태
  -- status가 ''(빈 문자열)이지만 지연이 발생한 경우 → '지연'으로 보정
  CASE
    WHEN status = '' AND current_delay_min > 0 THEN '지연'
    WHEN status = '' THEN '정보없음'
    ELSE status
  END AS status,
  status_remark,

  -- 지연 분석
  current_delay_min,

  CASE
    WHEN current_delay_min IS NULL THEN '정보없음'
    WHEN current_delay_min <= 0 THEN '정시 운항'
    WHEN current_delay_min <= delay_threshold_15 THEN '15분 이하 지연'
    WHEN current_delay_min <= delay_threshold_30 THEN '15~30분 지연'
    WHEN current_delay_min <= delay_threshold_60 THEN '30~60분 지연'
    WHEN current_delay_min <= delay_threshold_120 THEN '1~2시간 지연'
    ELSE '2시간 이상 지연'
  END AS delay_category,

  -- 통계 계산용 플래그
  CASE WHEN current_delay_min > delay_threshold_15 THEN 1 ELSE 0 END AS is_delayed_15min,
  CASE WHEN current_delay_min > delay_threshold_30 THEN 1 ELSE 0 END AS is_delayed_30min,
  CASE WHEN current_delay_min > delay_threshold_60 THEN 1 ELSE 0 END AS is_delayed_60min,
  CASE WHEN current_delay_min > delay_threshold_120 THEN 1 ELSE 0 END AS is_delayed_120min,
  CASE WHEN status = '취소' THEN 1 ELSE 0 END AS is_canceled

FROM tz_converted 
WHERE nature = '여객';
