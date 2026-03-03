-- ──────────────────────────────────────────────
-- Gold Table 1: 운항 현황 대시보드용
-- 갱신 주기: 매일 1회 (BigQuery Scheduled Query)
-- 데이터 소스: silver.flights (최신 상태) + bronze.flights (부가 정보)
-- ──────────────────────────────────────────────

CREATE OR REPLACE TABLE `gold.tableau_flights_dashboard` AS
WITH base_flights AS (
  SELECT * FROM `silver.flights`
),

-- Bronze에서 Silver에 없는 부가 정보 가져오기 (항공사 한글명, 공항 정보 등)
bronze_info AS (
  SELECT * FROM (
    SELECT 
      flight_key,
      airline_kr,
      arr_airport_iata,
      arr_airport_kr,
      nature,
      ROW_NUMBER() OVER(PARTITION BY flight_key ORDER BY collected_at DESC) AS rn
    FROM `bronze.flights`
  ) WHERE rn = 1
)

SELECT
  -- ── 기본 식별자 ──
  f.flight_key,
  f.ymd,
  f.airline_icao,
  f.flight_iata,
  b.airline_kr,                     -- 항공사 한글명 (ex: 대한항공)
  b.arr_airport_iata,               -- 도착 공항 코드 (ex: NRT)
  b.arr_airport_kr,                 -- 도착 공항 한글명 (ex: 나리타)
  b.nature,                         -- 운항 유형 (화물/여객/기타)
  
  -- ── 시간 관련 ──
  f.scheduled_utc,
  f.expected_utc,
  f.actual_utc,
  EXTRACT(DATE FROM f.scheduled_utc) AS scheduled_date,
  EXTRACT(HOUR FROM f.scheduled_utc) AS scheduled_hour,
  EXTRACT(DAYOFWEEK FROM f.scheduled_utc) AS scheduled_day_of_week,   -- 1=일, 7=토
  
  -- 시간대 그룹 (태블로 필터용)
  CASE 
    WHEN EXTRACT(HOUR FROM f.scheduled_utc) BETWEEN 5 AND 11 THEN '오전 (05-11시)'
    WHEN EXTRACT(HOUR FROM f.scheduled_utc) BETWEEN 12 AND 17 THEN '오후 (12-17시)'
    WHEN EXTRACT(HOUR FROM f.scheduled_utc) BETWEEN 18 AND 22 THEN '저녁 (18-22시)'
    ELSE '심야/새벽 (23-04시)'
  END AS time_of_day,
  
  -- 운항 상태
  -- status가 ''(빈 문자열)이지만 지연이 발생한 경우 → '지연'으로 보정
  CASE
    WHEN f.status = '' AND f.current_delay_min > 0 THEN '지연'
    WHEN f.status = '' THEN '정보없음'
    ELSE f.status
  END AS status,
  
  -- 지연 분석
  f.current_delay_min,
  
  CASE 
    WHEN f.current_delay_min IS NULL THEN '정보없음'
    WHEN f.current_delay_min <= 0 THEN '정시 운항'
    WHEN f.current_delay_min <= 15 THEN '경미한 지연 (15분 이하)'
    WHEN f.current_delay_min <= 60 THEN '보통 지연 (15분~1시간)'
    WHEN f.current_delay_min <= 120 THEN '심각한 지연 (1~2시간)'
    ELSE '매우 심각한 지연 (2시간 초과)'
  END AS delay_category,
  
  -- 통계 계산용 플래그
  CASE WHEN f.current_delay_min > 15 THEN 1 ELSE 0 END AS is_delayed_15min,
  CASE WHEN f.status = '취소' THEN 1 ELSE 0 END AS is_canceled

FROM base_flights f
LEFT JOIN bronze_info b 
  ON f.flight_key = b.flight_key
WHERE b.nature = '여객';
