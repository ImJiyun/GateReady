CREATE TABLE IF NOT EXISTS `PROJECT_ID.silver.flights_snapshots` (
  flight_key STRING,
  airline_icao STRING,
  airline_kr STRING,        -- 항공사 한글명 (예: 대한항공)
  flight_iata STRING,
  arr_airport_iata STRING,  -- 도착 공항 코드 (예: NRT)
  arr_airport_kr STRING,    -- 도착 공항 한글명 (예: 나리타)
  nature STRING,            -- 운항 유형 (여객/화물/기타)
  
  scheduled_utc TIMESTAMP,    -- 정제된 예정 시간 (UTC)
  expected_utc TIMESTAMP,     -- 정제된 예상 시간 (UTC)
  actual_utc TIMESTAMP,       -- 정제된 실제 시간 (UTC)
  
  status STRING,            -- 항공 상태 (출발, 지연 등)
  status_remark STRING,     -- 지연 사유 (예: 기상, 정비 등)
  current_delay_min FLOAT64, -- 계산된 지연 시간 (분)
  
  collected_at TIMESTAMP,   -- 데이터 수집 시점 (Snapshot 시점, UTC)
  ymd STRING                -- 운항 일자 (YYYYMMDD, KST 기준)
)
PARTITION BY 
  DATE(scheduled_utc)
CLUSTER BY 
  flight_key;
