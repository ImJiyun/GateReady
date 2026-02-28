CREATE TABLE IF NOT EXISTS `PROJECT_ID.silver.flights_snapshots` (
  flight_key STRING,
  airline_icao STRING,
  flight_iata STRING,
  
  scheduled_dt DATETIME,    -- 정제된 예정 시간
  expected_dt DATETIME,     -- 정제된 예상 시간
  actual_dt DATETIME,       -- 정제된 실제 시간
  
  status STRING,            -- 항공 상태 (출발, 지연 등)
  current_delay_min FLOAT64, -- 계산된 지연 시간 (분)
  
  collected_at TIMESTAMP,   -- 데이터 수집 시점 (Snapshot 시점)
  ymd STRING                -- 운항 일자 (YYYYMMDD)
)
PARTITION BY 
  DATE(scheduled_dt)
CLUSTER BY 
  flight_iata, 
  airline_icao;
