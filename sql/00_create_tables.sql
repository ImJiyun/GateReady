CREATE TABLE `PROJECT_ID.flight_delay.raw_flights` (
  airline_icao STRING, -- 항공사 ICAO 코드
  airline_kr STRING, -- 항공사명 (한글)
  flight_iata STRING, -- 항공편 IATA 코드
  dep_airport_iata STRING, -- 출발 공항 IATA 코드
  dep_airport_kr STRING, -- 출발 공항명 (한글)
  status STRING, -- 운항 상태
  status_remark STRING, -- 운항 상태 상세
  status_remark_code STRING, -- 운항 상태 상세 코드
  scheduled_time TIME, -- 예정 출발 시각
  expected_time TIME, -- 예상 출발 시각
  actual_time TIME, -- 실제 출발 시각
  nature STRING,     -- 여객/화물
  cargo_yn BOOL,     -- nature 기반 파생
  ymd DATE,
  collected_at TIMESTAMP
)
PARTITION BY ymd;