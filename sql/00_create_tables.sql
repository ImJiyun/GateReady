CREATE TABLE `PROJECT_ID.flight_delay.raw_flights` (
  flight_key STRING, -- ymd  + flight_iata

  airline_icao STRING, -- 항공사 ICAO 코드
  airline_kr STRING,  -- 항공사 한글명
  flight_iata STRING, -- 항공편 IATA 코드

  arr_airport_iata STRING, -- 목적지 IATA (apIata)
  arr_airport_kr   STRING, -- 목적지 한글명 (apKr)

  status STRING,
  status_remark STRING,
  status_remark_code STRING,

  scheduled_dt DATETIME,
  expected_dt  DATETIME,
  actual_dt    DATETIME,

  nature STRING,
  ymd DATE,
  collected_at TIMESTAMP
)
PARTITION BY ymd;