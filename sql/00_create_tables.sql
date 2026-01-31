CREATE TABLE `PROJECT_ID.raw.raw_flights` (
  flight_key STRING, -- ymd  + flight_iata

  airline_icao STRING, -- 항공사 ICAO 코드
  airline_kr STRING,  -- 항공사 한글명
  flight_iata STRING, -- 항공편 IATA 코드

  arr_airport_iata STRING, -- 목적지 IATA (apIata)
  arr_airport_kr   STRING, -- 목적지 한글명 (apKr)

  status STRING,
  status_remark STRING,
  status_remark_code STRING,

  scheduled_utc_ts   TIMESTAMP,
  expected_utc_ts    TIMESTAMP,
  actual_utc_ts      TIMESTAMP,

  nature STRING,
  ymd DATE,
  collected_at TIMESTAMP
)
PARTITION BY ymd;

CREATE TABLE `PROJECT_ID.raw.airlines` (
  airline_icao STRING, -- 항공사 ICAO 코드
  airline_iata STRING, -- 항공사 IATA 코드
  airline_kr STRING,   -- 항공사 한글명
  airline_en STRING,    -- 항공사 영문명

  city_kr STRING,    -- 본사 도시 한글명
  city_en STRING,    -- 본사 도시 영문명

  country_kr STRING,  -- 본사 국가 한글명
  country_en STRING,   -- 본사 국가 영문명

  continent_kr STRING, -- 본사 대륙 한글명
  continent_en STRING,  -- 본사 대륙 영문명

  business_model STRING, -- 항공사 비즈니스 모델 
  operating_status STRING, -- 운영 상태 

  collected_at TIMESTAMP -- 데이터 수집 시각
); 