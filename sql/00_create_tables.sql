CREATE TABLE `PROJECT_ID.flight_delay.raw_flights` (
  flight_key STRING, -- ymd  + flight_iata

  airline_icao STRING,
  airline_kr STRING,
  flight_iata STRING,

  dep_airport_iata STRING,
  dep_airport_kr STRING,

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