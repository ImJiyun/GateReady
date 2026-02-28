CREATE TABLE IF NOT EXISTS `PROJECT_ID.bronze.flights` (
  flight_key STRING,
  
  airline_icao STRING,
  airline_kr STRING,
  flight_iata STRING,
  arr_airport_iata STRING,
  arr_airport_kr STRING,
  
  scheduled_time STRING,      
  expected_time STRING,       
  actual_time STRING,         
  
  status STRING,
  status_remark STRING,
  status_remark_code STRING,
  
  nature STRING,
  
  ymd STRING,                 
  collected_at TIMESTAMP,
  
  raw_json STRING
)
PARTITION BY DATE(collected_at)
CLUSTER BY airline_icao, ymd;