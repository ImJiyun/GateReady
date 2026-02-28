CREATE TABLE IF NOT EXISTS `PROJECT_ID.bronze.airlines` (
  airline_icao STRING, 
  airline_iata STRING, 
  airline_kr STRING,   
  airline_en STRING,    

  city_kr STRING,    
  city_en STRING,    

  country_kr STRING,  
  country_en STRING,   

  continent_kr STRING, 
  continent_en STRING,  

  business_model STRING, 
  operating_status STRING, 

  collected_at TIMESTAMP 
); 