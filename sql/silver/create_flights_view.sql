CREATE OR REPLACE VIEW `silver.flights` AS
WITH latest_snapshot AS (
  SELECT 
    *,
    ROW_NUMBER() OVER(
      PARTITION BY ymd, flight_iata 
      ORDER BY collected_at DESC
    ) as row_num
  FROM 
    `silver.flights_snapshots`
)
SELECT 
  * 
  EXCEPT(row_num)
FROM 
  latest_snapshot
WHERE 
  row_num = 1;
