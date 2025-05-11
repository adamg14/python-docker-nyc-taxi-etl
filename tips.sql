-- For the passengers picked up in October 2019 in the zone named "East Harlem North" which was the drop off zone that had the largest tip?

SELECT 
	"dropoff_zone"."Zone"
FROM green_tripdata
JOIN taxi_zone AS pickup_zone
ON "green_tripdata"."PULocationID" = "pickup_zone"."LocationID"
JOIN taxi_zone AS dropoff_zone
ON "green_tripdata"."DOLocationID" = "dropoff_zone"."LocationID"
WHERE "pickup_zone"."Zone" = 'East Harlem North'
AND EXTRACT(MONTH FROM lpep_pickup_datetime) = 10
AND EXTRACT(YEAR FROM lpep_pickup_datetime) = 2019
ORDER BY tip_amount DESC
LIMIT 1