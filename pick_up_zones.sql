-- Which were the top pickup locations with over 13,000 in total_amount (across all trips) for 2019-10-18?

SELECT 
	"taxi_zone"."Zone",
	SUM(total_amount) AS total_zone_amount
FROM green_tripdata
JOIN taxi_zone
ON "green_tripdata"."PULocationID" = "taxi_zone"."LocationID"
WHERE DATE(lpep_pickup_datetime) = '2019-10-18'
GROUP BY "taxi_zone"."Zone"
HAVING SUM(total_amount) > 13000
ORDER BY SUM(total_amount) DESC
