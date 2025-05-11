-- Which was the pick up day with the longest trip distance? Use the pick up time for your calculations.
SELECT
	DATE(lpep_pickup_datetime),
	MAX(trip_distance) AS longest_trip
FROM green_tripdata
GROUP BY DATE(lpep_pickup_datetime)
ORDER BY MAX(trip_distance) DESC
LIMIT 1