-- During the period of October 1st 2019 (inclusive) and November 1st 2019 (exclusive), how many trips, respectively, happened:

-- Up to 1 mile
-- In between 1 (exclusive) and 3 miles (inclusive),
-- In between 3 (exclusive) and 7 miles (inclusive),
-- In between 7 (exclusive) and 10 miles (inclusive),
-- Over 10 miles
SELECT 
	COUNT(CASE
			WHEN trip_distance <= 1 THEN 1
			ELSE NULL
		END) AS under_1_mile,
	COUNT(CASE
			WHEN trip_distance > 1 AND trip_distance <= 3
			THEN 1
			ELSE NULL
		END) AS between_1_and_3_miles,
	COUNT(
		CASE
			WHEN trip_distance > 3 AND trip_distance <= 7
			THEN 1
			ELSE NULL
		END) AS between_3_and_7_miles,
	COUNT(
		CASE
			WHEN trip_distance > 7 AND trip_distance <= 10
			THEN 1
			ELSE NULL
		END
	) AS between_7_and_10_miles,
	COUNT(
		CASE
			WHEN trip_distance > 10
			THEN 1
			ELSE NULL
		END
	) AS over_10_miles
FROM green_tripdata
WHERE
	DATE(lpep_pickup_datetime) >= '2019-10-01'
AND
	DATE(lpep_pickup_datetime) < '2019-11-01'
LIMIT 10