--station congestion
CREATE OR REPLACE TABLE TRANSIT_GFTS.GOLD.STATION_CONGESTION AS
SELECT
    stop_id,
    COUNT(DISTINCT trip_id) AS train_count_48h
FROM TRANSIT_GFTS.SILVER.TRIPS_UPDATE_SILVER
WHERE arrival_time >= DATEADD(HOUR, -48, CURRENT_TIMESTAMP())
GROUP BY stop_id
ORDER BY train_count_48h DESC;


--train gaps analysis
CREATE OR REPLACE TABLE TRANSIT_GFTS.GOLD.TRAIN_GAPS AS
SELECT
    stop_id,
    route_id,
    arrival_time,
    DATEDIFF(
        'minute',
        LAG(arrival_time) OVER (PARTITION BY stop_id ORDER BY arrival_time),
        arrival_time
    ) AS gap_minutes
FROM TRANSIT_GFTS.SILVER.TRIPS_UPDATE_SILVER;


--gosht trains
CREATE OR REPLACE TABLE TRANSIT_GFTS.GOLD.GHOST_TRAINS AS
WITH latest_trip AS (
    SELECT
        trip_id,
        arrival_time,
        ROW_NUMBER() OVER (
            PARTITION BY trip_id
            ORDER BY arrival_time DESC
        ) AS rn
    FROM TRANSIT_GFTS.SILVER.TRIPS_UPDATE_SILVER
)
SELECT
    trip_id,
    arrival_time
FROM latest_trip
WHERE rn = 1
  AND arrival_time < DATEADD(MINUTE, -20, CURRENT_TIMESTAMP());
