

SELECT COUNT(*) as total_records
FROM `ark-of-data-2000.de_gcp_lakehouse_iceberg_taxi_dataset.taxi_trips`;

SELECT 
  COUNT(*) as records_loaded,
  MIN(created_at) as first_loaded,
  MAX(created_at) as last_loaded
FROM `ark-of-data-2000.de_gcp_lakehouse_iceberg_taxi_dataset.taxi_trips`
WHERE DATE(created_at) = CURRENT_DATE();

SELECT *
FROM `ark-of-data-2000.de_gcp_lakehouse_iceberg_taxi_dataset.taxi_trips`
ORDER BY created_at DESC
LIMIT 10;

SELECT 
  MIN(pickup_datetime) as earliest_pickup,
  MAX(pickup_datetime) as latest_pickup,
  MIN(dropoff_datetime) as earliest_dropoff,
  MAX(dropoff_datetime) as latest_dropoff,
  COUNT(*) as total_trips
FROM `ark-of-data-2000.de_gcp_lakehouse_iceberg_taxi_dataset.taxi_trips`;

SELECT 
  MIN(pickup_datetime) as earliest_pickup,
  MAX(pickup_datetime) as latest_pickup,
  MIN(dropoff_datetime) as earliest_dropoff,
  MAX(dropoff_datetime) as latest_dropoff,
  COUNT(*) as total_trips
FROM `ark-of-data-2000.de_gcp_lakehouse_iceberg_taxi_dataset.taxi_trips`;

SELECT 
  pickup_location_id,
  COUNT(*) as trip_count,
  AVG(fare_amount) as avg_fare
FROM `ark-of-data-2000.de_gcp_lakehouse_iceberg_taxi_dataset.taxi_trips`
GROUP BY pickup_location_id
ORDER BY trip_count DESC
LIMIT 10;

SELECT COUNT(*) as hourly_records
FROM `ark-of-data-2000.de_gcp_lakehouse_iceberg_taxi_dataset.hourly_trip_stats`;

SELECT 
  stat_hour,
  pickup_location_id,
  trip_count,
  total_revenue,
  avg_trip_distance,
  avg_trip_duration_minutes
FROM `ark-of-data-2000.de_gcp_lakehouse_iceberg_taxi_dataset.hourly_trip_stats`
ORDER BY stat_hour DESC, trip_count DESC
LIMIT 20;

SELECT 
  COUNT(*) as total_records,
  COUNT(pickup_datetime) as valid_pickup_times,
  COUNT(dropoff_datetime) as valid_dropoff_times,
  COUNT(fare_amount) as valid_fares,
  COUNT(pickup_location_id) as valid_pickup_locations
FROM `ark-of-data-2000.de_gcp_lakehouse_iceberg_taxi_dataset.taxi_trips`;

SELECT 
  trip_id,
  fare_amount,
  extra,
  mta_tax,
  tip_amount,
  tolls_amount,
  total_amount,
  (fare_amount + extra + mta_tax + tip_amount + tolls_amount) as calculated_total,
  ABS(total_amount - (fare_amount + extra + mta_tax + tip_amount + tolls_amount)) as difference
FROM `ark-of-data-2000.de_gcp_lakehouse_iceberg_taxi_dataset.taxi_trips`
WHERE ABS(total_amount - (fare_amount + extra + mta_tax + tip_amount + tolls_amount)) > 0.01
LIMIT 10;

SELECT 
  ROUND(pickup_longitude, 3) as pickup_lng_rounded,
  ROUND(pickup_latitude, 3) as pickup_lat_rounded,
  COUNT(*) as trip_count
FROM `ark-of-data-2000.de_gcp_lakehouse_iceberg_taxi_dataset.taxi_trips`
GROUP BY pickup_lng_rounded, pickup_lat_rounded
ORDER BY trip_count DESC
LIMIT 15;

DELETE FROM `ark-of-data-2000.de_gcp_lakehouse_iceberg_taxi_dataset.taxi_trips`
WHERE DATE(pickup_datetime) = '2025-05-16';

-- Update specific fields
UPDATE `ark-of-data-2000.de_gcp_lakehouse_iceberg_taxi_dataset.taxi_trips`
SET payment_type = 'credit_card'
WHERE payment_type = 'card';

-- Check table type in INFORMATION_SCHEMA
SELECT 
  table_name,
  table_type,
  ddl
FROM `ark-of-data-2000.de_gcp_lakehouse_iceberg_taxi_dataset.INFORMATION_SCHEMA.TABLES`
WHERE table_name IN ('taxi_trips', 'hourly_trip_stats');

-- Query historical data (if snapshots exist)
SELECT COUNT(*) 
FROM `ark-of-data-2000.de_gcp_lakehouse_iceberg_taxi_dataset.taxi_trips`
FOR SYSTEM_TIME AS OF TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 30 minute);



