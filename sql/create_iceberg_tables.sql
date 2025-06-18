-- Create main taxi trips Iceberg table in BigQuery using BigLake
CREATE TABLE `${PROJECT_ID}.${DATASET_ID}.taxi_trips`
(
  trip_id STRING,
  vendor_id INT64,
  pickup_datetime TIMESTAMP,
  dropoff_datetime TIMESTAMP,
  passenger_count INT64,
  trip_distance NUMERIC,
  pickup_longitude NUMERIC,
  pickup_latitude NUMERIC,
  dropoff_longitude NUMERIC,
  dropoff_latitude NUMERIC,
  payment_type STRING,
  fare_amount NUMERIC,
  extra NUMERIC,
  mta_tax NUMERIC,
  tip_amount NUMERIC,
  tolls_amount NUMERIC,
  total_amount NUMERIC,
  pickup_location_id INT64,
  dropoff_location_id INT64,
  created_at TIMESTAMP
)
WITH CONNECTION `${PROJECT_ID}.${REGION}.${CONNECTION_ID}`
OPTIONS (
  table_format = 'ICEBERG',
  storage_uri = 'gs://${ICEBERG_BUCKET}/taxi_trips',
  description = 'NYC Taxi trip records with Iceberg format'
);

-- Create aggregated table for hourly statistics
CREATE TABLE `${PROJECT_ID}.${DATASET_ID}.hourly_trip_stats`
(
  stat_hour TIMESTAMP,
  pickup_location_id INT64,
  trip_count INT64,
  avg_fare_amount NUMERIC,
  avg_trip_distance NUMERIC,
  avg_trip_duration_minutes NUMERIC,
  total_revenue NUMERIC,
  created_at TIMESTAMP
)
WITH CONNECTION `${PROJECT_ID}.${REGION}.${CONNECTION_ID}`
OPTIONS (
  table_format = 'ICEBERG',
  storage_uri = 'gs://${ICEBERG_BUCKET}/hourly_trip_stats',
  description = 'Hourly aggregated taxi trip statistics'
);

-- Create location lookup table
CREATE TABLE `${PROJECT_ID}.${DATASET_ID}.taxi_zones`
(
  location_id INT64,
  borough STRING,
  zone_name STRING,
  service_zone STRING,
  geometry STRING,
  created_at TIMESTAMP
)
WITH CONNECTION `${PROJECT_ID}.${REGION}.${CONNECTION_ID}`
OPTIONS (
  table_format = 'ICEBERG',
  storage_uri = 'gs://${ICEBERG_BUCKET}/taxi_zones',
  description = 'NYC taxi zone reference data'
);

-- Create error tracking table
CREATE TABLE `${PROJECT_ID}.${DATASET_ID}.processing_errors`
(
  error_id STRING,
  error_timestamp TIMESTAMP,
  error_type STRING,
  error_message STRING,
  source_data STRING,
  pipeline_name STRING,
  retry_count INT64
)
WITH CONNECTION `${PROJECT_ID}.${REGION}.${CONNECTION_ID}`
OPTIONS (
  table_format = 'ICEBERG',
  storage_uri = 'gs://${ICEBERG_BUCKET}/processing_errors',
  description = 'Data processing error tracking'
);

-- Create schema evolution log table
CREATE TABLE `${PROJECT_ID}.${DATASET_ID}.schema_evolution_log`
(
  evolution_id STRING,
  table_name STRING,
  change_type STRING,
  change_description STRING,
  applied_timestamp TIMESTAMP,
  applied_by STRING,
  rollback_script STRING
)
WITH CONNECTION `${PROJECT_ID}.${REGION}.${CONNECTION_ID}`
OPTIONS (
  table_format = 'ICEBERG',
  storage_uri = 'gs://${ICEBERG_BUCKET}/schema_evolution_log',
  description = 'Schema evolution tracking for audit purposes'
);

-- Insert initial taxi zones data
INSERT INTO `${PROJECT_ID}.${DATASET_ID}.taxi_zones`
(location_id, borough, zone_name, service_zone, geometry)
VALUES
  (1, 'EWR', 'Newark Airport', 'EWR', 'POINT(-74.1745 40.6895)'),
  (2, 'Queens', 'Jamaica Bay', 'Boro Zone', 'POINT(-73.8370 40.6089)'),
  (3, 'Bronx', 'Allerton/Pelham Gardens', 'Boro Zone', 'POINT(-73.8478 40.8656)'),
  (4, 'Manhattan', 'Alphabet City', 'Yellow Zone', 'POINT(-73.9776 40.7258)'),
  (5, 'Staten Island', 'Arden Heights', 'Boro Zone', 'POINT(-74.1827 40.5560)'),
  (6, 'Staten Island', 'Arrochar/Fort Wadsworth', 'Boro Zone', 'POINT(-74.0633 40.6067)'),
  (7, 'Queens', 'Astoria', 'Boro Zone', 'POINT(-73.9249 40.7648)'),
  (8, 'Queens', 'Astoria Park', 'Boro Zone', 'POINT(-73.9200 40.7796)'),
  (9, 'Queens', 'Auburndale', 'Boro Zone', 'POINT(-73.7963 40.7567)'),
  (10, 'Queens', 'Baisley Park', 'Boro Zone', 'POINT(-73.7707 40.6957)'),
  (90, 'Manhattan', 'Financial District South', 'Yellow Zone', 'POINT(-74.0776 40.7282)'),
  (132, 'Manhattan', 'Upper East Side South', 'Yellow Zone', 'POINT(-73.9712 40.7831)'),
  (161, 'Manhattan', 'Midtown Center', 'Yellow Zone', 'POINT(-73.9851 40.7589)'),
  (186, 'Brooklyn', 'Brooklyn Heights', 'Boro Zone', 'POINT(-73.9442 40.6781)'),
  (237, 'Manhattan', 'Times Sq/Theatre District', 'Yellow Zone', 'POINT(-73.9934 40.7505)');

-- Create views for backward compatibility
CREATE OR REPLACE VIEW `${PROJECT_ID}.${DATASET_ID}.taxi_trips_v1`
AS SELECT 
  trip_id,
  vendor_id,
  pickup_datetime,
  dropoff_datetime,
  passenger_count,
  trip_distance,
  pickup_longitude,
  pickup_latitude,
  dropoff_longitude,
  dropoff_latitude,
  payment_type,
  fare_amount,
  extra,
  mta_tax,
  tip_amount,
  tolls_amount,
  total_amount,
  pickup_location_id,
  dropoff_location_id
FROM `${PROJECT_ID}.${DATASET_ID}.taxi_trips`;

-- Create view for common aggregations
CREATE OR REPLACE VIEW `${PROJECT_ID}.${DATASET_ID}.daily_zone_stats`
AS
SELECT 
  DATE(pickup_datetime) as stat_date,
  pickup_location_id,
  COUNT(*) as daily_trips,
  SUM(total_amount) as daily_revenue,
  AVG(fare_amount) as avg_fare,
  AVG(trip_distance) as avg_distance,
  AVG(TIMESTAMP_DIFF(dropoff_datetime, pickup_datetime, MINUTE)) as avg_duration_minutes,
  COUNT(DISTINCT payment_type) as payment_type_variety,
  SUM(CASE WHEN tip_amount > 0 THEN 1 ELSE 0 END) / COUNT(*) as tip_percentage
FROM `${PROJECT_ID}.${DATASET_ID}.taxi_trips`
WHERE pickup_datetime >= '2020-01-01'
GROUP BY stat_date, pickup_location_id; 