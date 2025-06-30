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

-- Create location features table for sliding window analytics
CREATE TABLE `${PROJECT_ID}.${DATASET_ID}.location_features_sliding_window`
(
  pickup_location_id INT64,
  window_start TIMESTAMP,
  window_end TIMESTAMP,
  
  -- Volume and velocity features
  trip_count INT64,
  demand_intensity_per_minute NUMERIC,
  avg_inter_arrival_minutes NUMERIC,
  
  -- Revenue features
  total_revenue NUMERIC,
  avg_trip_amount NUMERIC,
  std_trip_amount NUMERIC,
  max_trip_amount NUMERIC,
  min_trip_amount NUMERIC,
  revenue_coefficient_variation NUMERIC,
  
  -- Distance features
  total_distance NUMERIC,
  avg_trip_distance NUMERIC,
  std_trip_distance NUMERIC,
  max_trip_distance NUMERIC,
  
  -- Duration features
  avg_trip_duration_minutes NUMERIC,
  std_trip_duration_minutes NUMERIC,
  
  -- Tip and service features
  total_tips NUMERIC,
  avg_tip_amount NUMERIC,
  tip_percentage NUMERIC,
  
  -- Destination diversity
  unique_dropoff_locations INT64,
  destination_diversity_ratio NUMERIC,
  
  -- Temporal features
  unique_hours_of_day INT64,
  weekend_trip_count INT64,
  weekend_trip_ratio NUMERIC,
  rush_hour_trip_count INT64,
  rush_hour_trip_ratio NUMERIC,
  
  -- Performance indicators
  high_demand_flag BOOLEAN,
  high_revenue_flag BOOLEAN,
  long_distance_flag BOOLEAN,
  premium_location_flag BOOLEAN,
  
  created_at TIMESTAMP
)
WITH CONNECTION `${PROJECT_ID}.${REGION}.${CONNECTION_ID}`
OPTIONS (
  table_format = 'ICEBERG',
  storage_uri = 'gs://${ICEBERG_BUCKET}/location_features_sliding_window',
  description = 'Real-time location-based features from sliding window analysis for demand forecasting and anomaly detection'
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

-- Create windowed statistics table for streaming aggregations (legacy - kept for backward compatibility)
CREATE TABLE `${PROJECT_ID}.${DATASET_ID}.windowed_trip_stats`
(
  stat_hour TIMESTAMP,
  pickup_location_id INT64,
  trip_count INT64,
  avg_fare_amount NUMERIC,
  avg_trip_distance NUMERIC,
  avg_trip_duration_minutes NUMERIC,
  total_revenue NUMERIC,
  window_start TIMESTAMP,
  window_end TIMESTAMP,
  created_at TIMESTAMP
)
WITH CONNECTION `${PROJECT_ID}.${REGION}.${CONNECTION_ID}`
OPTIONS (
  table_format = 'ICEBERG',
  storage_uri = 'gs://${ICEBERG_BUCKET}/windowed_trip_stats',
  description = 'Legacy windowed aggregated taxi trip statistics'
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

-- Create view for location demand analytics
CREATE OR REPLACE VIEW `${PROJECT_ID}.${DATASET_ID}.location_demand_analytics`
AS
SELECT 
  pickup_location_id,
  window_start,
  window_end,
  trip_count,
  demand_intensity_per_minute,
  total_revenue,
  avg_trip_amount,
  destination_diversity_ratio,
  rush_hour_trip_ratio,
  weekend_trip_ratio,
  high_demand_flag,
  premium_location_flag,
  
  -- Calculate percentile rankings for comparative analysis
  PERCENT_RANK() OVER (PARTITION BY pickup_location_id ORDER BY demand_intensity_per_minute) as demand_intensity_percentile,
  PERCENT_RANK() OVER (PARTITION BY pickup_location_id ORDER BY total_revenue) as revenue_percentile,
  PERCENT_RANK() OVER (PARTITION BY pickup_location_id ORDER BY trip_count) as volume_percentile,
  
  -- Rolling averages for trend analysis
  AVG(demand_intensity_per_minute) OVER (
    PARTITION BY pickup_location_id 
    ORDER BY window_start 
    ROWS BETWEEN 5 PRECEDING AND CURRENT ROW
  ) as rolling_avg_demand_intensity,
  
  AVG(total_revenue) OVER (
    PARTITION BY pickup_location_id 
    ORDER BY window_start 
    ROWS BETWEEN 5 PRECEDING AND CURRENT ROW
  ) as rolling_avg_revenue,
  
  created_at
FROM `${PROJECT_ID}.${DATASET_ID}.location_features_sliding_window`
WHERE window_start >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 24 HOUR);

-- Create view for real-time alerting and monitoring
CREATE OR REPLACE VIEW `${PROJECT_ID}.${DATASET_ID}.real_time_location_alerts`
AS
SELECT 
  pickup_location_id,
  window_start,
  window_end,
  
  -- Alert conditions
  CASE 
    WHEN high_demand_flag AND demand_intensity_per_minute > 2.0 THEN 'SURGE_DEMAND'
    WHEN trip_count = 0 AND window_start > TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 HOUR) THEN 'NO_ACTIVITY'
    WHEN premium_location_flag AND avg_trip_amount > 50.0 THEN 'HIGH_VALUE_ZONE'
    WHEN long_distance_flag AND max_trip_distance > 30.0 THEN 'LONG_DISTANCE_TRIPS'
    ELSE 'NORMAL'
  END as alert_type,
  
  -- Alert severity
  CASE 
    WHEN demand_intensity_per_minute > 3.0 THEN 'HIGH'
    WHEN demand_intensity_per_minute > 1.5 THEN 'MEDIUM'
    ELSE 'LOW'
  END as alert_severity,
  
  trip_count,
  demand_intensity_per_minute,
  total_revenue,
  avg_trip_amount,
  destination_diversity_ratio,
  created_at
  
FROM `${PROJECT_ID}.${DATASET_ID}.location_features_sliding_window`
WHERE window_start >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 2 HOUR)
  AND (high_demand_flag OR premium_location_flag OR long_distance_flag OR trip_count = 0); 