-- ============================================================================
-- Cloud Spanner DDL Script for Taxi Lakehouse with Sliding Window Analytics
-- ============================================================================
-- This script creates the main tables required for the Dataflow streaming job
-- with sliding window functionality in Cloud Spanner.
--
-- NOTE: This file uses Spanner-specific DDL syntax (OPTIONS, PRIMARY KEY)
-- which may show linter errors in generic SQL parsers but is correct for Spanner.
--
-- Usage:
--   gcloud spanner databases ddl update DATABASE_NAME --instance=INSTANCE_NAME \
--     --ddl-file=sql/create_spanner_tables.sql
--
-- Tables Created:
--   1. taxi_trips - Main trip records from Pub/Sub stream
--   2. location_features_sliding_window - 30-minute sliding window analytics
-- ============================================================================

-- ============================================================================
-- 1. MAIN TAXI TRIPS TABLE
-- ============================================================================
-- Stores individual taxi trip completion events from Pub/Sub stream
-- Primary Key: trip_id (globally unique)
-- Secondary Index: pickup_location_id, pickup_datetime for analytics queries

CREATE TABLE taxi_trips (
  -- Primary identifier
  trip_id STRING(MAX) NOT NULL,
  
  -- Trip metadata
  vendor_id INT64,
  pickup_datetime TIMESTAMP,
  dropoff_datetime TIMESTAMP,
  
  -- Trip details
  passenger_count INT64,
  trip_distance NUMERIC,
  
  -- Location coordinates (for backup/validation)
  pickup_longitude NUMERIC,
  pickup_latitude NUMERIC,
  dropoff_longitude NUMERIC,
  dropoff_latitude NUMERIC,
  
  -- Location zone IDs (primary for analytics)
  pickup_location_id INT64,
  dropoff_location_id INT64,
  
  -- Payment information
  payment_type STRING(50),
  fare_amount NUMERIC,
  extra NUMERIC,
  mta_tax NUMERIC,
  tip_amount NUMERIC,
  tolls_amount NUMERIC,
  total_amount NUMERIC,
  
  -- Processing metadata
  created_at TIMESTAMP NOT NULL OPTIONS (allow_commit_timestamp=true),
  
  -- Primary key for global uniqueness and distribution
  PRIMARY KEY (trip_id)
);

-- ============================================================================
-- 2. SLIDING WINDOW LOCATION FEATURES TABLE
-- ============================================================================
-- Stores 30-minute sliding window analytics per pickup location
-- Primary Key: Composite key with window_start, pickup_location_id
-- Optimized for time-series queries and location-based analytics

CREATE TABLE location_features_sliding_window (
  -- Window time boundaries
  window_start TIMESTAMP NOT NULL,
  window_end TIMESTAMP NOT NULL,
  
  -- Location identifiers
  pickup_location_id INT64 NOT NULL,
  location_name STRING(255),
  borough STRING(50),
  demand_tier STRING(20), -- 'high', 'medium', 'low'
  
  -- Core demand metrics
  trip_count INT64,
  demand_intensity NUMERIC, -- trips per minute
  destination_diversity INT64, -- unique dropoff locations
  
  -- Revenue analytics
  total_revenue NUMERIC,
  avg_fare NUMERIC,
  revenue_per_minute NUMERIC,
  
  -- Operational metrics
  avg_distance NUMERIC,
  avg_duration_minutes NUMERIC,
  avg_trip_efficiency NUMERIC, -- distance/duration ratio
  avg_passengers NUMERIC,
  
  -- Payment behavior
  credit_card_percentage NUMERIC,
  tip_percentage NUMERIC,
  
  -- Time-based indicators
  rush_hour_indicator BOOL,
  
  -- Anomaly detection flags
  high_demand_flag BOOL,      -- Unusually high trip volume
  premium_zone_flag BOOL,     -- High-value zone indicator  
  long_distance_flag BOOL,    -- Above-average trip distances
  anomaly_score NUMERIC,      -- Composite anomaly score (0-1)
  
  -- Processing metadata
  created_at TIMESTAMP NOT NULL OPTIONS (allow_commit_timestamp=true),
  
  -- Composite primary key for time-series partitioning
  -- Orders by time first, then location for efficient range queries
  PRIMARY KEY (window_start, pickup_location_id)
);

-- ============================================================================
-- SECONDARY INDEXES FOR QUERY OPTIMIZATION
-- ============================================================================

-- Index for taxi_trips: Optimize pickup location and time-based queries
CREATE INDEX idx_taxi_trips_location_time ON taxi_trips (
  pickup_location_id, 
  pickup_datetime DESC
);

-- Index for taxi_trips: Optimize revenue analysis queries
CREATE INDEX idx_taxi_trips_revenue ON taxi_trips (
  pickup_datetime DESC,
  total_amount,
  payment_type
);

-- Index for sliding window: Optimize real-time monitoring queries
CREATE INDEX idx_sliding_window_current ON location_features_sliding_window (
  pickup_location_id,
  window_start DESC
);

-- Index for sliding window: Optimize borough-level aggregations
CREATE INDEX idx_sliding_window_borough ON location_features_sliding_window (
  borough,
  demand_tier,
  window_start DESC
);

-- Index for sliding window: Optimize anomaly detection queries
CREATE INDEX idx_sliding_window_anomalies ON location_features_sliding_window (
  window_start DESC,
  anomaly_score DESC,
  high_demand_flag,
  premium_zone_flag
);

-- ============================================================================
-- SAMPLE QUERIES FOR VALIDATION
-- ============================================================================

-- Query 1: Recent trips by location
-- SELECT pickup_location_id, COUNT(*) as trip_count, AVG(total_amount) as avg_fare
-- FROM taxi_trips 
-- WHERE pickup_datetime >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 HOUR)
-- GROUP BY pickup_location_id
-- ORDER BY trip_count DESC
-- LIMIT 10;

-- Query 2: Current sliding window analytics
-- SELECT 
--   pickup_location_id,
--   location_name,
--   borough,
--   trip_count,
--   demand_intensity,
--   total_revenue,
--   high_demand_flag,
--   anomaly_score
-- FROM location_features_sliding_window
-- WHERE window_start >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 30 MINUTE)
-- ORDER BY demand_intensity DESC
-- LIMIT 20;

-- Query 3: Anomaly detection across boroughs
-- SELECT 
--   borough,
--   COUNT(*) as locations_with_anomalies,
--   AVG(anomaly_score) as avg_anomaly_score,
--   MAX(demand_intensity) as peak_demand
-- FROM location_features_sliding_window
-- WHERE window_start >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 2 HOUR)
--   AND (high_demand_flag = true OR anomaly_score > 0.7)
-- GROUP BY borough
-- ORDER BY avg_anomaly_score DESC;

-- ============================================================================
-- PERFORMANCE AND SCALING NOTES
-- ============================================================================
--
-- 1. PRIMARY KEY DESIGN:
--    - taxi_trips: Uses trip_id for global distribution
--    - sliding_window: Uses (window_start, pickup_location_id) for time-series efficiency
--
-- 2. HOTSPOTTING MITIGATION:
--    - trip_id includes UUID components for distribution
--    - Time-based primary key in sliding_window is acceptable for analytics workload
--
-- 3. QUERY PATTERNS:
--    - Recent trips by location: Optimized with idx_taxi_trips_location_time
--    - Real-time monitoring: Optimized with idx_sliding_window_current
--    - Cross-borough analytics: Optimized with idx_sliding_window_borough
--    - Anomaly detection: Optimized with idx_sliding_window_anomalies
--
-- 4. STORAGE OPTIMIZATION:
--    - STRING fields have appropriate max lengths
--    - NUMERIC used for precise financial calculations
--    - BOOL used instead of STRING for flags
--
-- 5. DATAFLOW INTEGRATION:
--    - commit_timestamp=true enables automatic timestamping
--    - Schema matches Dataflow pipeline expectations
--    - Indexes support both streaming inserts and analytical queries
--
-- ============================================================================ 