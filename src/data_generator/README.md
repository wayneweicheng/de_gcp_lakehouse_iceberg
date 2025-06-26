# NYC Taxi Trip Data Generator

This module provides tools for generating realistic NYC taxi trip data and publishing it to Google Cloud Pub/Sub for real-time processing.

## Overview

The data generator consists of two main components:

1. **`taxi_trip_simulator.py`** - Standalone script for generating and publishing taxi trip data
2. **`main.py`** - Cloud Function entry point for serverless data generation

## Files

- `taxi_trip_simulator.py` - Main simulator class and CLI script
- `main.py` - Cloud Function wrapper for the simulator
- `requirements.txt` - Python dependencies for the data generator

## Features

- **Realistic Data Generation**: Creates authentic NYC taxi trip data with proper:
  - Geographic coordinates (NYC taxi zones)
  - Fare calculations based on distance and time
  - Payment type distributions
  - Trip duration patterns
  - Rush hour traffic variations

- **Multiple Operation Modes**:
  - **Batch**: Generate a specific number of trips
  - **Continuous**: Stream trips at a specified rate
  - **Historical**: Generate backdated trip data

- **Pub/Sub Integration**: Publishes generated trips to Google Cloud Pub/Sub topics

## Usage

### Prerequisites

1. **Google Cloud Setup**:
   ```bash
   # Authenticate with Google Cloud
   gcloud auth application-default login
   
   # Set your project
   gcloud config set project YOUR_PROJECT_ID
   ```

2. **Create Pub/Sub Topic** (if not already created):
   ```bash
   gcloud pubsub topics create taxi-trips-stream
   ```

3. **Install Dependencies**:
   ```bash
   pip install -r requirements.txt
   ```

### Running the Simulator

#### 1. Batch Mode
Generate a specific number of taxi trips:

```bash
python src/data_generator/taxi_trip_simulator.py \
    --project_id=YOUR_PROJECT_ID \
    --topic_name=taxi-trips-stream \
    --mode=batch \
    --trip_count=50
```

**Parameters:**
- `--trip_count`: Number of trips to generate (default: 100)

#### 2. Continuous Mode
Stream taxi trips continuously at a specified rate:

```bash
python src/data_generator/taxi_trip_simulator.py \
    --project_id=YOUR_PROJECT_ID \
    --topic_name=taxi-trips-stream \
    --mode=continuous \
    --trips_per_minute=10 \
    --duration_hours=1
```

**Parameters:**
- `--trips_per_minute`: Rate of trip generation (default: 10)
- `--duration_hours`: How long to run the simulation (default: 1)

#### 3. Historical Mode
Generate historical trip data for a date range:

```bash
python src/data_generator/taxi_trip_simulator.py \
    --project_id=YOUR_PROJECT_ID \
    --topic_name=taxi-trips-stream \
    --mode=historical \
    --start_date=2024-01-01 \
    --end_date=2024-01-02 \
    --trips_per_day=1000
```

**Parameters:**
- `--start_date`: Start date in YYYY-MM-DD format
- `--end_date`: End date in YYYY-MM-DD format
- `--trips_per_day`: Number of trips per day (default: 1000)

### Common Parameters

All modes support these parameters:

- `--project_id`: **Required** - Your Google Cloud Project ID
- `--topic_name`: Pub/Sub topic name (default: `taxi-trips-stream`)

### Example Commands

#### Quick Test (10 trips)
```bash
python src/data_generator/taxi_trip_simulator.py \
    --project_id=YOUR_PROJECT_ID \
    --mode=batch \
    --trip_count=10
```

#### Realistic Stream (1 hour)
```bash
python src/data_generator/taxi_trip_simulator.py \
    --project_id=YOUR_PROJECT_ID \
    --mode=continuous \
    --trips_per_minute=5 \
    --duration_hours=1
```

#### Generate Yesterday's Data
```bash
python src/data_generator/taxi_trip_simulator.py \
    --project_id=YOUR_PROJECT_ID \
    --mode=historical \
    --start_date=2024-12-25 \
    --end_date=2024-12-25 \
    --trips_per_day=500
```

## Generated Data Format

Each taxi trip event includes the following fields:

```json
{
    "trip_id": "live_12345678-1234-1234-1234-123456789abc",
    "vendor_id": 1,
    "pickup_datetime": "2024-12-26T10:30:00",
    "dropoff_datetime": "2024-12-26T10:45:00",
    "passenger_count": 1,
    "trip_distance": 2.5,
    "pickup_longitude": -73.9851,
    "pickup_latitude": 40.7589,
    "dropoff_longitude": -73.9934,
    "dropoff_latitude": 40.7505,
    "payment_type": "card",
    "fare_amount": 12.50,
    "extra": 0.50,
    "mta_tax": 0.50,
    "tip_amount": 2.50,
    "tolls_amount": 0.00,
    "total_amount": 16.00,
    "pickup_location_id": 161,
    "dropoff_location_id": 237,
    "event_timestamp": "2024-12-26T10:45:30"
}
```

## Cloud Function Deployment

The `main.py` file can be deployed as a Cloud Function for serverless data generation:

```bash
gcloud functions deploy generate-taxi-data \
    --runtime python39 \
    --trigger-topic taxi-data-trigger \
    --entry-point generate_taxi_data \
    --set-env-vars PROJECT_ID=YOUR_PROJECT_ID,TOPIC_NAME=taxi-trips-stream
```

## Monitoring

The simulator provides detailed logging:

- Trip generation progress
- Publishing status
- Error handling
- Performance metrics

Example output:
```
INFO:__main__:Generating batch of 100 trips...
INFO:__main__:Published 10/100 trips
INFO:__main__:Published 20/100 trips
...
INFO:__main__:Batch simulation completed. Published 100 trips
```

## Configuration

### Environment Variables

For Cloud Function deployment:
- `PROJECT_ID`: Google Cloud Project ID
- `TOPIC_NAME`: Pub/Sub topic name (default: `taxi-trips-stream`)

### NYC Taxi Zones

The simulator includes realistic NYC taxi pickup locations:
- Midtown Center (161)
- Times Square (237)
- Brooklyn Heights (186)
- Upper East Side (132)
- Financial District (90)
- Newark Airport (1)
- And more...

### Payment Types

Realistic payment type distribution:
- Card: 70%
- Cash: 25%
- No Charge: 3%
- Dispute: 2%

## Troubleshooting

### Common Issues

1. **Authentication Error**:
   ```bash
   gcloud auth application-default login
   ```

2. **Topic Not Found**:
   ```bash
   gcloud pubsub topics create taxi-trips-stream
   ```

3. **Permission Denied**:
   Ensure your service account has `pubsub.publisher` role

4. **Import Errors**:
   ```bash
   pip install -r requirements.txt
   ```

### Logs

Check logs for detailed error information:
```bash
# For Cloud Functions
gcloud functions logs read generate-taxi-data

# For local runs
# Logs are printed to stdout
```

## Integration with Dataflow

This data generator is designed to work with the Dataflow streaming pipeline:

1. **Generate Data**: Use the simulator to publish to `taxi-trips-stream`
2. **Process Stream**: Dataflow reads from the topic and processes the data
3. **Store Results**: Processed data is written to BigQuery Iceberg tables

## Performance

### Throughput Recommendations

- **Development**: 1-10 trips/minute
- **Testing**: 10-100 trips/minute  
- **Load Testing**: 100+ trips/minute

### Resource Usage

- **Memory**: ~50MB for continuous mode
- **CPU**: Low usage, I/O bound
- **Network**: Depends on trip rate and message size

## License

Part of the DE GCP Lakehouse Iceberg project.
