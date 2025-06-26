# GCP Lakehouse Iceberg

A modern data lakehouse solution using Google Cloud Platform with Apache Iceberg tables.

## Features

- BigQuery Iceberg tables for ACID transactions
- Real-time and batch data processing with Dataflow
- Pub/Sub for streaming data ingestion
- Cloud Functions for data generation
- Terraform infrastructure as code

## Architecture

This project implements a complete data lakehouse on GCP using:

- **Storage**: GCS buckets with Iceberg format
- **Compute**: Dataflow for ETL processing
- **Analytics**: BigQuery for querying
- **Streaming**: Pub/Sub for real-time data
- **Orchestration**: Cloud Scheduler and Cloud Functions

## Getting Started

1. Configure your environment variables in `.env`
2. Run `./scripts/deploy.sh` to deploy the infrastructure
3. Use the Dataflow Flex Templates for processing data

## 🚀 Dataflow Job Execution

This project uses modern **Dataflow Flex Templates** with custom Docker images stored in **Artifact Registry**. The unified pipeline supports both batch and streaming modes with a single codebase.

### Available Templates

- **Batch Processing**: `gs://your-bucket/flex-templates/batch-taxi-processor.json`
- **Streaming Processing**: `gs://your-bucket/flex-templates/streaming-taxi-processor.json`

### Prerequisites

Before running any Dataflow jobs, ensure:

1. **Required Tables Exist**: All BigQuery tables must be created first
   ```bash
   # Create all required tables (including windowed_trip_stats for streaming)
   bq mk --table your-project:your-dataset.windowed_trip_stats \
     stat_hour:TIMESTAMP,pickup_location_id:INTEGER,trip_count:INTEGER,avg_fare_amount:NUMERIC,avg_trip_distance:NUMERIC,avg_trip_duration_minutes:NUMERIC,total_revenue:NUMERIC,window_start:TIMESTAMP,window_end:TIMESTAMP,created_at:TIMESTAMP
   ```

2. **Docker Image Built**: Templates require the latest Docker image
   ```bash
   # Build and push Docker image
   docker build -t your-region-docker.pkg.dev/your-project/dataflow-templates/lakehouse-iceberg:latest .
   docker push your-region-docker.pkg.dev/your-project/dataflow-templates/lakehouse-iceberg:latest
   ```

3. **Flex Templates Built**: Templates must be up-to-date
   ```bash
   ./scripts/build_flex_templates.sh
   ```

### 📦 Batch Processing Jobs

Batch jobs process historical data files from GCS and load them into BigQuery Iceberg tables.

#### Quick Start - Batch Jobs

```bash
# Using the convenience script (recommended)
./scripts/run_batch_job.sh

# Or specify custom input files
./scripts/run_batch_job.sh "gs://your-bucket/custom-data/*.json"
```

#### Manual Batch Job Execution

```bash
gcloud dataflow flex-template run "batch-taxi-$(date +%Y%m%d-%H%M%S)" \
    --template-file-gcs-location="gs://your-temp-bucket/flex-templates/batch-taxi-processor.json" \
    --region="your-region" \
    --project="your-project" \
    --parameters="mode=batch,input_files=gs://your-bucket/sample_data/*.json,project_id=your-project,dataset_id=your-dataset" \
    --temp-location="gs://your-temp-bucket/dataflow-temp" \
    --staging-location="gs://your-temp-bucket/dataflow-staging" \
    --max-workers=5 \
    --num-workers=2 \
    --worker-machine-type=e2-standard-2
```

#### Batch Job Parameters

| Parameter | Required | Description | Example |
|-----------|----------|-------------|---------|
| `mode` | ✅ | Must be "batch" | `batch` |
| `input_files` | ✅ | GCS path pattern for input files | `gs://bucket/data/*.json` |
| `project_id` | ✅ | GCP Project ID | `your-project-id` |
| `dataset_id` | ✅ | BigQuery dataset ID | `taxi_dataset` |
| `table_name` | ❌ | Target table name (default: taxi_trips) | `taxi_trips` |
| `batch_size` | ❌ | Records per batch (default: 1000) | `1000` |

### 🌊 Streaming Processing Jobs

Streaming jobs continuously process real-time data from Pub/Sub and write to BigQuery with windowed aggregations.

#### Quick Start - Streaming Jobs

```bash
# Using the convenience script (recommended)
./scripts/run_streaming_job.sh
```

#### Manual Streaming Job Execution

```bash
gcloud dataflow flex-template run "streaming-taxi-$(date +%Y%m%d-%H%M%S)" \
    --template-file-gcs-location="gs://your-temp-bucket/flex-templates/streaming-taxi-processor.json" \
    --region="your-region" \
    --project="your-project" \
    --parameters="mode=streaming,subscription_name=projects/your-project/subscriptions/taxi-trips-subscription,project_id=your-project,dataset_id=your-dataset,window_size=60" \
    --temp-location="gs://your-temp-bucket/dataflow-temp" \
    --staging-location="gs://your-temp-bucket/dataflow-staging" \
    --max-workers=3 \
    --num-workers=1 \
    --worker-machine-type=e2-standard-2 \
    --enable-streaming-engine
```

#### Streaming Job Parameters

| Parameter | Required | Description | Example |
|-----------|----------|-------------|---------|
| `mode` | ✅ | Must be "streaming" | `streaming` |
| `subscription_name` | ✅ | Full Pub/Sub subscription path | `projects/proj/subscriptions/taxi-trips-subscription` |
| `project_id` | ✅ | GCP Project ID | `your-project-id` |
| `dataset_id` | ✅ | BigQuery dataset ID | `taxi_dataset` |
| `table_name` | ❌ | Target table name (default: taxi_trips) | `taxi_trips` |
| `window_size` | ❌ | Window size in seconds (default: 60) | `60` |

### 🔧 Local Development & Testing

#### Running Locally with DataflowRunner

For development and testing, you can run jobs locally while still using remote GCS and BigQuery:

```bash
# Activate virtual environment
source .venv/bin/activate

# Install dependencies
pip install -r requirements.txt

# Run batch job locally
python main.py \
    --mode=batch \
    --input_files="gs://your-bucket/sample_data/*.json" \
    --project_id=your-project \
    --dataset_id=your-dataset \
    --runner=DataflowRunner \
    --project=your-project \
    --region=your-region \
    --temp_location="gs://your-temp-bucket/dataflow-temp" \
    --staging_location="gs://your-temp-bucket/dataflow-staging"

# Run streaming job locally (for testing)
python main.py \
    --mode=streaming \
    --subscription_name="projects/your-project/subscriptions/taxi-trips-subscription" \
    --project_id=your-project \
    --dataset_id=your-dataset \
    --runner=DataflowRunner \
    --project=your-project \
    --region=your-region \
    --temp_location="gs://your-temp-bucket/dataflow-temp" \
    --staging_location="gs://your-temp-bucket/dataflow-staging" \
    --streaming
```

### 📊 Job Monitoring & Management

#### Check Job Status

```bash
# List all jobs
gcloud dataflow jobs list --region=your-region

# Describe specific job
gcloud dataflow jobs describe JOB_NAME --region=your-region

# Get job status only
gcloud dataflow jobs describe JOB_NAME --region=your-region --format="value(currentState)"

# Monitor job in real-time
watch 'gcloud dataflow jobs describe JOB_NAME --region=your-region --format="value(currentState)"'
```

#### Cancel Jobs

```bash
# Cancel a specific job
gcloud dataflow jobs cancel JOB_NAME --region=your-region

# Cancel all running jobs (use with caution!)
gcloud dataflow jobs list --region=your-region --filter="state=JOB_STATE_RUNNING" --format="value(id)" | xargs -I {} gcloud dataflow jobs cancel {} --region=your-region
```

#### View Job Logs

```bash
# View job logs
gcloud dataflow jobs describe JOB_NAME --region=your-region --format="value(jobMetadata.sdkVersion)"

# Stream logs in real-time
gcloud logging read "resource.type=dataflow_job AND resource.labels.job_name=JOB_NAME" --follow
```

### ⚠️ Important Tips & Pitfalls

#### 🚨 Critical Requirements

1. **Table Schema Alignment**: Ensure all required tables exist with correct schemas
   - `taxi_trips` (main data table)
   - `hourly_trip_stats` (batch aggregations)
   - `windowed_trip_stats` (streaming aggregations) ⚠️ **Must exist before streaming jobs**

2. **Field Compatibility**: Streaming data includes `event_timestamp` field that gets filtered out
   - The pipeline automatically removes this field to match BigQuery schema
   - Don't add `event_timestamp` to your BigQuery tables

3. **Region Consistency**: All resources must be in the same region
   - Dataflow jobs, GCS buckets, BigQuery dataset
   - Use `--region` flag consistently across all commands

#### 🐛 Common Pitfalls

1. **Missing windowed_trip_stats Table**
   ```
   Error: Table requires a schema. None can be inferred because the table does not exist.
   ```
   **Solution**: Create the table before running streaming jobs
   ```bash
   bq mk --table your-project:your-dataset.windowed_trip_stats stat_hour:TIMESTAMP,pickup_location_id:INTEGER,trip_count:INTEGER,avg_fare_amount:NUMERIC,avg_trip_distance:NUMERIC,avg_trip_duration_minutes:NUMERIC,total_revenue:NUMERIC,window_start:TIMESTAMP,window_end:TIMESTAMP,created_at:TIMESTAMP
   ```

2. **Template Launch Failed: exit status 1**
   - Usually indicates Docker image or base image issues
   - Rebuild Docker image with `--no-cache` flag
   - Ensure using correct base image: `gcr.io/dataflow-templates-base/python3-template-launcher-base:flex_templates_base_image_release_20250616_RC00`

3. **Import Errors in Dataflow Workers**
   ```
   NameError: name 'datetime' is not defined
   ```
   **Solution**: The pipeline includes imports inside DoFn methods for worker compatibility

4. **Permission Errors**
   ```
   User does not have sufficient permissions
   ```
   **Solution**: Ensure service account has required roles and use `--project` flag in gcloud commands

5. **Subscription Not Found**
   ```
   Subscription does not exist
   ```
   **Solution**: Verify Pub/Sub subscription exists and use full path format
   ```bash
   gcloud pubsub subscriptions list
   # Use: projects/your-project/subscriptions/subscription-name
   ```

#### 💡 Performance Tips

1. **Batch Job Optimization**
   - Use appropriate `--max-workers` based on data volume
   - Set `--batch_size` parameter to optimize memory usage
   - Use `--worker-machine-type=e2-standard-4` for large datasets

2. **Streaming Job Optimization**
   - Enable `--enable-streaming-engine` for better performance
   - Adjust `window_size` parameter based on latency requirements
   - Use `--worker-machine-type=e2-standard-2` for streaming workloads

3. **Cost Optimization**
   - Use preemptible workers: `--use-public-ips --enable-ip-alias`
   - Set appropriate `--max-workers` to avoid over-provisioning
   - Monitor job metrics to optimize resource allocation

#### 🔄 Troubleshooting Workflow

1. **Check Prerequisites**
   ```bash
   # Verify tables exist
   bq ls your-dataset
   
   # Check Docker image
   gcloud container images list --repository=your-region-docker.pkg.dev/your-project/dataflow-templates
   
   # Verify templates exist
   gsutil ls gs://your-temp-bucket/flex-templates/
   ```

2. **Test Locally First**
   ```bash
   # Test with DirectRunner for quick validation
   python main.py --mode=batch --input_files="gs://bucket/small-file.json" --runner=DirectRunner
   ```

3. **Check Job Details**
   ```bash
   # Get detailed job information
   gcloud dataflow jobs describe JOB_NAME --region=your-region --format=json
   ```

4. **Review Logs**
   ```bash
   # Check worker logs for specific errors
   gcloud logging read "resource.type=dataflow_job AND resource.labels.job_name=JOB_NAME AND severity>=ERROR" --limit=50
   ```

## 🏗️ Architecture Overview

This solution implements a modern data lakehouse architecture with:

- **BigLake Catalog**: Unified metadata management for Iceberg tables
- **Google Cloud Storage**: Scalable data storage with lifecycle management
- **Dataflow**: Real-time and batch data processing with Apache Beam
- **Pub/Sub**: Event-driven data ingestion
- **BigQuery**: SQL analytics engine with Iceberg integration
- **Cross-Cloud Support**: Google Omni for AWS S3 integration

### Reference Dataset: NYC Taxi Trip Data

The solution uses NYC Taxi Trip Record dataset as a reference implementation, providing:
- Historical data processing (15+ years of records)
- Real-time trip completion events
- Rich schema with geospatial data
- Natural partitioning strategies
- Performance optimization examples

## 🚀 Quick Start

### Prerequisites

- Google Cloud Platform account with billing enabled
- Service account with required permissions
- Terraform >= 1.0
- Python 3.9+
- gcloud CLI installed and configured

### 1. Clone and Setup

```bash
git clone <repository-url>
cd dt_gcp_lakehouse_iceberg

# Install Poetry (if not already installed)
curl -sSL https://install.python-poetry.org | python3 -

# Install Python dependencies
poetry install

# Copy environment configuration
cp env.example .env
```

### 2. Configure Environment

Edit `.env` file with your GCP project details:

```bash
# Required Configuration
PROJECT_ID=your-gcp-project-id
REGION=us-central1
DATASET_ID=taxi_dataset
ICEBERG_BUCKET=your-project-iceberg-data
TEMP_BUCKET=your-project-temp-bucket

# Optional: Cross-cloud integration
ENABLE_CROSS_CLOUD=false
AWS_ACCESS_KEY_ID=your-aws-key
AWS_SECRET_ACCESS_KEY=your-aws-secret
```

### 3. Deploy Infrastructure

```bash
# Make deployment script executable
chmod +x scripts/deploy.sh

# Run deployment
./scripts/deploy.sh
```

The deployment script will:
1. Enable required GCP APIs
2. Deploy Terraform infrastructure (creates all GCP resources automatically)
3. Create BigQuery dataset and Iceberg tables
4. Build and deploy Dataflow templates
5. Set up Cloud Functions and Scheduler
6. Start streaming pipeline
7. Generate sample data

**Note**: Terraform will automatically create all required GCP resources including:
- GCS buckets (Iceberg data, temp, staging)
- BigQuery dataset
- Pub/Sub topics and subscriptions
- Service accounts with proper IAM roles
- BigLake catalog

You only need to manually create AWS resources if enabling cross-cloud integration.

### 4. Verify Deployment

```bash
# Check if tables were created
bq ls taxi_dataset

# Query sample data
bq query --use_legacy_sql=false 'SELECT COUNT(*) FROM `your-project.taxi_dataset.taxi_trips`'

# View Dataflow jobs
gcloud dataflow jobs list --region=us-central1
```

## 📊 Data Operations

### Real-time Data Generation

Generate continuous taxi trip events:

```bash
# Generate batch of trips
python -m src.data_generator.taxi_trip_simulator \
    --project_id=your-project \
    --mode=batch \
    --trip_count=1000

# Continuous stream (1 hour)
python -m src.data_generator.taxi_trip_simulator \
    --project_id=your-project \
    --mode=continuous \
    --trips_per_minute=20 \
    --duration_hours=1

# Historical data generation
python -m src.data_generator.taxi_trip_simulator \
    --project_id=your-project \
    --mode=historical \
    --start_date=2024-01-01 \
    --end_date=2024-01-31 \
    --trips_per_day=5000
```

### Batch Processing

Process historical CSV files:

```bash
python -m src.dataflow.batch_taxi_processor \
    --input_files="gs://your-bucket/taxi-data/*.csv" \
    --project_id=your-project \
    --temp_location="gs://your-temp-bucket/dataflow-temp" \
    --staging_location="gs://your-temp-bucket/dataflow-staging"
```

### Streaming Processing

The streaming pipeline runs continuously, processing real-time events from Pub/Sub.

## 🔧 Maintenance & Optimization

### Automated Maintenance

Run comprehensive maintenance:

```bash
# Full maintenance cycle
python -m src.maintenance.iceberg_maintenance \
    --project_id=your-project \
    --action=full

# Specific operations
python -m src.maintenance.iceberg_maintenance \
    --project_id=your-project \
    --action=compact \
    --table_name=taxi_trips

python -m src.maintenance.iceberg_maintenance \
    --project_id=your-project \
    --action=optimize \
    --table_name=taxi_trips
```

### Scheduled Maintenance

Maintenance runs automatically via Cloud Scheduler:
- **Daily**: Full maintenance at 2 AM UTC
- **Hourly**: Compaction during business hours
- **Weekly**: Deep optimization on Sundays

## 📈 Analytics Examples

### Basic Queries

```sql
-- Trip volume by location
SELECT 
  z.zone_name,
  COUNT(*) as trip_count,
  AVG(total_amount) as avg_fare
FROM `project.taxi_dataset.taxi_trips` t
JOIN `project.taxi_dataset.taxi_zones` z 
  ON t.pickup_location_id = z.location_id
WHERE DATE(pickup_datetime) = CURRENT_DATE()
GROUP BY z.zone_name
ORDER BY trip_count DESC;

-- Hourly patterns
SELECT 
  EXTRACT(HOUR FROM pickup_datetime) as hour,
  COUNT(*) as trips,
  AVG(trip_distance) as avg_distance,
  SUM(total_amount) as total_revenue
FROM `project.taxi_dataset.taxi_trips`
WHERE DATE(pickup_datetime) >= DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY)
GROUP BY hour
ORDER BY hour;
```

### Time Travel Queries

```sql
-- Compare data before and after processing
SELECT COUNT(*) as trip_count
FROM `project.taxi_dataset.taxi_trips`
FOR SYSTEM_TIME AS OF '2024-01-01 12:00:00'
WHERE DATE(pickup_datetime) = '2024-01-01';

-- Audit data changes
SELECT 
  trip_id,
  total_amount,
  _CHANGE_TYPE,
  _CHANGE_TIMESTAMP
FROM `project.taxi_dataset.taxi_trips`
FOR SYSTEM_TIME BETWEEN '2024-01-01' AND '2024-01-02'
WHERE trip_id = 'specific-trip-id'
ORDER BY _CHANGE_TIMESTAMP;
```

### Advanced Analytics

```sql
-- Real-time dashboard query
SELECT 
  DATE_TRUNC(pickup_datetime, HOUR) as hour_bucket,
  pickup_location_id,
  COUNT(*) as trips_count,
  SUM(total_amount) as hourly_revenue,
  AVG(trip_distance) as avg_distance
FROM `project.taxi_dataset.taxi_trips`
WHERE pickup_datetime >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 24 HOUR)
GROUP BY hour_bucket, pickup_location_id
HAVING trips_count > 10
ORDER BY hour_bucket DESC, hourly_revenue DESC;
```

## 🌐 Cross-Cloud Integration

### AWS S3 Integration

Enable cross-cloud capabilities:

```bash
# Set environment variables
export ENABLE_CROSS_CLOUD=true
export AWS_ACCOUNT_ID=123456789012

# Redeploy with cross-cloud support
./scripts/deploy.sh
```

### Federated Queries

```sql
-- Query across GCP and AWS data
WITH gcp_trips AS (
  SELECT 'GCP' as source, COUNT(*) as trip_count
  FROM `project.taxi_dataset.taxi_trips`
  WHERE DATE(pickup_datetime) = CURRENT_DATE()
),
aws_trips AS (
  SELECT 'AWS' as source, COUNT(*) as trip_count
  FROM `project.taxi_dataset.s3_taxi_trips`
  WHERE DATE(pickup_datetime) = CURRENT_DATE()
)
SELECT * FROM gcp_trips
UNION ALL
SELECT * FROM aws_trips;
```

## 🧪 Testing

Run the test suite:

```bash
# Run all tests
poetry run pytest tests/ -v

# Run specific test modules
poetry run pytest tests/test_taxi_simulator.py -v
poetry run pytest tests/test_iceberg_maintenance.py -v

# Run with coverage
poetry run pytest tests/ --cov=src --cov-report=html
```

## 📁 Project Structure

```
dt_gcp_lakehouse_iceberg/
├── src/
│   ├── dataflow/
│   │   ├── batch_taxi_processor.py      # Batch processing pipeline
│   │   └── streaming_taxi_processor.py  # Streaming pipeline
│   ├── data_generator/
│   │   └── taxi_trip_simulator.py       # Data generation
│   └── maintenance/
│       └── iceberg_maintenance.py       # Automated maintenance
├── terraform/
│   ├── main.tf                          # Infrastructure definition
│   ├── variables.tf                     # Terraform variables
│   └── terraform.tfvars.example         # Example configuration
├── sql/
│   └── create_iceberg_tables.sql        # Table creation scripts
├── scripts/
│   └── deploy.sh                        # Deployment automation
├── tests/
│   ├── test_taxi_simulator.py           # Simulator tests
│   └── test_iceberg_maintenance.py      # Maintenance tests
├── pyproject.toml                       # Poetry dependencies and config
├── env.example                          # Environment template
└── README.md                           # This file
```

## 🔒 Security & Permissions

### Required IAM Roles

The service account needs these roles:
- `roles/bigquery.admin`
- `roles/biglake.admin`
- `roles/storage.admin`
- `roles/dataflow.admin`
- `roles/pubsub.admin`
- `roles/cloudfunctions.admin`
- `roles/cloudscheduler.admin`

### Data Security

- Row-level security on sensitive data
- Column-level encryption for PII
- VPC-native networking for Dataflow
- Private Google Access enabled
- Audit logging for all operations

## 📊 Monitoring & Alerting

### Built-in Monitoring

- Dataflow job monitoring
- BigQuery query performance tracking
- Storage usage and costs
- Data quality metrics
- Pipeline health checks

### Custom Alerts

- Failed Dataflow jobs
- High query costs
- Data freshness issues
- Maintenance failures

## 🚀 Performance Optimization

### Partitioning Strategy

- **Date partitioning**: By pickup_datetime
- **Clustering**: By location_id, payment_type
- **Z-ordering**: For analytical workloads

### File Management

- **Compaction**: Automatic file size optimization
- **Snapshot management**: Configurable retention
- **Orphaned file cleanup**: Automated cleanup

### Query Optimization

- **Predicate pushdown**: Automatic filter optimization
- **Materialized views**: Pre-computed aggregations
- **Clustering**: Co-location of related data

## 🔄 CI/CD Integration

### GitHub Actions Example

```yaml
name: Deploy Lakehouse
on:
  push:
    branches: [main]

jobs:
  deploy:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v2
      - name: Setup Terraform
        uses: hashicorp/setup-terraform@v1
      - name: Deploy Infrastructure
        run: ./scripts/deploy.sh
        env:
          PROJECT_ID: ${{ secrets.PROJECT_ID }}
          GOOGLE_APPLICATION_CREDENTIALS: ${{ secrets.GCP_SA_KEY }}
```

## 🐛 Troubleshooting

### Common Issues

1. **Permission Errors**
   ```bash
   # Check service account permissions
   gcloud projects get-iam-policy your-project-id
   ```

2. **Dataflow Job Failures**
   ```bash
   # Check job logs
   gcloud dataflow jobs describe JOB_ID --region=us-central1
   ```

3. **BigQuery Errors**
   ```bash
   # Verify table exists
   bq show taxi_dataset.taxi_trips
   ```

### Debug Mode

Enable debug logging:

```bash
export GOOGLE_CLOUD_LOG_LEVEL=DEBUG
python -m src.data_generator.taxi_trip_simulator --project_id=your-project
```

## 📚 Additional Resources

- [BigLake Documentation](https://cloud.google.com/biglake/docs)
- [Apache Iceberg Specification](https://iceberg.apache.org/spec/)
- [Dataflow Templates](https://cloud.google.com/dataflow/docs/templates/overview)
- [BigQuery Iceberg Tables](https://cloud.google.com/bigquery/docs/iceberg-tables)

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests for new functionality
5. Run the test suite
6. Submit a pull request

## 📄 License

This project is licensed under the MIT License - see the LICENSE file for details.

## 🆘 Support

For issues and questions:
- Create an issue in the repository
- Check the troubleshooting section
- Review GCP documentation
- Contact the development team

---

**Built with ❤️ for modern data lakehouse architectures** 