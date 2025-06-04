#!/bin/bash

# GCP Lakehouse Iceberg Deployment Script
# This script deploys the complete GCP Lakehouse solution with BigLake Iceberg tables

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Function to print colored output
print_status() {
    echo -e "${GREEN}[INFO]${NC} $1"
}

print_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

print_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# Check if .env file exists
if [ ! -f ".env" ]; then
    print_error ".env file not found. Please copy env.example to .env and configure your settings."
    exit 1
fi

# Load environment variables
source .env

# Validate required environment variables
required_vars=("PROJECT_ID" "REGION" "DATASET_ID" "ICEBERG_BUCKET" "TEMP_BUCKET")
for var in "${required_vars[@]}"; do
    if [ -z "${!var}" ]; then
        print_error "Required environment variable $var is not set"
        exit 1
    fi
done

print_status "Starting GCP Lakehouse deployment for project: $PROJECT_ID"

# Step 1: Authenticate with GCP
print_status "Authenticating with GCP..."
if [ -n "$GOOGLE_APPLICATION_CREDENTIALS" ]; then
    gcloud auth activate-service-account --key-file="$GOOGLE_APPLICATION_CREDENTIALS"
else
    print_warning "GOOGLE_APPLICATION_CREDENTIALS not set. Using default authentication."
fi

gcloud config set project "$PROJECT_ID"

# Step 2: Enable required APIs
print_status "Enabling required GCP APIs..."
gcloud services enable \
    bigquery.googleapis.com \
    biglake.googleapis.com \
    storage.googleapis.com \
    pubsub.googleapis.com \
    dataflow.googleapis.com \
    cloudfunctions.googleapis.com \
    cloudscheduler.googleapis.com \
    monitoring.googleapis.com \
    logging.googleapis.com

# Step 3: Deploy Terraform infrastructure
print_status "Deploying Terraform infrastructure..."
cd terraform

# Initialize Terraform
terraform init

# Create terraform.tfvars from environment variables
cat > terraform.tfvars << EOF
project_id = "$PROJECT_ID"
region = "$REGION"
dataset_id = "$DATASET_ID"
iceberg_bucket_name = "$ICEBERG_BUCKET"
temp_bucket_name = "$TEMP_BUCKET"
pubsub_topic_name = "$PUBSUB_TOPIC"
dataflow_max_workers = $DATAFLOW_MAX_WORKERS
dataflow_machine_type = "$DATAFLOW_MACHINE_TYPE"
enable_cross_cloud = $ENABLE_CROSS_CLOUD
snapshot_retention_days = $SNAPSHOT_RETENTION_DAYS
enable_monitoring = $ENABLE_MONITORING
alert_email = "$ALERT_EMAIL"
EOF

# Plan and apply Terraform
terraform plan
terraform apply -auto-approve

cd ..

# Step 4: Create BigQuery dataset and Iceberg tables
print_status "Creating BigQuery dataset and Iceberg tables..."

# Replace variables in SQL script
sed -e "s/\${PROJECT_ID}/$PROJECT_ID/g" \
    -e "s/\${DATASET_ID}/$DATASET_ID/g" \
    -e "s/\${ICEBERG_BUCKET}/$ICEBERG_BUCKET/g" \
    sql/create_iceberg_tables.sql > /tmp/create_tables.sql

# Execute SQL script
bq query --use_legacy_sql=false < /tmp/create_tables.sql

# Step 5: Build and deploy Dataflow templates
print_status "Building Dataflow templates..."

# Create staging directory for Dataflow templates
gsutil mb -p "$PROJECT_ID" "gs://$TEMP_BUCKET/dataflow-templates" || true

# Build batch processing template
poetry run python -m src.dataflow.batch_taxi_processor \
    --runner=DataflowRunner \
    --project="$PROJECT_ID" \
    --region="$REGION" \
    --temp_location="gs://$TEMP_BUCKET/dataflow-temp" \
    --staging_location="gs://$TEMP_BUCKET/dataflow-staging" \
    --template_location="gs://$TEMP_BUCKET/dataflow-templates/batch-taxi-processor" \
    --save_main_session

# Build streaming processing template
poetry run python -m src.dataflow.streaming_taxi_processor \
    --runner=DataflowRunner \
    --project="$PROJECT_ID" \
    --region="$REGION" \
    --temp_location="gs://$TEMP_BUCKET/dataflow-temp" \
    --staging_location="gs://$TEMP_BUCKET/dataflow-staging" \
    --template_location="gs://$TEMP_BUCKET/dataflow-templates/streaming-taxi-processor" \
    --save_main_session

# Step 6: Deploy Cloud Functions
print_status "Deploying Cloud Functions..."

# Create Cloud Function for data generation
gcloud functions deploy taxi-data-generator \
    --runtime=python39 \
    --trigger-topic="$PUBSUB_TOPIC" \
    --source=src/data_generator \
    --entry-point=generate_taxi_data \
    --memory=256MB \
    --timeout=540s \
    --set-env-vars="PROJECT_ID=$PROJECT_ID,TOPIC_NAME=$PUBSUB_TOPIC"

# Step 7: Set up Cloud Scheduler jobs
print_status "Setting up Cloud Scheduler jobs..."

# Create job for data generation
gcloud scheduler jobs create pubsub taxi-data-generation \
    --schedule="*/5 * * * *" \
    --topic="$PUBSUB_TOPIC" \
    --message-body='{"action": "generate_batch", "count": 10}' \
    --description="Generate taxi trip data every 5 minutes"

# Create job for maintenance
gcloud scheduler jobs create http taxi-maintenance \
    --schedule="0 2 * * *" \
    --uri="https://$REGION-$PROJECT_ID.cloudfunctions.net/taxi-maintenance" \
    --http-method=POST \
    --description="Daily maintenance at 2 AM"

# Step 8: Start streaming Dataflow job
print_status "Starting streaming Dataflow job..."

gcloud dataflow flex-template run "taxi-streaming-$(date +%Y%m%d-%H%M%S)" \
    --template-file-gcs-location="gs://$TEMP_BUCKET/dataflow-templates/streaming-taxi-processor" \
    --region="$REGION" \
    --parameters="project_id=$PROJECT_ID,subscription_name=$PUBSUB_SUBSCRIPTION,dataset_id=$DATASET_ID,temp_location=gs://$TEMP_BUCKET/dataflow-temp,staging_location=gs://$TEMP_BUCKET/dataflow-staging"

# Step 9: Verify deployment
print_status "Verifying deployment..."

# Check if BigQuery dataset exists
if bq ls -d "$PROJECT_ID:$DATASET_ID" > /dev/null 2>&1; then
    print_status "✓ BigQuery dataset created successfully"
else
    print_error "✗ BigQuery dataset creation failed"
fi

# Check if GCS buckets exist
if gsutil ls "gs://$ICEBERG_BUCKET" > /dev/null 2>&1; then
    print_status "✓ Iceberg bucket created successfully"
else
    print_error "✗ Iceberg bucket creation failed"
fi

# Check if Pub/Sub topic exists
if gcloud pubsub topics describe "$PUBSUB_TOPIC" > /dev/null 2>&1; then
    print_status "✓ Pub/Sub topic created successfully"
else
    print_error "✗ Pub/Sub topic creation failed"
fi

# Step 10: Generate sample data
print_status "Generating sample taxi trip data..."
poetry run python -m src.data_generator.taxi_trip_simulator \
    --project_id="$PROJECT_ID" \
    --topic_name="$PUBSUB_TOPIC" \
    --mode=batch \
    --trip_count=100

print_status "Deployment completed successfully!"
print_status "You can now:"
print_status "1. Monitor your Dataflow jobs in the GCP Console"
print_status "2. Query your Iceberg tables in BigQuery"
print_status "3. View real-time data in the taxi_trips table"
print_status "4. Check maintenance logs and performance metrics"

# Display useful commands
echo ""
print_status "Useful commands:"
echo "  # Query taxi trips:"
echo "  bq query --use_legacy_sql=false 'SELECT COUNT(*) FROM \`$PROJECT_ID.$DATASET_ID.taxi_trips\`'"
echo ""
echo "  # Run maintenance:"
echo "  poetry run python -m src.maintenance.iceberg_maintenance --project_id=$PROJECT_ID --action=full"
echo ""
echo "  # Generate more data:"
echo "  poetry run python -m src.data_generator.taxi_trip_simulator --project_id=$PROJECT_ID --mode=continuous --duration_hours=1" 