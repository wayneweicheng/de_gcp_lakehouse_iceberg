#!/bin/bash

# Run Batch Processing Job in US Central Region
# This script runs the batch taxi processing job using Flex Templates in us-central1

set -e

# Load environment variables
source .env

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

print_status() {
    echo -e "${GREEN}[INFO]${NC} $1"
}

print_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

print_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# Configuration
REGION="us-central1"
JOB_NAME="batch-taxi-$(date +%Y%m%d-%H%M%S)"
TEMPLATE_PATH="gs://${TEMP_BUCKET}/dataflow-templates/batch-taxi-processor"
INPUT_PATH="gs://${TEMP_BUCKET}/data/batch-taxi-processor/input/*"
DATASET="${PROJECT_ID}.${DATASET_ID}"

print_status "Starting Batch Processing Job: $JOB_NAME"
print_status "Input files: $INPUT_PATH"
print_status "Target dataset: $DATASET"
print_status "Region: $REGION"

# Build template for us-central1 if it doesn't exist
US_TEMPLATE_PATH="gs://${TEMP_BUCKET}/dataflow-templates/us-central1/batch-taxi-processor"

print_status "Building template for us-central1..."
gcloud dataflow flex-template build "$US_TEMPLATE_PATH" \
    --image="australia-southeast1-docker.pkg.dev/${PROJECT_ID}/dataflow-templates/lakehouse-iceberg:latest" \
    --sdk-language=PYTHON \
    --metadata-file="templates/batch_taxi_processor_metadata.json"

# Run the job with multiple fallback strategies
strategies=(
    "e2-standard-2:2:5:us-central1-a"
    "e2-standard-2:2:5:us-central1-b" 
    "e2-standard-2:2:5:us-central1-c"
    "e2-standard-2:2:5:us-central1-f"
    "e2-medium:1:3:us-central1-a"
    "n1-standard-1:1:3:us-central1-a"
    "e2-micro:1:2:no-zone"
)

for strategy in "${strategies[@]}"; do
    IFS=':' read -r machine_type min_workers max_workers zone <<< "$strategy"
    
    strategy_name="${machine_type}-${min_workers}-${max_workers}-${zone}"
    current_job_name="${JOB_NAME}-${strategy_name}"
    
    print_status "Attempting strategy: $strategy"
    print_status "Trying strategy: $machine_type, workers: $min_workers-$max_workers, zone: $zone"
    
    # Build parameters
    PARAMETERS="project_id=${PROJECT_ID}"
    PARAMETERS="${PARAMETERS},input_files=${INPUT_PATH}"
    PARAMETERS="${PARAMETERS},dataset_id=${DATASET_ID}"
    PARAMETERS="${PARAMETERS},temp_location=gs://${TEMP_BUCKET}/dataflow-temp"
    PARAMETERS="${PARAMETERS},staging_location=gs://${TEMP_BUCKET}/dataflow-staging"
    
    # Build gcloud command
    cmd="gcloud dataflow flex-template run \"$current_job_name\""
    cmd="$cmd --template-file-gcs-location=\"$US_TEMPLATE_PATH\""
    cmd="$cmd --region=\"$REGION\""
    cmd="$cmd --parameters=\"$PARAMETERS\""
    cmd="$cmd --service-account-email=\"dataflow-service-account-dev@${PROJECT_ID}.iam.gserviceaccount.com\""
    cmd="$cmd --max-workers=$max_workers"
    cmd="$cmd --num-workers=$min_workers"
    cmd="$cmd --worker-machine-type=$machine_type"
    cmd="$cmd --disable-public-ips"
    cmd="$cmd --enable-streaming-engine"
    
    # Add zone if specified
    if [ "$zone" != "no-zone" ]; then
        cmd="$cmd --worker-zone=$zone"
    fi
    
    # Execute command
    if eval "$cmd"; then
        print_status "✅ Job started successfully with strategy: $strategy"
        print_status "Job Name: $current_job_name"
        print_status "Monitor at: https://console.cloud.google.com/dataflow/jobs"
        exit 0
    else
        print_warning "❌ Strategy failed: $strategy"
        print_warning "Trying next strategy..."
        sleep 5
    fi
done

print_error "❌ All strategies failed!"
exit 1 