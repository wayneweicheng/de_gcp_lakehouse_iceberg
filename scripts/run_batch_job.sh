#!/bin/bash

# Run Batch Processing Job using Flex Template
# Usage: ./scripts/run_batch_job.sh [input_files_pattern]

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
required_vars=("PROJECT_ID" "REGION" "DATASET_ID" "TEMP_BUCKET")
for var in "${required_vars[@]}"; do
    if [ -z "${!var}" ]; then
        print_error "Required environment variable $var is not set"
        exit 1
    fi
done

# Set input files pattern (use argument or default)
INPUT_FILES="${1:-gs://$TEMP_BUCKET/data/batch-taxi-processor/input/*}"

# Generate unique job name
JOB_NAME="batch-taxi-$(date +%Y%m%d-%H%M%S)"

print_status "Starting Batch Processing Job: $JOB_NAME"
print_status "Input files: $INPUT_FILES"
print_status "Target dataset: $PROJECT_ID.$DATASET_ID"

# Run the Flex Template job
gcloud dataflow flex-template run "$JOB_NAME" \
    --template-file-gcs-location="gs://$TEMP_BUCKET/flex-templates/batch-taxi-processor.json" \
    --region="$REGION" \
    --parameters="input_files=$INPUT_FILES,project_id=$PROJECT_ID,dataset_id=$DATASET_ID" \
    --temp-location="gs://$TEMP_BUCKET/dataflow-temp" \
    --staging-location="gs://$TEMP_BUCKET/dataflow-staging" \
    --max-workers=5 \
    --num-workers=1 \
    --worker-machine-type=e2-standard-2 \
    --worker-zone=australia-southeast1-a \
    --service-account-email="dataflow-service-account-dev@$PROJECT_ID.iam.gserviceaccount.com"

if [ $? -eq 0 ]; then
    print_status "✅ Batch job started successfully!"
    print_status "Job Name: $JOB_NAME"
    print_status "Monitor at: https://console.cloud.google.com/dataflow/jobs/$REGION/$JOB_NAME?project=$PROJECT_ID"
    print_status ""
    print_status "To check job status:"
    print_status "  gcloud dataflow jobs describe $JOB_NAME --region=$REGION"
else
    print_error "❌ Failed to start batch job"
    exit 1
fi 