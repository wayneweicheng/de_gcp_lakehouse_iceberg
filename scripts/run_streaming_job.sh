#!/bin/bash

# Run Streaming Processing Job using Flex Template
# Usage: ./scripts/run_streaming_job.sh

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
required_vars=("PROJECT_ID" "REGION" "DATASET_ID" "TEMP_BUCKET" "PUBSUB_SUBSCRIPTION")
for var in "${required_vars[@]}"; do
    if [ -z "${!var}" ]; then
        print_error "Required environment variable $var is not set"
        exit 1
    fi
done

# Generate unique job name
JOB_NAME="streaming-taxi-$(date +%Y%m%d-%H%M%S)"

# Full subscription path
SUBSCRIPTION_PATH="projects/$PROJECT_ID/subscriptions/$PUBSUB_SUBSCRIPTION"

print_status "Starting Streaming Processing Job: $JOB_NAME"
print_status "Subscription: $SUBSCRIPTION_PATH"
print_status "Target dataset: $PROJECT_ID.$DATASET_ID"

# Run the Flex Template job
gcloud dataflow flex-template run "$JOB_NAME" \
    --template-file-gcs-location="gs://$TEMP_BUCKET/flex-templates/streaming-taxi-processor.json" \
    --region="$REGION" \
    --project="$PROJECT_ID" \
    --parameters="mode=streaming,subscription_name=$SUBSCRIPTION_PATH,project_id=$PROJECT_ID,dataset_id=$DATASET_ID,window_size=60" \
    --temp-location="gs://$TEMP_BUCKET/dataflow-temp" \
    --staging-location="gs://$TEMP_BUCKET/dataflow-staging" \
    --max-workers=3 \
    --num-workers=1 \
    --worker-machine-type=e2-standard-2 \
    --worker-zone=australia-southeast1-a \
    --enable-streaming-engine

if [ $? -eq 0 ]; then
    print_status "✅ Streaming job started successfully!"
    print_status "Job Name: $JOB_NAME"
    print_status "Monitor at: https://console.cloud.google.com/dataflow/jobs/$REGION/$JOB_NAME?project=$PROJECT_ID"
    print_status ""
    print_status "To check job status:"
    print_status "  gcloud dataflow jobs describe $JOB_NAME --region=$REGION"
    print_status ""
    print_status "To stop the streaming job:"
    print_status "  gcloud dataflow jobs cancel $JOB_NAME --region=$REGION"
else
    print_error "❌ Failed to start streaming job"
    exit 1
fi 