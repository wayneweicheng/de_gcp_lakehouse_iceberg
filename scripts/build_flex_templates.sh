#!/bin/bash

# Build Flex Templates for Dataflow
# This script creates Flex Templates using our custom Docker image

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
required_vars=("PROJECT_ID" "REGION" "TEMP_BUCKET")
for var in "${required_vars[@]}"; do
    if [ -z "${!var}" ]; then
        print_error "Required environment variable $var is not set"
        exit 1
    fi
done

# Set Docker image URL
DOCKER_IMAGE="${REGION}-docker.pkg.dev/$PROJECT_ID/dataflow-templates/lakehouse-iceberg:latest"

print_status "Building Flex Templates with Docker image: $DOCKER_IMAGE"

# Create templates directory in GCS if it doesn't exist
print_status "Ensuring templates directory exists in GCS..."
gsutil -q stat "gs://$TEMP_BUCKET/flex-templates/" 2>/dev/null || echo "Templates directory will be created automatically"

# Build Batch Processing Flex Template
print_status "Building Batch Processing Flex Template..."

gcloud dataflow flex-template build "gs://$TEMP_BUCKET/flex-templates/batch-taxi-processor.json" \
    --image="$DOCKER_IMAGE" \
    --sdk-language=PYTHON \
    --metadata-file="templates/batch_taxi_processor_metadata.json"

if [ $? -eq 0 ]; then
    print_status "✅ Batch Processing Flex Template created successfully!"
    print_status "   Location: gs://$TEMP_BUCKET/flex-templates/batch-taxi-processor.json"
else
    print_error "❌ Failed to create Batch Processing Flex Template"
    exit 1
fi

# Build Streaming Processing Flex Template
print_status "Building Streaming Processing Flex Template..."

gcloud dataflow flex-template build "gs://$TEMP_BUCKET/flex-templates/streaming-taxi-processor.json" \
    --image="$DOCKER_IMAGE" \
    --sdk-language=PYTHON \
    --metadata-file="templates/streaming_taxi_processor_metadata.json"

if [ $? -eq 0 ]; then
    print_status "✅ Streaming Processing Flex Template created successfully!"
    print_status "   Location: gs://$TEMP_BUCKET/flex-templates/streaming-taxi-processor.json"
else
    print_error "❌ Failed to create Streaming Processing Flex Template"
    exit 1
fi

print_status "🎉 All Flex Templates built successfully!"
print_status ""
print_status "You can now run jobs using:"
print_status "  Batch: gcloud dataflow flex-template run JOB_NAME --template-file-gcs-location=gs://$TEMP_BUCKET/flex-templates/batch-taxi-processor.json --region=$REGION --parameters=..."
print_status "  Streaming: gcloud dataflow flex-template run JOB_NAME --template-file-gcs-location=gs://$TEMP_BUCKET/flex-templates/streaming-taxi-processor.json --region=$REGION --parameters=..." 