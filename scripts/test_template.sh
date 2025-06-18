#!/bin/bash

# Test Flex Template Setup
# This script validates our Docker image and template configuration

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

# Load environment variables
source .env

DOCKER_IMAGE="australia-southeast1-docker.pkg.dev/$PROJECT_ID/dataflow-templates/lakehouse-iceberg:latest"

print_status "Testing Flex Template Setup..."

# Test 1: Check if Docker image exists and is accessible
print_status "1. Testing Docker image accessibility..."
if docker pull "$DOCKER_IMAGE" > /dev/null 2>&1; then
    print_status "✅ Docker image is accessible"
else
    print_error "❌ Cannot pull Docker image"
    exit 1
fi

# Test 2: Test Python imports in the container
print_status "2. Testing Python dependencies..."
if docker run --rm "$DOCKER_IMAGE" python -c "import apache_beam, google.cloud.bigquery; print('Dependencies OK')" > /dev/null 2>&1; then
    print_status "✅ Python dependencies are working"
else
    print_error "❌ Python dependencies failed"
    exit 1
fi

# Test 3: Test our entry point
print_status "3. Testing batch processor entry point..."
if docker run --rm "$DOCKER_IMAGE" python -m src.dataflow.batch_taxi_processor --help > /dev/null 2>&1; then
    print_status "✅ Batch processor entry point is working"
else
    print_error "❌ Batch processor entry point failed"
    exit 1
fi

# Test 4: Check if template files exist
print_status "4. Checking template files..."
if gsutil ls "gs://$TEMP_BUCKET/flex-templates/batch-taxi-processor.json" > /dev/null 2>&1; then
    print_status "✅ Batch template file exists"
else
    print_error "❌ Batch template file not found"
    exit 1
fi

# Test 5: Check if input data exists
print_status "5. Checking input data..."
if gsutil ls "gs://$TEMP_BUCKET/data/batch-taxi-processor/input/*.json" > /dev/null 2>&1; then
    print_status "✅ Input data files exist"
else
    print_error "❌ Input data files not found"
    exit 1
fi

# Test 6: Check BigQuery table exists
print_status "6. Checking BigQuery table..."
if bq show "$PROJECT_ID:$DATASET_ID.taxi_trips" > /dev/null 2>&1; then
    print_status "✅ BigQuery table exists"
else
    print_error "❌ BigQuery table not found"
    exit 1
fi

print_status "🎉 All tests passed! The setup looks good."
print_status ""
print_status "If jobs are still failing, the issue might be:"
print_status "- Network connectivity in the Dataflow workers"
print_status "- Service account permissions"
print_status "- Regional resource availability"
print_status ""
print_status "Try running a job with verbose logging:"
print_status "gcloud dataflow flex-template run test-job --template-file-gcs-location=gs://$TEMP_BUCKET/flex-templates/batch-taxi-processor.json --region=$REGION --parameters=input_files=gs://$TEMP_BUCKET/data/batch-taxi-processor/input/*,project_id=$PROJECT_ID,dataset_id=$DATASET_ID --verbosity=debug" 