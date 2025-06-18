#!/bin/bash

# Run Batch Processing Job with Multi-Zone Fallback
# Usage: ./scripts/run_batch_job_multi_zone.sh [input_files_pattern]

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

# Define zones to try (in order of preference)
ZONES=("australia-southeast1-a" "australia-southeast1-c" "australia-southeast1-b")
MACHINE_TYPES=("e2-standard-2" "e2-standard-4" "n1-standard-2" "n2-standard-2")

# Function to try running job in a specific zone with a specific machine type
try_run_job() {
    local zone=$1
    local machine_type=$2
    
    print_status "Trying zone: $zone with machine type: $machine_type"
    
    gcloud dataflow flex-template run "$JOB_NAME-$zone" \
        --template-file-gcs-location="gs://$TEMP_BUCKET/flex-templates/batch-taxi-processor.json" \
        --region="$REGION" \
        --parameters="input_files=$INPUT_FILES,project_id=$PROJECT_ID,dataset_id=$DATASET_ID" \
        --temp-location="gs://$TEMP_BUCKET/dataflow-temp" \
        --staging-location="gs://$TEMP_BUCKET/dataflow-staging" \
        --max-workers=3 \
        --num-workers=1 \
        --worker-machine-type="$machine_type" \
        --worker-zone="$zone" \
        --service-account-email="dataflow-service-account-dev@$PROJECT_ID.iam.gserviceaccount.com"
}

# Try each zone and machine type combination
for zone in "${ZONES[@]}"; do
    for machine_type in "${MACHINE_TYPES[@]}"; do
        print_status "Attempting to start job in zone: $zone with machine type: $machine_type"
        
        if try_run_job "$zone" "$machine_type" 2>/dev/null; then
            print_status "✅ Job started successfully!"
            print_status "Job Name: $JOB_NAME-$zone"
            print_status "Zone: $zone"
            print_status "Machine Type: $machine_type"
            print_status "Monitor at: https://console.cloud.google.com/dataflow/jobs/$REGION/$JOB_NAME-$zone?project=$PROJECT_ID"
            print_status ""
            print_status "To check job status:"
            print_status "  gcloud dataflow jobs describe $JOB_NAME-$zone --region=$REGION"
            exit 0
        else
            print_warning "Failed to start job in zone $zone with machine type $machine_type"
        fi
    done
done

print_error "❌ Failed to start job in any available zone/machine type combination"
print_error "This might indicate:"
print_error "- Resource exhaustion across all zones in the region"
print_error "- Quota limits reached"
print_error "- Service account permission issues"
print_error ""
print_error "Try again later or consider using a different region."
exit 1 