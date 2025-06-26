#!/bin/bash

# Run Batch Processing Job using Flex Template with Machine Type and Region Flexibility
# Usage: ./scripts/run_batch_job.sh [input_files_pattern] [machine_type] [preferred_region]

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

# Function to try launching job with a specific machine type and region
try_launch_job() {
    local machine_type=$1
    local region=$2
    
    print_status "Attempting to launch job with machine type: $machine_type in region: $region"
    
    # Run the Flex Template job
    if gcloud dataflow flex-template run "$JOB_NAME" \
        --template-file-gcs-location="gs://$TEMP_BUCKET/flex-templates/batch-taxi-processor.json" \
        --region="$region" \
        --project="$PROJECT_ID" \
        --parameters="mode=batch,input_files=$INPUT_FILES,project_id=$PROJECT_ID,dataset_id=$DATASET_ID,project=ark-of-data-2000" \
        --temp-location="gs://$TEMP_BUCKET/dataflow-temp" \
        --staging-location="gs://$TEMP_BUCKET/dataflow-staging" \
        --max-workers=3 \
        --num-workers=1 \
        --worker-machine-type="$machine_type" \
        --service-account-email="dataflow-service-account-dev@$PROJECT_ID.iam.gserviceaccount.com" \
        --quiet 2>/dev/null; then
        
        print_status "✅ Job launched successfully with $machine_type in $region!"
        print_status "Job Name: $JOB_NAME"
        print_status "Monitor at: https://console.cloud.google.com/dataflow/jobs/$region/$JOB_NAME?project=$PROJECT_ID"
        return 0
    else
        print_warning "❌ Failed to launch with $machine_type in $region"
        return 1
    fi
}

# Check if .env file exists
if [ ! -f ".env" ]; then
    print_error ".env file not found. Please copy env.example to .env and configure your settings."
    exit 1
fi

# Load environment variables
source .env

# Validate required environment variables
required_vars=("PROJECT_ID" "DATASET_ID" "TEMP_BUCKET")
for var in "${required_vars[@]}"; do
    if [ -z "${!var}" ]; then
        print_error "Required environment variable $var is not set"
        exit 1
    fi
done

# Set input files pattern (use argument or default)
INPUT_FILES="${1:-gs://$TEMP_BUCKET/data/batch-taxi-processor/input/*}"

# Set preferred machine type (use argument or start with smallest viable)
PREFERRED_MACHINE_TYPE="${2:-e2-small}"

# Set preferred region (use argument, env var, or default to southeast2)
PREFERRED_REGION="${3:-${REGION:-australia-southeast2}}"

# Generate unique job name
JOB_NAME="batch-taxi-$(date +%Y%m%d-%H%M%S)"

# Define regions in order of preference (Australia SE2 first since SE1 has resource issues)
REGIONS=("$PREFERRED_REGION")
if [[ "$PREFERRED_REGION" != "australia-southeast2" ]]; then
    REGIONS+=("australia-southeast2")
fi
if [[ "$PREFERRED_REGION" != "australia-southeast1" ]]; then
    REGIONS+=("australia-southeast1")
fi
# Add US regions as fallback
REGIONS+=("us-central1" "us-west1")

# Define machine types in order of preference (smallest viable to largest)
# e2-micro is too small for Dataflow (insufficient memory)
MACHINE_TYPES=("$PREFERRED_MACHINE_TYPE")

# Add fallback machine types if not already specified
if [[ "$PREFERRED_MACHINE_TYPE" != "e2-small" ]]; then
    MACHINE_TYPES+=("e2-small")
fi
if [[ "$PREFERRED_MACHINE_TYPE" != "e2-medium" ]]; then
    MACHINE_TYPES+=("e2-medium")
fi
if [[ "$PREFERRED_MACHINE_TYPE" != "e2-standard-2" ]]; then
    MACHINE_TYPES+=("e2-standard-2")
fi
if [[ "$PREFERRED_MACHINE_TYPE" != "e2-standard-4" ]]; then
    MACHINE_TYPES+=("e2-standard-4")
fi

print_status "Starting Batch Processing Job: $JOB_NAME"
print_status "Input files: $INPUT_FILES"
print_status "Target dataset: $PROJECT_ID.$DATASET_ID"
print_status "Preferred region: $PREFERRED_REGION"
print_status "Regions to try: ${REGIONS[*]}"
print_status "Zone selection: Auto (Dataflow will distribute across available zones)"
print_status "Machine types to try: ${MACHINE_TYPES[*]}"

# Try each region and machine type combination until one succeeds
SUCCESS=false
for region in "${REGIONS[@]}"; do
    print_status "🔄 Trying region: $region"
    
    for machine_type in "${MACHINE_TYPES[@]}"; do
        if try_launch_job "$machine_type" "$region"; then
            SUCCESS=true
            FINAL_MACHINE_TYPE="$machine_type"
            FINAL_REGION="$region"
            break 2  # Break out of both loops
        fi
        
        # Wait a bit before trying next machine type
        sleep 3
    done
    
    print_warning "All machine types exhausted in $region, trying next region..."
    sleep 5
done

if [ "$SUCCESS" = true ]; then
    print_status "🎉 Batch job started successfully!"
    print_status "Final region: $FINAL_REGION"
    print_status "Final machine type: $FINAL_MACHINE_TYPE"
    print_status ""
    print_status "To check job status:"
    print_status "  gcloud dataflow jobs describe $JOB_NAME --region=$FINAL_REGION"
    print_status ""
    print_status "💡 Tip: For future jobs, you can specify these working parameters:"
    print_status "  ./scripts/run_batch_job.sh \"$INPUT_FILES\" $FINAL_MACHINE_TYPE $FINAL_REGION"
    exit 0
else
    print_error "❌ Failed to start batch job with any combination"
    print_error "All regions and machine types exhausted. Please:"
    print_error "  1. Try again later when resources become available"
    print_error "  2. Check resource quotas in all regions"
    print_error "  3. Verify service account permissions"
    print_error "  4. Check Flex template configuration"
    exit 1
fi 