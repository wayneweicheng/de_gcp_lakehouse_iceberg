#!/bin/bash

# Run Batch Processing Job with Flexible Resource Strategy
# Usage: ./scripts/run_batch_job_flexible.sh [input_files_pattern]

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

# Define different strategies to try
declare -a STRATEGIES=(
    # Strategy 1: Small machine, let Dataflow choose zone
    "e2-micro:1:1:no-zone"
    # Strategy 2: Standard machine, let Dataflow choose zone  
    "e2-standard-2:1:2:no-zone"
    # Strategy 3: Different region entirely
    "e2-standard-2:1:2:australia-southeast2"
    # Strategy 4: Preemptible instances
    "e2-standard-2:1:2:preemptible"
    # Strategy 5: Different machine family
    "n2-standard-2:1:2:no-zone"
)

# Function to try running job with a specific strategy
try_run_job() {
    local strategy=$1
    IFS=':' read -r machine_type num_workers max_workers special <<< "$strategy"
    
    print_status "Trying strategy: $machine_type, workers: $num_workers-$max_workers, special: $special"
    
    local cmd="gcloud dataflow flex-template run \"$JOB_NAME-$(echo $strategy | tr ':' '-')\" \
        --template-file-gcs-location=\"gs://$TEMP_BUCKET/flex-templates/batch-taxi-processor.json\" \
        --parameters=\"input_files=$INPUT_FILES,project_id=$PROJECT_ID,dataset_id=$DATASET_ID\" \
        --temp-location=\"gs://$TEMP_BUCKET/dataflow-temp\" \
        --staging-location=\"gs://$TEMP_BUCKET/dataflow-staging\" \
        --max-workers=$max_workers \
        --num-workers=$num_workers \
        --worker-machine-type=\"$machine_type\" \
        --service-account-email=\"dataflow-service-account-dev@$PROJECT_ID.iam.gserviceaccount.com\""
    
    # Add special configurations
    case $special in
        "australia-southeast2")
            cmd="$cmd --region=\"australia-southeast2\""
            ;;
        "preemptible")
            cmd="$cmd --region=\"$REGION\" --use-preemptible-workers"
            ;;
        *)
            cmd="$cmd --region=\"$REGION\""
            ;;
    esac
    
    eval $cmd
}

# Try each strategy
for strategy in "${STRATEGIES[@]}"; do
    print_status "Attempting strategy: $strategy"
    
    if try_run_job "$strategy" 2>/dev/null; then
        print_status "✅ Job started successfully with strategy: $strategy"
        print_status "Job Name: $JOB_NAME-$(echo $strategy | tr ':' '-')"
        print_status "Monitor at: https://console.cloud.google.com/dataflow/jobs"
        exit 0
    else
        print_warning "Failed with strategy: $strategy"
    fi
done

print_error "❌ Failed to start job with any strategy"
print_error "This might indicate widespread resource exhaustion or other issues."
exit 1 