#!/bin/bash

# Production Batch Processing Job with Multiple Fallback Strategies
# This script handles resource exhaustion issues in GCP regions

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

print_status() { echo -e "${GREEN}[INFO]${NC} $1"; }
print_warning() { echo -e "${YELLOW}[WARNING]${NC} $1"; }
print_error() { echo -e "${RED}[ERROR]${NC} $1"; }
print_debug() { echo -e "${BLUE}[DEBUG]${NC} $1"; }

# Load environment
if [ ! -f ".env" ]; then
    print_error ".env file not found"
    exit 1
fi
source .env

# Validate required variables
for var in PROJECT_ID DATASET_ID TEMP_BUCKET; do
    if [ -z "${!var}" ]; then
        print_error "Required environment variable $var is not set"
        exit 1
    fi
done

INPUT_FILES="${1:-gs://$TEMP_BUCKET/data/batch-taxi-processor/input/*}"
JOB_NAME="batch-taxi-$(date +%Y%m%d-%H%M%S)"

print_status "🚀 Starting Production Batch Job: $JOB_NAME"
print_status "📁 Input files: $INPUT_FILES"
print_status "🎯 Target dataset: $PROJECT_ID.$DATASET_ID"

# Production strategies (ordered by preference)
declare -a STRATEGIES=(
    # Primary: australia-southeast1 with minimal resources
    "australia-southeast1:e2-micro:1:1:gs://$TEMP_BUCKET/flex-templates/batch-taxi-processor.json"
    # Fallback 1: australia-southeast2 
    "australia-southeast2:e2-standard-2:1:2:gs://$TEMP_BUCKET/flex-templates/batch-taxi-processor-au2.json"
    # Fallback 2: asia-southeast1 (Singapore)
    "asia-southeast1:e2-standard-2:1:2:gs://$TEMP_BUCKET/flex-templates/batch-taxi-processor.json"
    # Fallback 3: asia-southeast2 (Jakarta)
    "asia-southeast2:e2-standard-2:1:2:gs://$TEMP_BUCKET/flex-templates/batch-taxi-processor.json"
    # Fallback 4: us-central1 (if all else fails)
    "us-central1:e2-standard-2:1:2:gs://$TEMP_BUCKET/flex-templates/batch-taxi-processor.json"
)

run_job_strategy() {
    local strategy=$1
    IFS=':' read -r region machine_type num_workers max_workers template_path <<< "$strategy"
    
    local job_id="$JOB_NAME-$(echo $region | tr '-' '')"
    
    print_status "🔄 Trying: Region=$region, Machine=$machine_type, Workers=$num_workers-$max_workers"
    
    # Build template for region if needed
    if [[ "$region" != "australia-southeast1" ]] && [[ ! "$template_path" =~ au2 ]]; then
        print_debug "Building template for region $region..."
        local new_template="gs://$TEMP_BUCKET/flex-templates/batch-taxi-processor-${region}.json"
        gcloud dataflow flex-template build "$new_template" \
            --image="australia-southeast1-docker.pkg.dev/$PROJECT_ID/dataflow-templates/lakehouse-iceberg:latest" \
            --sdk-language=PYTHON \
            --metadata-file="templates/batch_taxi_processor_metadata.json" \
            --quiet || return 1
        template_path="$new_template"
    fi
    
    # Run the job
    gcloud dataflow flex-template run "$job_id" \
        --template-file-gcs-location="$template_path" \
        --region="$region" \
        --parameters="input_files=$INPUT_FILES,project_id=$PROJECT_ID,dataset_id=$DATASET_ID" \
        --temp-location="gs://$TEMP_BUCKET/dataflow-temp" \
        --staging-location="gs://$TEMP_BUCKET/dataflow-staging" \
        --max-workers="$max_workers" \
        --num-workers="$num_workers" \
        --worker-machine-type="$machine_type" \
        --service-account-email="dataflow-service-account-dev@$PROJECT_ID.iam.gserviceaccount.com" \
        --quiet
    
    if [ $? -eq 0 ]; then
        print_status "✅ SUCCESS! Job started in region: $region"
        print_status "🔗 Job ID: $job_id"
        print_status "🌐 Monitor: https://console.cloud.google.com/dataflow/jobs/$region/$job_id?project=$PROJECT_ID"
        print_status ""
        print_status "📊 Check status: gcloud dataflow jobs describe $job_id --region=$region"
        return 0
    else
        return 1
    fi
}

# Try each strategy
for strategy in "${STRATEGIES[@]}"; do
    if run_job_strategy "$strategy" 2>/dev/null; then
        exit 0
    else
        region=$(echo "$strategy" | cut -d':' -f1)
        print_warning "❌ Failed in region: $region"
    fi
done

print_error "🚨 CRITICAL: All regions failed!"
print_error ""
print_error "This indicates either:"
print_error "  1. Widespread GCP resource exhaustion"
print_error "  2. Account quota limits reached"
print_error "  3. Service account permission issues"
print_error "  4. Template/code issues"
print_error ""
print_error "🔧 Recommended actions:"
print_error "  1. Wait 30-60 minutes and retry"
print_error "  2. Check quotas: gcloud compute project-info describe"
print_error "  3. Try different machine types"
print_error "  4. Contact GCP support if persistent"

exit 1 