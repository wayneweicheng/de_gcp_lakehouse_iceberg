#!/bin/bash

# Run Enhanced Streaming Processing Job with Sliding Windows
# Usage: ./scripts/run_streaming_job.sh [sliding_window_size] [sliding_window_period]
# Default: 30-minute sliding windows with 30-second refresh
#
# Examples:
#   ./scripts/run_streaming_job.sh                    # 30-min windows, 30-sec refresh (default)
#   ./scripts/run_streaming_job.sh 900 15             # 15-min windows, 15-sec refresh (faster)
#   ./scripts/run_streaming_job.sh 3600 60            # 1-hour windows, 1-min refresh (longer analysis)
#   ./scripts/run_streaming_job.sh 600 10             # 10-min windows, 10-sec refresh (testing)
#
# Sliding Window Recommendations:
#   - For real-time alerting: 300 seconds (5 min) with 10-second refresh
#   - For demand forecasting: 1800 seconds (30 min) with 30-second refresh
#   - For trend analysis: 3600 seconds (1 hour) with 60-second refresh
#   - For testing/development: 300 seconds (5 min) with 10-second refresh

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

# Parse command line arguments for sliding window configuration
SLIDING_WINDOW_SIZE=${1:-1800}  # Default: 30 minutes (1800 seconds)
SLIDING_WINDOW_PERIOD=${2:-30}  # Default: 30 seconds refresh

# Validate window parameters
if [ "$SLIDING_WINDOW_SIZE" -lt 60 ]; then
    print_error "Sliding window size must be at least 60 seconds"
    exit 1
fi

if [ "$SLIDING_WINDOW_PERIOD" -lt 10 ]; then
    print_error "Sliding window period must be at least 10 seconds"
    exit 1
fi

if [ "$SLIDING_WINDOW_PERIOD" -gt "$SLIDING_WINDOW_SIZE" ]; then
    print_error "Sliding window period cannot be larger than window size"
    exit 1
fi

# Calculate window description for user
WINDOW_SIZE_MIN=$((SLIDING_WINDOW_SIZE / 60))
WINDOW_PERIOD_SEC=$SLIDING_WINDOW_PERIOD

# Generate unique job name
JOB_NAME="enhanced-streaming-taxi-$(date +%Y%m%d-%H%M%S)"

# Full subscription path
SUBSCRIPTION_PATH="projects/$PROJECT_ID/subscriptions/$PUBSUB_SUBSCRIPTION"

print_status "Starting Enhanced Streaming Processing Job: $JOB_NAME"
print_status "Subscription: $SUBSCRIPTION_PATH"
print_status "Target dataset: $PROJECT_ID.$DATASET_ID"
print_status "Sliding window: ${WINDOW_SIZE_MIN} minutes with ${WINDOW_PERIOD_SEC}-second refresh"
print_status "Features: Location-based demand patterns, real-time analytics"

# Run the Flex Template job with sliding window parameters
gcloud dataflow flex-template run "$JOB_NAME" \
    --template-file-gcs-location="gs://$TEMP_BUCKET/flex-templates/streaming-taxi-processor.json" \
    --region="$REGION" \
    --project="$PROJECT_ID" \
    --parameters="mode=streaming,subscription_name=$SUBSCRIPTION_PATH,project_id=$PROJECT_ID,dataset_id=$DATASET_ID,sliding_window_size=$SLIDING_WINDOW_SIZE,sliding_window_period=$SLIDING_WINDOW_PERIOD" \
    --temp-location="gs://$TEMP_BUCKET/dataflow-temp" \
    --staging-location="gs://$TEMP_BUCKET/dataflow-staging" \
    --max-workers=5 \
    --num-workers=2 \
    --worker-machine-type=e2-standard-4 \
    --worker-zone=australia-southeast1-a \
    --enable-streaming-engine \
    --additional-experiments=enable_google_cloud_profiler

if [ $? -eq 0 ]; then
    print_status "✅ Enhanced streaming job started successfully!"
    print_status "Job Name: $JOB_NAME"
    print_status "Monitor at: https://console.cloud.google.com/dataflow/jobs/$REGION/$JOB_NAME?project=$PROJECT_ID"
    print_status ""
    print_status "📊 BigQuery Tables Created:"
    print_status "  • $PROJECT_ID.$DATASET_ID.taxi_trips (raw trip data)"
    print_status "  • $PROJECT_ID.$DATASET_ID.location_features_sliding_window (location demand patterns)"
    print_status "  • $PROJECT_ID.$DATASET_ID.hourly_trip_stats (hourly aggregations)"
    print_status ""
    print_status "🔍 Location Features Include:"
    print_status "  • Demand intensity per location zone"
    print_status "  • Revenue patterns and pricing analytics"
    print_status "  • Destination diversity metrics"
    print_status "  • Rush hour and weekend patterns"
    print_status "  • Real-time anomaly detection flags"
    print_status ""
    print_status "⚙️ Management Commands:"
    print_status "  Check status:  gcloud dataflow jobs describe $JOB_NAME --region=$REGION"
    print_status "  Stop job:      gcloud dataflow jobs cancel $JOB_NAME --region=$REGION"
    print_status "  View logs:     gcloud dataflow jobs describe $JOB_NAME --region=$REGION --format='value(currentState)'"
    print_status ""
    print_warning "💰 Cost Alert: This streaming job will run continuously until stopped."
    print_warning "Remember to cancel the job when testing is complete!"
else
    print_error "❌ Failed to start enhanced streaming job"
    exit 1
fi 