#!/bin/bash

# Demo script for Enhanced Sliding Window Analytics
# This script demonstrates the location-based feature engineering capabilities
# by generating realistic taxi data and processing it with sliding windows

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

print_header() {
    echo -e "${BLUE}========================================${NC}"
    echo -e "${BLUE}  $1${NC}"
    echo -e "${BLUE}========================================${NC}"
}

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

print_header "Enhanced Sliding Window Analytics Demo"
print_status "This demo will:"
print_status "  1. Start enhanced data generation (50+ NYC zones)"
print_status "  2. Launch sliding window processing (5-min windows, 10-sec refresh)"
print_status "  3. Monitor real-time location features"
print_status "  4. Show example queries for demand analytics"
print_status ""
print_warning "Estimated demo time: 10-15 minutes"
print_warning "Estimated cost: $5-10 USD (remember to clean up afterwards)"

# Ask for confirmation
read -p "Do you want to proceed with the demo? (y/N): " -n 1 -r
echo
if [[ ! $REPLY =~ ^[Yy]$ ]]; then
    print_status "Demo cancelled."
    exit 0
fi

# Demo configuration
DEMO_SLIDING_WINDOW_SIZE=300  # 5 minutes
DEMO_SLIDING_WINDOW_PERIOD=10 # 10 seconds
DEMO_TRIPS_PER_MINUTE=25      # Higher volume for better analytics
DEMO_DURATION_MINUTES=10      # Run for 10 minutes

print_header "Step 1: Starting Enhanced Data Generator"
print_status "Generating $DEMO_TRIPS_PER_MINUTE trips/minute with enhanced NYC zones..."
print_status "Duration: $DEMO_DURATION_MINUTES minutes"

# Start data generator in background
python -m src.data_generator.taxi_trip_simulator \
    --project_id="$PROJECT_ID" \
    --topic_name="$PUBSUB_TOPIC" \
    --mode=continuous \
    --trips_per_minute=$DEMO_TRIPS_PER_MINUTE \
    --duration_hours=1 &

DATA_GENERATOR_PID=$!
print_status "✅ Data generator started (PID: $DATA_GENERATOR_PID)"
print_status "Enhanced features: 50+ zones, realistic demand patterns, borough diversity"

# Wait a moment for data to start flowing
sleep 15

print_header "Step 2: Starting Sliding Window Processing"
print_status "Window size: $DEMO_SLIDING_WINDOW_SIZE seconds (5 minutes)"
print_status "Refresh rate: $DEMO_SLIDING_WINDOW_PERIOD seconds (10 seconds)"
print_status "Features: Location demand patterns, revenue analytics, anomaly detection"

# Start streaming job
./scripts/run_streaming_job.sh $DEMO_SLIDING_WINDOW_SIZE $DEMO_SLIDING_WINDOW_PERIOD

# Get the job name from the most recent dataflow job
sleep 30
STREAMING_JOB=$(gcloud dataflow jobs list --region=$REGION --filter="name~enhanced-streaming-taxi" --format="value(name)" --limit=1)

if [ -z "$STREAMING_JOB" ]; then
    print_error "Could not find streaming job. Check the Dataflow console."
    kill $DATA_GENERATOR_PID 2>/dev/null || true
    exit 1
fi

print_status "✅ Streaming job started: $STREAMING_JOB"

print_header "Step 3: Monitoring Real-time Features (5 minutes)"
print_status "Waiting for sliding window features to populate..."
print_status "Monitor progress at: https://console.cloud.google.com/dataflow/jobs/$REGION/$STREAMING_JOB?project=$PROJECT_ID"

# Wait for data to accumulate
for i in {1..30}; do
    echo -n "."
    sleep 10
done
echo

print_header "Step 4: Demo Queries and Analytics"

# Query 1: Check if data is flowing
print_status "Query 1: Checking data flow..."
TRIP_COUNT=$(bq query --use_legacy_sql=false --format=csv --max_rows=1 \
    "SELECT COUNT(*) as count FROM \`$PROJECT_ID.$DATASET_ID.taxi_trips\` WHERE created_at > TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 10 MINUTE)" 2>/dev/null | tail -n 1)

if [ "$TRIP_COUNT" -gt 0 ]; then
    print_status "✅ $TRIP_COUNT trips processed in last 10 minutes"
else
    print_warning "⚠️  No trips detected yet. Data may still be processing..."
fi

# Query 2: Location features
sleep 30
print_status "Query 2: Checking location features..."
FEATURE_COUNT=$(bq query --use_legacy_sql=false --format=csv --max_rows=1 \
    "SELECT COUNT(*) as count FROM \`$PROJECT_ID.$DATASET_ID.location_features_sliding_window\` WHERE created_at > TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 5 MINUTE)" 2>/dev/null | tail -n 1)

if [ "$FEATURE_COUNT" -gt 0 ]; then
    print_status "✅ $FEATURE_COUNT location feature records generated"
else
    print_warning "⚠️  Location features still building up..."
fi

# Query 3: Show top demand locations
print_status "Query 3: Top demand locations (last 5 minutes)..."
bq query --use_legacy_sql=false --format=table --max_rows=10 \
    "SELECT 
        pickup_location_id,
        trip_count,
        ROUND(demand_intensity_per_minute, 4) as demand_per_min,
        ROUND(total_revenue, 2) as revenue,
        high_demand_flag,
        premium_location_flag
     FROM \`$PROJECT_ID.$DATASET_ID.location_features_sliding_window\`
     WHERE window_start > TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 5 MINUTE)
     ORDER BY demand_intensity_per_minute DESC
     LIMIT 10" 2>/dev/null || print_warning "Query may need more time to process..."

print_header "Step 5: Demo Analytics Queries"

echo "Try these queries in BigQuery for advanced analytics:"
echo
echo "-- Real-time demand hotspots"
echo "SELECT pickup_location_id, demand_intensity_per_minute, high_demand_flag"
echo "FROM \`$PROJECT_ID.$DATASET_ID.location_features_sliding_window\`"
echo "WHERE window_start > TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 30 MINUTE)"
echo "  AND high_demand_flag = true"
echo "ORDER BY demand_intensity_per_minute DESC;"
echo
echo "-- Revenue patterns by location"
echo "SELECT pickup_location_id, avg_trip_amount, tip_percentage, premium_location_flag"
echo "FROM \`$PROJECT_ID.$DATASET_ID.location_features_sliding_window\`"
echo "WHERE total_revenue > 100"
echo "ORDER BY avg_trip_amount DESC;"
echo
echo "-- Location diversity analysis"
echo "SELECT pickup_location_id, unique_dropoff_locations, destination_diversity_ratio"
echo "FROM \`$PROJECT_ID.$DATASET_ID.location_features_sliding_window\`"
echo "WHERE trip_count > 5"
echo "ORDER BY destination_diversity_ratio DESC;"

print_header "Demo Complete!"
print_status "The demo is now running. Key components:"
print_status "  • Data Generator: Generating realistic trips (PID: $DATA_GENERATOR_PID)"
print_status "  • Streaming Job: $STREAMING_JOB"
print_status "  • Features: Updated every 10 seconds with 5-minute sliding windows"
print_status ""
print_warning "💰 COST REMINDER: Remember to stop these resources when done!"
print_status ""
print_status "To stop the demo:"
print_status "  1. Stop data generator: kill $DATA_GENERATOR_PID"
print_status "  2. Stop streaming job: gcloud dataflow jobs cancel $STREAMING_JOB --region=$REGION"
print_status ""
print_status "Monitor and explore:"
print_status "  • Dataflow Console: https://console.cloud.google.com/dataflow/jobs/$REGION/$STREAMING_JOB?project=$PROJECT_ID"
print_status "  • BigQuery Tables: https://console.cloud.google.com/bigquery?project=$PROJECT_ID"
print_status "  • Location Analytics View: \`$PROJECT_ID.$DATASET_ID.location_demand_analytics\`"
print_status "  • Real-time Alerts View: \`$PROJECT_ID.$DATASET_ID.real_time_location_alerts\`"

# Create a cleanup script
cat > /tmp/cleanup_demo.sh << EOF
#!/bin/bash
echo "Cleaning up sliding window demo..."
kill $DATA_GENERATOR_PID 2>/dev/null || echo "Data generator already stopped"
gcloud dataflow jobs cancel $STREAMING_JOB --region=$REGION 2>/dev/null || echo "Streaming job already stopped"
echo "Demo cleanup complete!"
EOF

chmod +x /tmp/cleanup_demo.sh
print_status ""
print_status "Quick cleanup script created: /tmp/cleanup_demo.sh"
print_status "Run it when you're done with the demo!"

print_header "Enjoy exploring real-time location analytics! 🚕📊" 