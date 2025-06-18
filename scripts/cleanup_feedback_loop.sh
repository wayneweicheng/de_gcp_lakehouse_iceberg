#!/bin/bash

# Cleanup script to stop the feedback loop
# This script stops the scheduler and cleans up the mess

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

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

print_status "Cleaning up feedback loop for project: $PROJECT_ID"

# Step 1: Disable the scheduler job immediately
print_status "Disabling Cloud Scheduler job..."
gcloud scheduler jobs pause taxi-data-generation --location="$REGION" || print_warning "Scheduler job might not exist or already paused"

# Step 2: Delete the Cloud Function to stop new executions
print_status "Deleting Cloud Function to stop new executions..."
gcloud functions delete taxi-data-generator --region="$REGION" --quiet || print_warning "Function might not exist"

# Step 3: Clean up any remaining messages in the topic
print_status "Cleaning up Pub/Sub messages..."
gcloud pubsub subscriptions seek "$PUBSUB_SUBSCRIPTION" --time="$(date -u +%Y-%m-%dT%H:%M:%SZ)" || print_warning "Subscription might not exist"

# Step 4: Show current function execution stats
print_status "Checking recent function executions..."
gcloud logging read "resource.type=cloud_function AND resource.labels.function_name=taxi-data-generator" --limit=10 --format="table(timestamp,severity,textPayload)" --freshness=1h || print_warning "No recent logs found"

# Step 5: Show current Pub/Sub message stats
print_status "Checking Pub/Sub topic message count..."
gcloud pubsub topics describe "$PUBSUB_TOPIC" || print_warning "Topic might not exist"

print_status "Cleanup completed!"
print_warning "The feedback loop should now be stopped."
print_warning "You can now redeploy with the fixed configuration using ./scripts/deploy.sh" 