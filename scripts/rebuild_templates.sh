#!/bin/bash

# Quick Template Rebuild Script
# Use this when you've only changed the Dataflow pipeline code (main.py)
# and need to rebuild/redeploy just the Flex Templates

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
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

print_title() {
    echo -e "${BLUE}[REBUILD]${NC} $1"
}

print_title "🔄 Quick Template Rebuild for Code Changes"
print_status "This script rebuilds Docker image and Flex Templates after main.py changes"
echo ""

# Check if we're in the right directory
if [ ! -f "main.py" ]; then
    print_error "main.py not found. Please run this script from the project root directory."
    exit 1
fi

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

print_status "Project: $PROJECT_ID"
print_status "Region: $REGION"
print_status "Temp Bucket: $TEMP_BUCKET"
echo ""

# Stop any existing streaming jobs (optional, commented out for safety)
# print_status "Checking for existing streaming jobs..."
# existing_jobs=$(gcloud dataflow jobs list --region=$REGION --filter="name~streaming AND state=Running" --format="value(id)" | head -5)
# if [ -n "$existing_jobs" ]; then
#     print_warning "Found running streaming jobs. Consider stopping them first:"
#     for job_id in $existing_jobs; do
#         echo "  gcloud dataflow jobs cancel $job_id --region=$REGION"
#     done
#     echo ""
#     read -p "Continue anyway? (y/N): " -n 1 -r
#     echo
#     if [[ ! $REPLY =~ ^[Yy]$ ]]; then
#         exit 1
#     fi
# fi

# Build templates using the comprehensive build script
print_status "Running complete template build process..."
print_status "This will:"
print_status "  1. Build new Docker image with your code changes"
print_status "  2. Push image to Artifact Registry"
print_status "  3. Create updated Flex Templates"
print_status "  4. Validate deployment"
echo ""

# Run the build script
./scripts/build_flex_templates.sh

# Check if build was successful
if [ $? -eq 0 ]; then
    print_title "🎉 Template rebuild completed successfully!"
    echo ""
    print_status "✅ Your updated Dataflow templates are ready with:"
    print_status "   • Enhanced sliding window functionality (30-min windows, 30-sec refresh)"
    print_status "   • New CalculateLocationFeatures DoFn"
    print_status "   • New AggregateLocationFeatures DoFn"
    print_status "   • Real-time location analytics and anomaly detection"
    echo ""
    print_status "🚀 Next steps:"
    print_status "   1. Test with demo: ./scripts/demo_sliding_windows.sh"
    print_status "   2. Or run streaming job: ./scripts/run_streaming_job.sh"
    print_status "   3. Monitor in GCP Console: https://console.cloud.google.com/dataflow/jobs?project=$PROJECT_ID"
    echo ""
    print_warning "💡 Remember to update any running streaming jobs to use the new template!"
else
    print_error "❌ Template rebuild failed. Check the error messages above."
    exit 1
fi 