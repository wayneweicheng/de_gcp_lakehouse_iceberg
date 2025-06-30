#!/bin/bash

# Build Flex Templates for Dataflow
# This script builds Docker image and creates Flex Templates

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

# Set Docker image URL - using Artifact Registry
DOCKER_REGISTRY="${REGION}-docker.pkg.dev"
DOCKER_REPO="dataflow-templates"
DOCKER_IMAGE="${DOCKER_REGISTRY}/$PROJECT_ID/$DOCKER_REPO/lakehouse-iceberg:latest"

print_status "Building and deploying complete Flex Template pipeline..."
print_status "Docker image: $DOCKER_IMAGE"

# Step 1: Enable required APIs
print_status "Ensuring Artifact Registry API is enabled..."
gcloud services enable artifactregistry.googleapis.com --quiet || true

# Step 2: Create Artifact Registry repository if it doesn't exist
print_status "Creating Artifact Registry repository if needed..."
if ! gcloud artifacts repositories describe "$DOCKER_REPO" --location="$REGION" --quiet 2>/dev/null; then
    print_status "Creating new Artifact Registry repository: $DOCKER_REPO"
    gcloud artifacts repositories create "$DOCKER_REPO" \
        --location="$REGION" \
        --repository-format=docker \
        --description="Dataflow Flex Templates for Lakehouse Iceberg"
else
    print_status "Artifact Registry repository already exists"
fi

# Step 3: Configure Docker authentication
print_status "Configuring Docker authentication for Artifact Registry..."
gcloud auth configure-docker "${DOCKER_REGISTRY}" --quiet

# Step 4: Build Docker Image
print_status "Building Docker image with updated Dataflow code..."
print_status "This includes the new sliding window functionality and location analytics"

docker build \
    --platform linux/amd64 \
    -t "$DOCKER_IMAGE" \
    --build-arg PROJECT_ID="$PROJECT_ID" \
    --no-cache \
    .

if [ $? -eq 0 ]; then
    print_status "✅ Docker image built successfully!"
else
    print_error "❌ Docker image build failed"
    exit 1
fi

# Step 5: Push Docker Image to Artifact Registry
print_status "Pushing Docker image to Artifact Registry..."

docker push "$DOCKER_IMAGE"

if [ $? -eq 0 ]; then
    print_status "✅ Docker image pushed successfully!"
else
    print_error "❌ Docker image push failed"
    exit 1
fi

# Step 6: Create templates directory in GCS if it doesn't exist
print_status "Ensuring templates directory exists in GCS..."
gsutil -q stat "gs://$TEMP_BUCKET/flex-templates/" 2>/dev/null || echo "Templates directory will be created automatically"

# Step 7: Build Batch Processing Flex Template
print_status "Building Batch Processing Flex Template..."

gcloud dataflow flex-template build "gs://$TEMP_BUCKET/flex-templates/batch-taxi-processor.json" \
    --image="$DOCKER_IMAGE" \
    --sdk-language=PYTHON \
    --metadata-file="templates/batch_taxi_processor_metadata.json" \
    --additional-experiments=use_runner_v2

if [ $? -eq 0 ]; then
    print_status "✅ Batch Processing Flex Template created successfully!"
    print_status "   Location: gs://$TEMP_BUCKET/flex-templates/batch-taxi-processor.json"
else
    print_error "❌ Failed to create Batch Processing Flex Template"
    exit 1
fi

# Step 8: Build Streaming Processing Flex Template (with updated sliding window metadata)
print_status "Building Streaming Processing Flex Template with sliding window support..."

gcloud dataflow flex-template build "gs://$TEMP_BUCKET/flex-templates/streaming-taxi-processor.json" \
    --image="$DOCKER_IMAGE" \
    --sdk-language=PYTHON \
    --metadata-file="templates/streaming_taxi_processor_metadata.json" \
    --additional-experiments=use_runner_v2

if [ $? -eq 0 ]; then
    print_status "✅ Streaming Processing Flex Template created successfully!"
    print_status "   Location: gs://$TEMP_BUCKET/flex-templates/streaming-taxi-processor.json"
    print_status "   🎯 New features: 30-minute sliding windows, location analytics, real-time features"
else
    print_error "❌ Failed to create Streaming Processing Flex Template"
    exit 1
fi

# Step 9: Validate templates
print_status "Validating Flex Templates..."

# List templates to verify they exist
gsutil ls "gs://$TEMP_BUCKET/flex-templates/" | grep -E "(batch|streaming)-taxi-processor.json" && \
    print_status "✅ Templates validated successfully!" || \
    print_warning "⚠️  Template validation incomplete"

print_status "🎉 Complete build and deployment finished!"
print_status ""
print_status "🚀 You can now run jobs using the updated templates:"
print_status ""
print_status "📊 Batch Processing:"
print_status "  gcloud dataflow flex-template run batch-job-\$(date +%Y%m%d-%H%M%S) \\"
print_status "    --template-file-gcs-location=gs://$TEMP_BUCKET/flex-templates/batch-taxi-processor.json \\"
print_status "    --region=$REGION \\"
print_status "    --parameters=mode=batch,project_id=$PROJECT_ID,dataset_id=\$DATASET_ID"
print_status ""
print_status "🌊 Streaming Processing (with sliding windows):"
print_status "  gcloud dataflow flex-template run streaming-job-\$(date +%Y%m%d-%H%M%S) \\"
print_status "    --template-file-gcs-location=gs://$TEMP_BUCKET/flex-templates/streaming-taxi-processor.json \\"
print_status "    --region=$REGION \\"
print_status "    --parameters=mode=streaming,subscription_name=projects/$PROJECT_ID/subscriptions/taxi-trips-sub,project_id=$PROJECT_ID,dataset_id=\$DATASET_ID,sliding_window_size=1800,sliding_window_period=30,enable_sliding_windows=true"
print_status ""
print_status "🎯 Key new features in updated templates:"
print_status "  • 30-minute sliding windows with 30-second refresh"
print_status "  • Location-based demand pattern analysis"
print_status "  • Real-time anomaly detection"
print_status "  • Enhanced revenue and distance analytics"
print_status "  • Cross-borough trip analysis"
print_status ""
print_status "💡 Use the demo script to test sliding window functionality:"
print_status "  ./scripts/demo_sliding_windows.sh" 