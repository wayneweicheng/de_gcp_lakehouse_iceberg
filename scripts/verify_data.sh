#!/bin/bash

# Verify loaded data in BigQuery Iceberg tables
# Usage: ./scripts/verify_data.sh

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
required_vars=("PROJECT_ID" "DATASET_ID")
for var in "${required_vars[@]}"; do
    if [ -z "${!var}" ]; then
        print_error "Required environment variable $var is not set"
        exit 1
    fi
done

print_status "Verifying data in $PROJECT_ID.$DATASET_ID"

# Create temporary SQL file with substituted values
TEMP_SQL_FILE=$(mktemp)
sed "s/\${PROJECT_ID}/$PROJECT_ID/g; s/\${DATASET_ID}/$DATASET_ID/g" scripts/verify_loaded_data.sql > "$TEMP_SQL_FILE"

print_status "Running data verification queries..."

# Execute the SQL queries
if bq query --use_legacy_sql=false < "$TEMP_SQL_FILE"; then
    print_status "✅ Data verification completed successfully!"
else
    print_error "❌ Data verification failed"
    rm -f "$TEMP_SQL_FILE"
    exit 1
fi

# Clean up
rm -f "$TEMP_SQL_FILE"

print_status "🎉 Data verification completed!" 