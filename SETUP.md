# Setup Guide

This guide will help you set up the GCP Data Lakehouse with Apache Iceberg project using Poetry for dependency management.

## Prerequisites

- Python 3.9 or higher
- Google Cloud SDK (`gcloud`)
- Terraform >= 1.0
- Poetry (for dependency management)

## Quick Start

### 1. Install Poetry

If you don't have Poetry installed:

```bash
# Install Poetry
curl -sSL https://install.python-poetry.org | python3 -

# Add Poetry to your PATH (if needed)
export PATH="$HOME/.local/bin:$PATH"

# Verify installation
poetry --version
```

### 2. Clone and Setup Project

```bash
git clone <repository-url>
cd dt_gcp_lakehouse_iceberg

# Install dependencies with Poetry
poetry install

# Activate the virtual environment
poetry shell
```

### 3. Configure Environment

```bash
# Copy the environment template
cp env.example .env

# Edit the .env file with your GCP project details
# Required variables:
# - PROJECT_ID: Your GCP project ID
# - REGION: GCP region (default: us-central1)
# - DATASET_ID: BigQuery dataset name (default: taxi_dataset)
```

### 4. Authenticate with GCP

```bash
# Login to GCP
gcloud auth login

# Set your project
gcloud config set project YOUR_PROJECT_ID

# Create application default credentials
gcloud auth application-default login
```

### 5. Deploy Infrastructure

```bash
# Make the deployment script executable
chmod +x scripts/deploy.sh

# Run the deployment
./scripts/deploy.sh
```

## What Gets Created Automatically

The Terraform configuration will automatically create all necessary GCP resources:

### Storage Resources
- **Iceberg Data Bucket**: Stores Apache Iceberg table data
- **Temp Bucket**: Temporary storage for Dataflow operations
- **Staging Bucket**: Staging area for data processing

### BigQuery Resources
- **Dataset**: Main dataset for taxi data
- **BigLake Catalog**: Iceberg catalog for table metadata

### Pub/Sub Resources
- **Main Topic**: For taxi trip data ingestion
- **Subscription**: For processing messages
- **Dead Letter Topic**: For failed message handling

### Service Accounts
- **Dataflow Service Account**: With appropriate permissions for data processing
- **Cloud Function Service Account**: For serverless functions

### IAM Roles
All service accounts are automatically configured with the minimum required permissions.

## Manual Setup (AWS Resources Only)

If you enable cross-cloud integration (`ENABLE_CROSS_CLOUD=true`), you'll need to manually create AWS resources:

1. **S3 Bucket**: For external data storage
2. **IAM Role**: For BigQuery Omni access
3. **Access Keys**: For authentication

## Development Workflow

### Running Commands with Poetry

All Python commands should be run through Poetry:

```bash
# Generate sample data
poetry run python -m src.data_generator.taxi_trip_simulator \
    --project_id=your-project \
    --mode=batch \
    --trip_count=100

# Run maintenance
poetry run python -m src.maintenance.iceberg_maintenance \
    --project_id=your-project \
    --action=full

# Run tests
poetry run pytest tests/ -v
```

### Adding Dependencies

```bash
# Add a new dependency
poetry add package-name

# Add a development dependency
poetry add --group dev package-name

# Update dependencies
poetry update
```

### Environment Management

```bash
# Activate the virtual environment
poetry shell

# Run a command in the environment
poetry run python script.py

# Show environment info
poetry env info
```

## Environment Configuration

The project uses `dev` as the default environment name. You can customize this in your `.env` file:

```bash
ENVIRONMENT=dev  # or staging, prod, etc.
```

This affects resource naming:
- Buckets: `{project-id}-iceberg-data-{environment}`
- Service accounts: `dataflow-service-account-{environment}`
- Topics: `{topic-name}-{environment}`

## Troubleshooting

### Poetry Issues

```bash
# Clear Poetry cache
poetry cache clear pypi --all

# Reinstall dependencies
poetry install --no-cache

# Check for dependency conflicts
poetry check
```

### GCP Authentication Issues

```bash
# Check current authentication
gcloud auth list

# Re-authenticate
gcloud auth application-default login

# Check project configuration
gcloud config list
```

### Terraform Issues

```bash
# Initialize Terraform
cd terraform
terraform init

# Check configuration
terraform validate

# Plan deployment
terraform plan
```

## Next Steps

After successful deployment:

1. **Verify Resources**: Check that all GCP resources were created
2. **Generate Data**: Run the taxi trip simulator to create sample data
3. **Query Data**: Use BigQuery to query your Iceberg tables
4. **Monitor**: Check Dataflow jobs and Cloud Functions logs
5. **Customize**: Modify the configuration for your specific use case

For detailed usage instructions, see the main [README.md](README.md). 