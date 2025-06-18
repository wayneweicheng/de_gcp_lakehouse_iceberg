#!/bin/bash

# AWS Setup Script for BigQuery Omni Integration
# This script creates the necessary AWS resources for cross-cloud integration

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

# Check if AWS CLI is installed and configured
if ! command -v aws &> /dev/null; then
    print_error "AWS CLI is not installed. Please install it first."
    exit 1
fi

# Check AWS credentials
if ! aws sts get-caller-identity &> /dev/null; then
    print_error "AWS credentials not configured. Please run 'aws configure' first."
    exit 1
fi

# Get AWS Account ID
AWS_ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text)
print_status "Using AWS Account ID: $AWS_ACCOUNT_ID"

# Prompt for required information
read -p "Enter your GCP Project ID: " GCP_PROJECT_ID
read -p "Enter your GCP Project Number: " GCP_PROJECT_NUMBER
read -p "Enter S3 bucket name for external data (e.g., ${GCP_PROJECT_ID}-external-taxi-data): " S3_BUCKET_NAME

# Validate inputs
if [ -z "$GCP_PROJECT_ID" ] || [ -z "$GCP_PROJECT_NUMBER" ] || [ -z "$S3_BUCKET_NAME" ]; then
    print_error "All fields are required"
    exit 1
fi

print_status "Creating AWS resources for BigQuery Omni integration..."

# Step 1: Create S3 bucket
print_status "Creating S3 bucket: $S3_BUCKET_NAME"
if aws s3 mb "s3://$S3_BUCKET_NAME" 2>/dev/null; then
    print_status "✓ S3 bucket created successfully"
else
    print_warning "S3 bucket might already exist or creation failed"
fi

# Step 2: Create IAM role trust policy
print_status "Creating IAM role trust policy..."
cat > /tmp/trust-policy.json << EOF
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Principal": {
                "Federated": "arn:aws:iam::${AWS_ACCOUNT_ID}:oidc-provider/accounts.google.com"
            },
            "Action": "sts:AssumeRoleWithWebIdentity",
            "Condition": {
                "StringEquals": {
                    "accounts.google.com:sub": "service-${GCP_PROJECT_NUMBER}@gcp-sa-bigquery-omni.iam.gserviceaccount.com"
                }
            }
        }
    ]
}
EOF

# Step 3: Create IAM role
print_status "Creating IAM role: BigQueryOmniRole"
if aws iam create-role \
    --role-name BigQueryOmniRole \
    --assume-role-policy-document file:///tmp/trust-policy.json \
    --description "Role for BigQuery Omni to access S3 data" 2>/dev/null; then
    print_status "✓ IAM role created successfully"
else
    print_warning "IAM role might already exist"
fi

# Step 4: Create S3 access policy
print_status "Creating S3 access policy..."
cat > /tmp/s3-policy.json << EOF
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Action": [
                "s3:GetObject",
                "s3:GetObjectVersion",
                "s3:ListBucket",
                "s3:GetBucketLocation"
            ],
            "Resource": [
                "arn:aws:s3:::${S3_BUCKET_NAME}",
                "arn:aws:s3:::${S3_BUCKET_NAME}/*"
            ]
        }
    ]
}
EOF

# Step 5: Create and attach policy
print_status "Creating and attaching S3 policy..."
POLICY_ARN="arn:aws:iam::${AWS_ACCOUNT_ID}:policy/BigQueryOmniS3Policy"

if aws iam create-policy \
    --policy-name BigQueryOmniS3Policy \
    --policy-document file:///tmp/s3-policy.json \
    --description "Policy for BigQuery Omni S3 access" 2>/dev/null; then
    print_status "✓ S3 policy created successfully"
else
    print_warning "S3 policy might already exist"
fi

# Attach policy to role
if aws iam attach-role-policy \
    --role-name BigQueryOmniRole \
    --policy-arn "$POLICY_ARN" 2>/dev/null; then
    print_status "✓ Policy attached to role successfully"
else
    print_warning "Policy might already be attached"
fi

# Step 6: Set up OIDC provider (if not exists)
print_status "Setting up Google OIDC provider..."
if aws iam create-open-id-connect-provider \
    --url https://accounts.google.com \
    --thumbprint-list 1c58a3a8518e8759bf075b76b750d4f2df264fcd \
    --client-id-list sts.amazonaws.com 2>/dev/null; then
    print_status "✓ OIDC provider created successfully"
else
    print_warning "OIDC provider might already exist"
fi

# Clean up temporary files
rm -f /tmp/trust-policy.json /tmp/s3-policy.json

# Output configuration
print_status "AWS setup completed successfully!"
echo ""
print_status "Add these values to your .env file:"
echo "ENABLE_CROSS_CLOUD=true"
echo "AWS_ACCOUNT_ID=$AWS_ACCOUNT_ID"
echo "S3_EXTERNAL_BUCKET=$S3_BUCKET_NAME"
echo ""
print_status "The IAM role ARN is:"
echo "arn:aws:iam::${AWS_ACCOUNT_ID}:role/BigQueryOmniRole"
echo ""
print_status "You can now run ./scripts/deploy.sh to deploy the infrastructure"

# Test AWS access
print_status "Testing AWS access..."
if aws s3 ls "s3://$S3_BUCKET_NAME" &>/dev/null; then
    print_status "✓ S3 bucket access confirmed"
else
    print_error "✗ S3 bucket access failed"
fi

print_status "Setup complete! 🎉" 