terraform {
  required_version = ">= 1.0"
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 5.0"
    }
    google-beta = {
      source  = "hashicorp/google-beta"
      version = "~> 5.0"
    }
  }
}

provider "google" {
  project = var.project_id
  region  = var.region
}

provider "google-beta" {
  project = var.project_id
  region  = var.region
}

# Local values for resource naming
locals {
  iceberg_bucket_name = var.iceberg_bucket_name != "" ? var.iceberg_bucket_name : "${var.project_id}-iceberg-data-${var.environment}"
  temp_bucket_name    = var.temp_bucket_name != "" ? var.temp_bucket_name : "${var.project_id}-temp-${var.environment}"
  staging_bucket_name = "${var.project_id}-staging-${var.environment}"
}

# Enable required APIs
resource "google_project_service" "required_apis" {
  for_each = toset([
    "bigquery.googleapis.com",
    "biglake.googleapis.com",
    "storage.googleapis.com",
    "pubsub.googleapis.com",
    "dataflow.googleapis.com",
    "cloudfunctions.googleapis.com",
    "cloudscheduler.googleapis.com",
    "monitoring.googleapis.com",
    "logging.googleapis.com"
  ])

  service = each.value
  project = var.project_id

  disable_dependent_services = true
}

# Cloud Storage Buckets
resource "google_storage_bucket" "iceberg_data" {
  name          = local.iceberg_bucket_name
  location      = var.region
  storage_class = "STANDARD"
  project       = var.project_id

  versioning {
    enabled = true
  }

  lifecycle_rule {
    condition {
      age = 30
    }
    action {
      type          = "SetStorageClass"
      storage_class = "NEARLINE"
    }
  }

  lifecycle_rule {
    condition {
      age = 90
    }
    action {
      type          = "SetStorageClass"
      storage_class = "COLDLINE"
    }
  }

  lifecycle_rule {
    condition {
      age = 365
    }
    action {
      type          = "SetStorageClass"
      storage_class = "ARCHIVE"
    }
  }

  depends_on = [google_project_service.required_apis]
}

resource "google_storage_bucket" "temp_bucket" {
  name          = local.temp_bucket_name
  location      = var.region
  storage_class = "STANDARD"
  project       = var.project_id

  lifecycle_rule {
    condition {
      age = 1
    }
    action {
      type = "Delete"
    }
  }

  depends_on = [google_project_service.required_apis]
}

resource "google_storage_bucket" "staging_bucket" {
  name          = local.staging_bucket_name
  location      = var.region
  storage_class = "STANDARD"
  project       = var.project_id

  lifecycle_rule {
    condition {
      age = 7
    }
    action {
      type = "Delete"
    }
  }

  depends_on = [google_project_service.required_apis]
}

# BigLake Catalog
resource "google_biglake_catalog" "iceberg_catalog" {
  name     = "iceberg-catalog-${var.environment}"
  location = var.region
  project  = var.project_id

  depends_on = [google_project_service.required_apis]
}

# BigQuery Dataset
resource "google_bigquery_dataset" "taxi_dataset" {
  dataset_id  = var.dataset_id
  location    = var.region
  project     = var.project_id
  description = "NYC Taxi dataset with Iceberg tables"

  labels = {
    environment = var.environment
    purpose     = "lakehouse"
  }

  depends_on = [google_project_service.required_apis]
}

# Pub/Sub Topic and Subscription
resource "google_pubsub_topic" "data_ingestion" {
  name    = var.pubsub_topic_name
  project = var.project_id

  message_storage_policy {
    allowed_persistence_regions = [var.region]
  }

  depends_on = [google_project_service.required_apis]
}

resource "google_pubsub_subscription" "data_ingestion_sub" {
  name    = "${var.pubsub_topic_name}-subscription"
  topic   = google_pubsub_topic.data_ingestion.name
  project = var.project_id

  ack_deadline_seconds = 600

  retry_policy {
    minimum_backoff = "10s"
    maximum_backoff = "600s"
  }

  dead_letter_policy {
    dead_letter_topic     = google_pubsub_topic.dead_letter.id
    max_delivery_attempts = 5
  }

  depends_on = [google_project_service.required_apis]
}

resource "google_pubsub_topic" "dead_letter" {
  name    = "${var.pubsub_topic_name}-dead-letter"
  project = var.project_id

  depends_on = [google_project_service.required_apis]
}

# Service Accounts
resource "google_service_account" "dataflow_sa" {
  account_id   = "dataflow-service-account-${var.environment}"
  display_name = "Dataflow Service Account"
  project      = var.project_id
}

resource "google_service_account" "cloud_function_sa" {
  account_id   = "cloud-function-sa-${var.environment}"
  display_name = "Cloud Function Service Account"
  project      = var.project_id
}

# IAM Bindings for Dataflow Service Account
resource "google_project_iam_member" "dataflow_roles" {
  for_each = toset([
    "roles/dataflow.worker",
    "roles/storage.admin",
    "roles/bigquery.admin",
    "roles/biglake.admin",
    "roles/pubsub.subscriber",
    "roles/pubsub.publisher"
  ])

  project = var.project_id
  role    = each.value
  member  = "serviceAccount:${google_service_account.dataflow_sa.email}"
}

# IAM Bindings for Cloud Function Service Account
resource "google_project_iam_member" "cloud_function_roles" {
  for_each = toset([
    "roles/pubsub.publisher",
    "roles/logging.logWriter",
    "roles/monitoring.metricWriter"
  ])

  project = var.project_id
  role    = each.value
  member  = "serviceAccount:${google_service_account.cloud_function_sa.email}"
}

# Cloud Scheduler for data generation
resource "google_cloud_scheduler_job" "taxi_data_generator" {
  name        = "taxi-data-generator-${var.environment}"
  description = "Generate taxi trip events"
  schedule    = "*/5 * * * *" # Every 5 minutes
  time_zone   = "UTC"
  region      = var.region
  project     = var.project_id

  pubsub_target {
    topic_name = google_pubsub_topic.data_ingestion.id
    data = base64encode(jsonencode({
      action = "generate_trips"
      count  = 10
    }))
  }

  depends_on = [google_project_service.required_apis]
}

# Cross-cloud resources (conditional)
resource "google_bigquery_connection" "aws_omni" {
  count         = var.enable_cross_cloud ? 1 : 0
  connection_id = "aws-taxi-omni-${var.environment}"
  location      = var.region
  description   = "Connection to AWS S3 for external taxi data"
  project       = var.project_id

  aws {
    access_role {
      iam_role_id = "arn:aws:iam::${var.aws_account_id}:role/BigQueryOmniRole"
    }
  }

  depends_on = [google_project_service.required_apis]
}

# Monitoring and Alerting
resource "google_monitoring_notification_channel" "email" {
  count        = var.enable_monitoring && var.alert_email != "" ? 1 : 0
  display_name = "Email Notification Channel"
  type         = "email"
  project      = var.project_id

  labels = {
    email_address = var.alert_email
  }

  depends_on = [google_project_service.required_apis]
}

resource "google_monitoring_alert_policy" "dataflow_job_failed" {
  count        = var.enable_monitoring ? 1 : 0
  display_name = "Dataflow Job Failed"
  project      = var.project_id

  conditions {
    display_name = "Dataflow job failure rate"

    condition_threshold {
      filter          = "resource.type=\"dataflow_job\""
      comparison      = "COMPARISON_GREATER_THAN"
      threshold_value = 0
      duration        = "300s"

      aggregations {
        alignment_period     = "300s"
        per_series_aligner   = "ALIGN_RATE"
        cross_series_reducer = "REDUCE_SUM"
        group_by_fields      = ["resource.label.job_name"]
      }
    }
  }

  combiner = "OR"
  notification_channels = var.enable_monitoring && var.alert_email != "" ? [google_monitoring_notification_channel.email[0].id] : []

  alert_strategy {
    auto_close = "1800s"
  }

  depends_on = [google_project_service.required_apis]
}

# Outputs
output "iceberg_bucket_name" {
  description = "Name of the Iceberg data bucket"
  value       = google_storage_bucket.iceberg_data.name
}

output "temp_bucket_name" {
  description = "Name of the temporary data bucket"
  value       = google_storage_bucket.temp_bucket.name
}

output "pubsub_topic_name" {
  description = "Name of the Pub/Sub topic"
  value       = google_pubsub_topic.data_ingestion.name
}

output "dataset_id" {
  description = "BigQuery dataset ID"
  value       = google_bigquery_dataset.taxi_dataset.dataset_id
}

output "dataflow_service_account" {
  description = "Dataflow service account email"
  value       = google_service_account.dataflow_sa.email
}

output "biglake_catalog_name" {
  description = "BigLake catalog name"
  value       = google_biglake_catalog.iceberg_catalog.name
} 