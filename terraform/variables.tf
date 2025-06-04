variable "project_id" {
  description = "GCP Project ID"
  type        = string
}

variable "region" {
  description = "GCP Region"
  type        = string
  default     = "us-central1"
}

variable "zone" {
  description = "GCP Zone"
  type        = string
  default     = "us-central1-a"
}

variable "environment" {
  description = "Environment (dev, staging, prod)"
  type        = string
  default     = "dev"
}

variable "dataset_id" {
  description = "BigQuery dataset ID for taxi data"
  type        = string
  default     = "taxi_dataset"
}

variable "iceberg_bucket_name" {
  description = "GCS bucket name for Iceberg data"
  type        = string
  default     = ""
}

variable "temp_bucket_name" {
  description = "GCS bucket name for temporary data"
  type        = string
  default     = ""
}

variable "pubsub_topic_name" {
  description = "Pub/Sub topic name for data ingestion"
  type        = string
  default     = "taxi-trips-stream"
}

variable "enable_cross_cloud" {
  description = "Enable cross-cloud integration with AWS"
  type        = bool
  default     = false
}

variable "aws_account_id" {
  description = "AWS Account ID for cross-cloud integration"
  type        = string
  default     = ""
}

variable "dataflow_max_workers" {
  description = "Maximum number of Dataflow workers"
  type        = number
  default     = 20
}

variable "dataflow_machine_type" {
  description = "Machine type for Dataflow workers"
  type        = string
  default     = "n1-standard-4"
}

variable "snapshot_retention_days" {
  description = "Number of days to retain Iceberg snapshots"
  type        = number
  default     = 7
}

variable "enable_monitoring" {
  description = "Enable monitoring and alerting"
  type        = bool
  default     = true
}

variable "alert_email" {
  description = "Email address for alerts"
  type        = string
  default     = ""
} 