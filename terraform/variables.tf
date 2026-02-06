variable "project_id" {
  description = "GCP Project ID"
  type        = string
}

variable "region" {
  description = "GCP region"
  type        = string
  default     = "europe-west1"
}

variable "zone" {
  description = "GCP zone"
  type        = string
  default     = "europe-west1-b"
}

variable "bucket_name" {
  description = "Nom du bucket GCS"
  type        = string
}

variable "bq_dataset_id" {
  description = "BigQuery Dataset ID"
  type        = string
}