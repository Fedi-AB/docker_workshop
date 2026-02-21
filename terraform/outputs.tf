# Outputs for the Terraform configuration
output "bucket_name" {
  value = google_storage_bucket.data_lake.name
}

# Output the dataset ID for BigQuery
output "dataset_id" {
  value = google_bigquery_dataset.dataset.dataset_id
}
