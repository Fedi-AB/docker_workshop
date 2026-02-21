# Bucket GCS (Data Lake)
resource "google_storage_bucket" "data_lake" {
  name          = var.bucket_name
  location      = var.region
  storage_class = "STANDARD"
  force_destroy = true
}


# Dataset BigQuery (Analytics)
resource "google_bigquery_dataset" "dataset" {
  dataset_id = var.bq_dataset_id
  location   = var.region
}
