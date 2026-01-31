terraform {
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "7.17.0"
    }
  }
}

provider "google" {
  project = "dezoomcamp2026-485910"
  region  = "us-central1"
}


resource "google_storage_bucket" "auto-expire" {
  name          = "demo-bucket-dezoomcamp2026-485910"
  storage_class = var.gcs_storage_class
  location      = "US"
  force_destroy = true

  lifecycle_rule {
    condition {
      age = 3
    }
    action {
      type = "Delete"
    }
  }

  lifecycle_rule {
    condition {
      age = 1
    }
    action {
      type = "AbortIncompleteMultipartUpload"
    }
  }
}

resource "google_bigquery_dataset" "demo_dataset" {
  dataset_id = var.bg_dataset_name

}
