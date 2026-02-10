terraform {
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "7.17.0"
    }
  }
}

provider "google" {
  project     = "dezoomcamp2026-485910"
  region      = "us-central1"
  credentials = file("/workspaces/dezoomcamp2026/google-creds.json")
}

resource "google_storage_bucket" "kestra-bucket" {
  name          = "kestra-bucket-dezoomcamp2026-485910"
  location      = "us-central1"
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

resource "google_bigquery_dataset" "kestra-dataset" {
  dataset_id = "kestra_dataset"
  delete_contents_on_destroy = true
  location = "us-central1"
}