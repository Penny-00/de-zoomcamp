terraform {
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "7.20.0"
    }
  }
}

provider "google" {
  #you can hardcode it like this...
  #credentials = file("./keys/my-cred.json")
  # or you can type the following in the terminal
  # export GOOGLE_APPLICATION_CREDENTIALS="/home/penny_dev/projects/terrademo/keys/my-cred.json"
  # export GOOGLE_CREDENTIALS="/home/penny_dev/projects/terrademo/keys/my-cred.json"
  project = var.project
  region  = var.region
}

resource "google_storage_bucket" "demo-bucket" {
  name          = var.gcs_bucket_name
  location      = var.location
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
  dataset_id = var.bq_dataset_name
  location = var.location
}