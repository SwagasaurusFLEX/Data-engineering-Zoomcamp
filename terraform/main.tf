terraform {
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "4.51.0"
    }
  }
}

provider "google" {
  
  credentials = "./keys/keys.json"
  project = "totemic-studio-484706-n3"
  region  = "us-central1"
}



resource "google_storage_bucket" "demo-bucket" {
  name     = "i-h4ve-no-m0n3y"
  location = "US"

  # Optional, but recommended settings:
  storage_class               = "STANDARD"
  uniform_bucket_level_access = true

  versioning {
    enabled = true
  }

  lifecycle_rule {
    action {
      type = "Delete"
    }
    condition {
      age = 30 // days
    }
  }

  force_destroy = true
}


resource "google_bigquery_dataset" "dataset" {
  dataset_id = "demo_dataset_free"
  project    = "totemic-studio-484706-n3"
  location   = "US"
}