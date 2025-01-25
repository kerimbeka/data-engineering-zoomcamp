variable "credentials" {
  description = "My Credentials"
  default     = "./keys/my-creds.json"
}


variable "project" {
  description = "Project"
  default     = "terraform-demo-412122"
}

variable "region" {
  description = "Region"
  default     = "europe-west2-a"
}

variable "location" {
  description = "Project Location"
  default     = "EUROPE-WEST2"
}

variable "bq_dataset_name" {
  description = "My BigQuery Dataset Name"
  default     = "demo_dataset"
}

variable "gcs_bucket_name" {
  description = "My Storage Bucket Name"
  default     = "terraform-demo-bekzhan"
}

variable "gcs_storage_class" {
  description = "Bucket Storage Class"
  default     = "STANDARD"
}