terraform {
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 6"
    }
  }

  required_version = ">= 1.2"
}

variable "hobo_bucket_name" {
  type        = string
  description = "S3 bucket name to store workflow state"
}

variable "hobo_queue_name" {
  type        = string
  description = "SQS queue name for workflow work items"
}