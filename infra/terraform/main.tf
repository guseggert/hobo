provider "aws" {
  region = "us-east-2"
}

terraform {
  backend "s3" {
    bucket = "hobo-tf"
    key    = "hobo/terraform.tfstate"
    region = "us-east-2"
  }
}

resource "aws_s3_bucket" "hobo_state" {
  bucket = var.hobo_bucket_name
}

resource "aws_s3_bucket_versioning" "hobo_state" {
  bucket = aws_s3_bucket.hobo_state.id
  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_sqs_queue" "hobo_queue" {
  name                      = var.hobo_queue_name
  visibility_timeout_seconds = 60
  message_retention_seconds  = 1209600
  receive_wait_time_seconds  = 10
}

output "hobo_bucket_name" {
  value = aws_s3_bucket.hobo_state.bucket
}

output "hobo_queue_url" {
  value = aws_sqs_queue.hobo_queue.id
}