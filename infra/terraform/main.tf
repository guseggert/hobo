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