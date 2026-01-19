terraform {
  required_version = ">= 1.0"

  backend "s3" {
    # Bucket name is set via -backend-config in Makefile
    # Key includes environment from -backend-config
    # Region is set via -backend-config
    encrypt = true
  }

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 6.0"
    }
    archive = {
      source  = "hashicorp/archive"
      version = "~> 2.0"
    }
  }
}

provider "aws" {
  region = var.aws_region

  default_tags {
    tags = {
      Project     = "jmap-service-email"
      ManagedBy   = "terraform"
      Environment = var.environment
      Application = "jmap-service-email-${var.environment}"
    }
  }
}

module "jmap_service_email" {
  source = "../../modules/jmap-service-email"

  aws_region           = var.aws_region
  environment          = var.environment
  log_retention_days   = var.log_retention_days
  lambda_memory_size   = var.lambda_memory_size
  lambda_timeout       = var.lambda_timeout
  jmap_core_table_name = var.jmap_core_table_name
  plugin_version       = var.plugin_version
}
