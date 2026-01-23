variable "aws_region" {
  description = "AWS region for resources"
  type        = string
  default     = "ap-southeast-2"
}

variable "environment" {
  description = "Environment name (test, prod)"
  type        = string

  validation {
    condition     = contains(["test", "prod"], var.environment)
    error_message = "Environment must be either 'test' or 'prod'"
  }
}

variable "log_retention_days" {
  description = "CloudWatch log retention in days"
  type        = number
  default     = 30
}

variable "lambda_memory_size" {
  description = "Lambda memory size in MB"
  type        = number
  default     = 256
}

variable "lambda_timeout" {
  description = "Lambda timeout in seconds"
  type        = number
  default     = 30
}

# Plugin-specific variables for integration with jmap-service-core

variable "jmap_core_table_name" {
  description = "Name of the JMAP core DynamoDB table"
  type        = string
}

variable "plugin_version" {
  description = "Plugin version"
  type        = string
  default     = "1.0.0"
}

variable "jmap_core_api_gateway_arn" {
  description = "ARN of the JMAP core API Gateway (execution ARN)"
  type        = string
}

variable "jmap_core_api_gateway_url" {
  description = "URL of the JMAP core API Gateway"
  type        = string
}
