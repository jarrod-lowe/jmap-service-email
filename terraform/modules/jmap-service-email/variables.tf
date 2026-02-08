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

variable "plugin_version" {
  description = "Plugin version"
  type        = string
  default     = "1.0.0"
}

variable "max_body_value_bytes" {
  description = "Maximum size in bytes for Email/get bodyValues content (server-side cap)"
  type        = number
  default     = 262144 # 256KB
}

variable "thread_query_concurrency" {
  description = "Maximum concurrent DynamoDB queries for Thread/get"
  type        = number
  default     = 5
}

variable "summary_model_id" {
  description = "Bedrock inference profile ID for AI email summarization"
  type        = string
  default     = "au.anthropic.claude-haiku-4-5-20251001-v1:0"
}

variable "summary_max_length" {
  description = "Maximum length in characters for AI-generated email summaries"
  type        = number
  default     = 256
}

variable "summary_overwrites_preview" {
  description = "Whether AI-generated summary should overwrite the text-extracted preview"
  type        = bool
  default     = true
}
