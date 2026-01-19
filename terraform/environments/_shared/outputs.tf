# Lambda outputs
output "placeholder_function_name" {
  description = "Name of the placeholder Lambda function"
  value       = module.jmap_service_email.placeholder_function_name
}

output "placeholder_function_arn" {
  description = "ARN of the placeholder Lambda function"
  value       = module.jmap_service_email.placeholder_function_arn
}

# IAM outputs
output "lambda_execution_role_arn" {
  description = "ARN of the Lambda execution role"
  value       = module.jmap_service_email.lambda_execution_role_arn
}

# Plugin outputs
output "plugin_id" {
  description = "Plugin identifier registered in core"
  value       = module.jmap_service_email.plugin_id
}
