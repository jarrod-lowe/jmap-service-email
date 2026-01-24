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
