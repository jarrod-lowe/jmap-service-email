# Module outputs

output "placeholder_function_name" {
  description = "Name of the placeholder Lambda function"
  value       = aws_lambda_function.placeholder.function_name
}

output "placeholder_function_arn" {
  description = "ARN of the placeholder Lambda function"
  value       = aws_lambda_function.placeholder.arn
}

output "lambda_execution_role_arn" {
  description = "ARN of the Lambda execution role"
  value       = aws_iam_role.lambda_execution.arn
}

output "plugin_id" {
  description = "Plugin identifier registered in core"
  value       = local.plugin_name
}
