# Module outputs

output "lambda_execution_role_arn" {
  description = "ARN of the Lambda execution role"
  value       = aws_iam_role.lambda_execution.arn
}

output "plugin_id" {
  description = "Plugin identifier registered in core"
  value       = local.plugin_name
}

output "email_data_table_name" {
  description = "Name of the email data DynamoDB table"
  value       = aws_dynamodb_table.email_data.name
}

output "email_data_table_arn" {
  description = "ARN of the email data DynamoDB table"
  value       = aws_dynamodb_table.email_data.arn
}

output "email_import_function_name" {
  description = "Name of the email-import Lambda function"
  value       = aws_lambda_function.email_import.function_name
}

output "email_import_function_arn" {
  description = "ARN of the email-import Lambda function"
  value       = aws_lambda_function.email_import.arn
}
