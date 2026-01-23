# CloudWatch log groups for Lambda functions

resource "aws_cloudwatch_log_group" "placeholder" {
  name              = "/aws/lambda/${local.name_prefix}-placeholder"
  retention_in_days = var.log_retention_days
}

resource "aws_cloudwatch_log_group" "email_import" {
  name              = "/aws/lambda/${local.name_prefix}-email-import"
  retention_in_days = var.log_retention_days
}
