# CloudWatch log groups for Lambda functions

resource "aws_cloudwatch_log_group" "placeholder" {
  name              = "/aws/lambda/${local.name_prefix}-placeholder"
  retention_in_days = var.log_retention_days
}

resource "aws_cloudwatch_log_group" "email_import" {
  name              = "/aws/lambda/${local.name_prefix}-email-import"
  retention_in_days = var.log_retention_days
}

resource "aws_cloudwatch_log_group" "email_get" {
  name              = "/aws/lambda/${local.name_prefix}-email-get"
  retention_in_days = var.log_retention_days
}

resource "aws_cloudwatch_log_group" "email_query" {
  name              = "/aws/lambda/${local.name_prefix}-email-query"
  retention_in_days = var.log_retention_days
}

resource "aws_cloudwatch_log_group" "mailbox_get" {
  name              = "/aws/lambda/${local.name_prefix}-mailbox-get"
  retention_in_days = var.log_retention_days
}

resource "aws_cloudwatch_log_group" "mailbox_set" {
  name              = "/aws/lambda/${local.name_prefix}-mailbox-set"
  retention_in_days = var.log_retention_days
}
