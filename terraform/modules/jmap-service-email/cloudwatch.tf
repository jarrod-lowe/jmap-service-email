# CloudWatch log groups for Lambda functions

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

resource "aws_cloudwatch_log_group" "email_set" {
  name              = "/aws/lambda/${local.name_prefix}-email-set"
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

resource "aws_cloudwatch_log_group" "thread_get" {
  name              = "/aws/lambda/${local.name_prefix}-thread-get"
  retention_in_days = var.log_retention_days
}

resource "aws_cloudwatch_log_group" "email_changes" {
  name              = "/aws/lambda/${local.name_prefix}-email-changes"
  retention_in_days = var.log_retention_days
}

resource "aws_cloudwatch_log_group" "mailbox_changes" {
  name              = "/aws/lambda/${local.name_prefix}-mailbox-changes"
  retention_in_days = var.log_retention_days
}

resource "aws_cloudwatch_log_group" "thread_changes" {
  name              = "/aws/lambda/${local.name_prefix}-thread-changes"
  retention_in_days = var.log_retention_days
}

resource "aws_cloudwatch_log_group" "blob_delete" {
  name              = "/aws/lambda/${local.name_prefix}-blob-delete"
  retention_in_days = var.log_retention_days
}

resource "aws_cloudwatch_log_group" "mailbox_cleanup" {
  name              = "/aws/lambda/${local.name_prefix}-mailbox-cleanup"
  retention_in_days = var.log_retention_days
}

# CloudWatch alarm for mailbox-cleanup DLQ depth
resource "aws_cloudwatch_metric_alarm" "mailbox_cleanup_dlq" {
  alarm_name          = "${local.name_prefix}-mailbox-cleanup-dlq-depth"
  alarm_description   = "Mailbox cleanup DLQ has messages - investigate failed mailbox email cleanups"
  comparison_operator = "GreaterThanThreshold"
  evaluation_periods  = 1
  metric_name         = "ApproximateNumberOfMessagesVisible"
  namespace           = "AWS/SQS"
  period              = 300
  statistic           = "Maximum"
  threshold           = 0
  treat_missing_data  = "notBreaching"

  dimensions = {
    QueueName = aws_sqs_queue.mailbox_cleanup_dlq.name
  }
}

# CloudWatch alarm for blob-delete DLQ depth
resource "aws_cloudwatch_metric_alarm" "blob_delete_dlq" {
  alarm_name          = "${local.name_prefix}-blob-delete-dlq-depth"
  alarm_description   = "Blob delete DLQ has messages - investigate failed blob deletions"
  comparison_operator = "GreaterThanThreshold"
  evaluation_periods  = 1
  metric_name         = "ApproximateNumberOfMessagesVisible"
  namespace           = "AWS/SQS"
  period              = 300
  statistic           = "Maximum"
  threshold           = 0
  treat_missing_data  = "notBreaching"

  dimensions = {
    QueueName = aws_sqs_queue.blob_delete_dlq.name
  }
}

resource "aws_cloudwatch_log_group" "email_cleanup" {
  name              = "/aws/lambda/${local.name_prefix}-email-cleanup"
  retention_in_days = var.log_retention_days
}

# CloudWatch alarm for email-cleanup DLQ depth
resource "aws_cloudwatch_metric_alarm" "email_cleanup_dlq" {
  alarm_name          = "${local.name_prefix}-email-cleanup-dlq-depth"
  alarm_description   = "Email cleanup DLQ has messages - investigate failed email cleanup from DynamoDB Streams"
  comparison_operator = "GreaterThanThreshold"
  evaluation_periods  = 1
  metric_name         = "ApproximateNumberOfMessagesVisible"
  namespace           = "AWS/SQS"
  period              = 300
  statistic           = "Maximum"
  threshold           = 0
  treat_missing_data  = "notBreaching"

  dimensions = {
    QueueName = aws_sqs_queue.email_cleanup_dlq.name
  }
}

resource "aws_cloudwatch_log_group" "account_init" {
  name              = "/aws/lambda/${local.name_prefix}-account-init"
  retention_in_days = var.log_retention_days
}

# CloudWatch alarm for account-events DLQ depth
resource "aws_cloudwatch_metric_alarm" "account_events_dlq" {
  alarm_name          = "${local.name_prefix}-account-events-dlq-depth"
  alarm_description   = "Account events DLQ has messages - investigate failed account initialization"
  comparison_operator = "GreaterThanThreshold"
  evaluation_periods  = 1
  metric_name         = "ApproximateNumberOfMessagesVisible"
  namespace           = "AWS/SQS"
  period              = 300
  statistic           = "Maximum"
  threshold           = 0
  treat_missing_data  = "notBreaching"

  dimensions = {
    QueueName = aws_sqs_queue.account_events_dlq.name
  }
}

resource "aws_cloudwatch_log_group" "email_index" {
  name              = "/aws/lambda/${local.name_prefix}-email-index"
  retention_in_days = var.log_retention_days
}

resource "aws_cloudwatch_log_group" "searchsnippet_get" {
  name              = "/aws/lambda/${local.name_prefix}-searchsnippet-get"
  retention_in_days = var.log_retention_days
}

# Log metric filter + alarm for summary generation failures
resource "aws_cloudwatch_log_metric_filter" "summary_generation_failures" {
  name           = "${local.name_prefix}-summary-generation-failures"
  log_group_name = aws_cloudwatch_log_group.email_index.name
  pattern        = "\"Failed to generate summary\""

  metric_transformation {
    name      = "SummaryGenerationFailures"
    namespace = local.name_prefix
    value     = "1"
  }
}

resource "aws_cloudwatch_metric_alarm" "summary_generation_failures" {
  alarm_name          = "${local.name_prefix}-summary-generation-failures"
  alarm_description   = "Email summary generation is failing - check Bedrock access/permissions"
  comparison_operator = "GreaterThanThreshold"
  evaluation_periods  = 1
  metric_name         = "SummaryGenerationFailures"
  namespace           = local.name_prefix
  period              = 300
  statistic           = "Sum"
  threshold           = 0
  treat_missing_data  = "notBreaching"
}

# CloudWatch alarm for search-index DLQ depth
resource "aws_cloudwatch_metric_alarm" "search_index_dlq" {
  alarm_name          = "${local.name_prefix}-search-index-dlq-depth"
  alarm_description   = "Search index DLQ has messages - investigate failed search indexing"
  comparison_operator = "GreaterThanThreshold"
  evaluation_periods  = 1
  metric_name         = "ApproximateNumberOfMessagesVisible"
  namespace           = "AWS/SQS"
  period              = 300
  statistic           = "Maximum"
  threshold           = 0
  treat_missing_data  = "notBreaching"

  dimensions = {
    QueueName = aws_sqs_queue.search_index_dlq.name
  }
}
