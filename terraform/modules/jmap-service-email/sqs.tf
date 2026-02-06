# SQS queue for async blob deletion

resource "aws_sqs_queue" "blob_delete_dlq" {
  name                      = "${local.name_prefix}-blob-delete-dlq"
  message_retention_seconds = 1209600 # 14 days
}

resource "aws_sqs_queue" "blob_delete" {
  name                       = "${local.name_prefix}-blob-delete"
  visibility_timeout_seconds = 60
  message_retention_seconds  = 86400 # 1 day

  redrive_policy = jsonencode({
    deadLetterTargetArn = aws_sqs_queue.blob_delete_dlq.arn
    maxReceiveCount     = 3
  })
}

# SQS queue for async mailbox email cleanup

resource "aws_sqs_queue" "mailbox_cleanup_dlq" {
  name                      = "${local.name_prefix}-mailbox-cleanup-dlq"
  message_retention_seconds = 1209600 # 14 days
}

resource "aws_sqs_queue" "mailbox_cleanup" {
  name                       = "${local.name_prefix}-mailbox-cleanup"
  visibility_timeout_seconds = 300
  message_retention_seconds  = 86400 # 1 day

  redrive_policy = jsonencode({
    deadLetterTargetArn = aws_sqs_queue.mailbox_cleanup_dlq.arn
    maxReceiveCount     = 3
  })
}

# SQS DLQ for DynamoDB Streams email cleanup failures

resource "aws_sqs_queue" "email_cleanup_dlq" {
  name                      = "${local.name_prefix}-email-cleanup-dlq"
  message_retention_seconds = 1209600 # 14 days
}

# SQS queue for account events from jmap-service-core

resource "aws_sqs_queue" "account_events_dlq" {
  name                      = "${local.name_prefix}-account-events-dlq"
  message_retention_seconds = 1209600 # 14 days
}

resource "aws_sqs_queue" "account_events" {
  name                       = "${local.name_prefix}-account-events"
  visibility_timeout_seconds = 60
  message_retention_seconds  = 86400 # 1 day

  redrive_policy = jsonencode({
    deadLetterTargetArn = aws_sqs_queue.account_events_dlq.arn
    maxReceiveCount     = 3
  })
}

# Queue policy allowing jmap-service-core's account-init role to send messages
resource "aws_sqs_queue_policy" "account_events" {
  queue_url = aws_sqs_queue.account_events.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid    = "AllowCoreAccountInitSend"
        Effect = "Allow"
        Principal = {
          AWS = data.aws_ssm_parameter.account_init_role_arn.value
        }
        Action   = "sqs:SendMessage"
        Resource = aws_sqs_queue.account_events.arn
      }
    ]
  })
}

# SQS queue for async search indexing

resource "aws_sqs_queue" "search_index_dlq" {
  name                      = "${local.name_prefix}-search-index-dlq"
  message_retention_seconds = 1209600 # 14 days
}

resource "aws_sqs_queue" "search_index" {
  name                       = "${local.name_prefix}-search-index"
  visibility_timeout_seconds = 300
  message_retention_seconds  = 86400 # 1 day

  redrive_policy = jsonencode({
    deadLetterTargetArn = aws_sqs_queue.search_index_dlq.arn
    maxReceiveCount     = 3
  })
}
