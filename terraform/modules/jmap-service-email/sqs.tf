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
