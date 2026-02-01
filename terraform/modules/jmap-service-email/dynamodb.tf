# DynamoDB table for email data storage

resource "aws_dynamodb_table" "email_data" {
  name         = "jmap-service-email-data-${var.environment}"
  billing_mode = "PAY_PER_REQUEST"
  hash_key     = "pk"
  range_key    = "sk"

  attribute {
    name = "pk"
    type = "S"
  }

  attribute {
    name = "sk"
    type = "S"
  }

  attribute {
    name = "lsi1sk"
    type = "S"
  }

  attribute {
    name = "lsi2sk"
    type = "S"
  }

  attribute {
    name = "lsi3sk"
    type = "S"
  }

  # LSI1 for querying all emails sorted by receivedAt
  # Format: RCVD#{receivedAt}#{emailId}
  # Includes threadId for collapseThreads support, deletedAt for soft-delete filtering
  local_secondary_index {
    name               = "lsi1"
    range_key          = "lsi1sk"
    projection_type    = "INCLUDE"
    non_key_attributes = ["emailId", "threadId", "deletedAt"]
  }

  # LSI2 for finding emails by Message-ID header (threading parent lookup)
  # Format: MSGID#{messageId}
  local_secondary_index {
    name               = "lsi2"
    range_key          = "lsi2sk"
    projection_type    = "INCLUDE"
    non_key_attributes = ["emailId", "threadId"]
  }

  # LSI3 for querying all emails in a thread sorted by receivedAt
  # Format: THREAD#{threadId}#RCVD#{receivedAt}#{emailId}
  local_secondary_index {
    name               = "lsi3"
    range_key          = "lsi3sk"
    projection_type    = "INCLUDE"
    non_key_attributes = ["emailId", "threadId", "receivedAt"]
  }

  stream_enabled   = true
  stream_view_type = "NEW_AND_OLD_IMAGES"

  point_in_time_recovery {
    enabled = true
  }

  tags = {
    Name        = "jmap-service-email-data-${var.environment}"
    Environment = var.environment
    Service     = "jmap-service-email"
  }
}
