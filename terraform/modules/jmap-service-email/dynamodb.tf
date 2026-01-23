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

  point_in_time_recovery {
    enabled = true
  }

  tags = {
    Name        = "jmap-service-email-data-${var.environment}"
    Environment = var.environment
    Service     = "jmap-service-email"
  }
}
