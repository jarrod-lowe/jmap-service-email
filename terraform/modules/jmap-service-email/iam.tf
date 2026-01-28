# IAM roles and policies for Lambda execution

# Lambda assume role policy
data "aws_iam_policy_document" "lambda_assume_role" {
  statement {
    actions = ["sts:AssumeRole"]
    effect  = "Allow"

    principals {
      type        = "Service"
      identifiers = ["lambda.amazonaws.com"]
    }
  }
}

# Lambda execution role
resource "aws_iam_role" "lambda_execution" {
  name               = "${local.name_prefix}-lambda-execution"
  assume_role_policy = data.aws_iam_policy_document.lambda_assume_role.json
}

# Basic Lambda execution policy (CloudWatch Logs)
resource "aws_iam_role_policy_attachment" "lambda_basic" {
  role       = aws_iam_role.lambda_execution.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole"
}

# X-Ray tracing policy
resource "aws_iam_role_policy_attachment" "lambda_xray" {
  role       = aws_iam_role.lambda_execution.name
  policy_arn = "arn:aws:iam::aws:policy/AWSXRayDaemonWriteAccess"
}

# Policy document for writing plugin registration to core DynamoDB table
data "aws_iam_policy_document" "dynamodb_plugin_write" {
  statement {
    effect = "Allow"
    actions = [
      "dynamodb:PutItem",
      "dynamodb:UpdateItem",
      "dynamodb:DeleteItem"
    ]
    resources = [data.aws_ssm_parameter.jmap_table_arn.value]

    condition {
      test     = "ForAllValues:StringLike"
      variable = "dynamodb:LeadingKeys"
      values   = ["PLUGIN#"]
    }
  }
}

resource "aws_iam_policy" "dynamodb_plugin_write" {
  name        = "${local.name_prefix}-dynamodb-plugin-write"
  description = "Allow writing plugin registration to core DynamoDB table"
  policy      = data.aws_iam_policy_document.dynamodb_plugin_write.json
}

resource "aws_iam_role_policy_attachment" "dynamodb_plugin_write" {
  role       = aws_iam_role.lambda_execution.name
  policy_arn = aws_iam_policy.dynamodb_plugin_write.arn
}

# Policy document for email data table operations
data "aws_iam_policy_document" "dynamodb_email_data" {
  statement {
    effect = "Allow"
    actions = [
      "dynamodb:GetItem",
      "dynamodb:PutItem",
      "dynamodb:UpdateItem",
      "dynamodb:DeleteItem",
      "dynamodb:Query",
      "dynamodb:BatchGetItem",
      "dynamodb:BatchWriteItem",
      "dynamodb:TransactWriteItems",
      "dynamodb:TransactGetItems"
    ]
    resources = [
      aws_dynamodb_table.email_data.arn,
      "${aws_dynamodb_table.email_data.arn}/index/*"
    ]
  }
}

resource "aws_iam_policy" "dynamodb_email_data" {
  name        = "${local.name_prefix}-dynamodb-email-data"
  description = "Allow full access to email data DynamoDB table"
  policy      = data.aws_iam_policy_document.dynamodb_email_data.json
}

resource "aws_iam_role_policy_attachment" "dynamodb_email_data" {
  role       = aws_iam_role.lambda_execution.name
  policy_arn = aws_iam_policy.dynamodb_email_data.arn
}

# Policy document for invoking core API Gateway (IAM-authenticated download endpoint)
data "aws_iam_policy_document" "api_gateway_invoke" {
  statement {
    effect  = "Allow"
    actions = ["execute-api:Invoke"]
    resources = [
      "${data.aws_ssm_parameter.jmap_api_gateway_execution_arn.value}/*/GET/download-iam/*",
      "${data.aws_ssm_parameter.jmap_api_gateway_execution_arn.value}/*/DELETE/delete-iam/*",
    ]
  }
}

resource "aws_iam_policy" "api_gateway_invoke" {
  name        = "${local.name_prefix}-api-gateway-invoke"
  description = "Allow invoking core API Gateway IAM-authenticated endpoints"
  policy      = data.aws_iam_policy_document.api_gateway_invoke.json
}

resource "aws_iam_role_policy_attachment" "api_gateway_invoke" {
  role       = aws_iam_role.lambda_execution.name
  policy_arn = aws_iam_policy.api_gateway_invoke.arn
}

# Policy document for SQS blob-delete queue operations
data "aws_iam_policy_document" "sqs_blob_delete" {
  statement {
    effect = "Allow"
    actions = [
      "sqs:SendMessage"
    ]
    resources = [aws_sqs_queue.blob_delete.arn]
  }

  statement {
    effect = "Allow"
    actions = [
      "sqs:ReceiveMessage",
      "sqs:DeleteMessage",
      "sqs:GetQueueAttributes"
    ]
    resources = [aws_sqs_queue.blob_delete.arn]
  }
}

resource "aws_iam_policy" "sqs_blob_delete" {
  name        = "${local.name_prefix}-sqs-blob-delete"
  description = "Allow SQS operations on blob-delete queue"
  policy      = data.aws_iam_policy_document.sqs_blob_delete.json
}

resource "aws_iam_role_policy_attachment" "sqs_blob_delete" {
  role       = aws_iam_role.lambda_execution.name
  policy_arn = aws_iam_policy.sqs_blob_delete.arn
}

# Policy document for SQS mailbox-cleanup queue operations
data "aws_iam_policy_document" "sqs_mailbox_cleanup" {
  statement {
    effect = "Allow"
    actions = [
      "sqs:SendMessage"
    ]
    resources = [aws_sqs_queue.mailbox_cleanup.arn]
  }

  statement {
    effect = "Allow"
    actions = [
      "sqs:ReceiveMessage",
      "sqs:DeleteMessage",
      "sqs:GetQueueAttributes"
    ]
    resources = [aws_sqs_queue.mailbox_cleanup.arn]
  }
}

resource "aws_iam_policy" "sqs_mailbox_cleanup" {
  name        = "${local.name_prefix}-sqs-mailbox-cleanup"
  description = "Allow SQS operations on mailbox-cleanup queue"
  policy      = data.aws_iam_policy_document.sqs_mailbox_cleanup.json
}

resource "aws_iam_role_policy_attachment" "sqs_mailbox_cleanup" {
  role       = aws_iam_role.lambda_execution.name
  policy_arn = aws_iam_policy.sqs_mailbox_cleanup.arn
}

# DynamoDB Streams permissions for email-cleanup Lambda
data "aws_iam_policy_document" "dynamodb_stream_email_data" {
  statement {
    effect = "Allow"
    actions = [
      "dynamodb:GetRecords",
      "dynamodb:GetShardIterator",
      "dynamodb:DescribeStream",
      "dynamodb:ListStreams"
    ]
    resources = ["${aws_dynamodb_table.email_data.arn}/stream/*"]
  }

  # Allow sending to DLQ on stream processing failure
  statement {
    effect = "Allow"
    actions = [
      "sqs:SendMessage"
    ]
    resources = [aws_sqs_queue.email_cleanup_dlq.arn]
  }
}

resource "aws_iam_policy" "dynamodb_stream_email_data" {
  name        = "${local.name_prefix}-dynamodb-stream-email-data"
  description = "Allow DynamoDB Streams operations on email data table"
  policy      = data.aws_iam_policy_document.dynamodb_stream_email_data.json
}

resource "aws_iam_role_policy_attachment" "dynamodb_stream_email_data" {
  role       = aws_iam_role.lambda_execution.name
  policy_arn = aws_iam_policy.dynamodb_stream_email_data.arn
}
