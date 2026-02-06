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

# Policy document for invoking core API Gateway (IAM-authenticated blob endpoints)
data "aws_iam_policy_document" "api_gateway_invoke" {
  statement {
    effect  = "Allow"
    actions = ["execute-api:Invoke"]
    resources = [
      "${data.aws_ssm_parameter.jmap_api_gateway_execution_arn.value}/*/GET/download-iam/*",
      "${data.aws_ssm_parameter.jmap_api_gateway_execution_arn.value}/*/DELETE/delete-iam/*",
      "${data.aws_ssm_parameter.jmap_api_gateway_execution_arn.value}/*/POST/upload-iam/*",
      "${data.aws_ssm_parameter.jmap_api_gateway_execution_arn.value}/*/POST/jmap-iam/*",
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

# Consolidated SQS queue policy (all queues in one policy to stay under IAM limit)
data "aws_iam_policy_document" "sqs_queues" {
  # Send messages to queues that receive work items
  statement {
    effect = "Allow"
    actions = [
      "sqs:SendMessage"
    ]
    resources = [
      aws_sqs_queue.blob_delete.arn,
      aws_sqs_queue.mailbox_cleanup.arn,
      aws_sqs_queue.search_index.arn,
    ]
  }

  # Consume messages from all queues
  statement {
    effect = "Allow"
    actions = [
      "sqs:ReceiveMessage",
      "sqs:DeleteMessage",
      "sqs:GetQueueAttributes"
    ]
    resources = [
      aws_sqs_queue.blob_delete.arn,
      aws_sqs_queue.mailbox_cleanup.arn,
      aws_sqs_queue.account_events.arn,
      aws_sqs_queue.search_index.arn,
    ]
  }

  # Allow sending to DLQ on search-index event source mapping failures
  statement {
    effect = "Allow"
    actions = [
      "sqs:SendMessage"
    ]
    resources = [aws_sqs_queue.search_index_dlq.arn]
  }
}

resource "aws_iam_policy" "sqs_queues" {
  name        = "${local.name_prefix}-sqs-queues"
  description = "Allow SQS operations on all queues"
  policy      = data.aws_iam_policy_document.sqs_queues.json
}

resource "aws_iam_role_policy_attachment" "sqs_queues" {
  role       = aws_iam_role.lambda_execution.name
  policy_arn = aws_iam_policy.sqs_queues.arn
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

# Policy for Bedrock Titan Embeddings v2
data "aws_iam_policy_document" "bedrock_embeddings" {
  statement {
    effect = "Allow"
    actions = [
      "bedrock:InvokeModel"
    ]
    resources = [
      "arn:aws:bedrock:${data.aws_region.current.id}::foundation-model/amazon.titan-embed-text-v2:0"
    ]
  }
}

resource "aws_iam_policy" "bedrock_embeddings" {
  name        = "${local.name_prefix}-bedrock-embeddings"
  description = "Allow invoking Bedrock Titan Embeddings v2 model"
  policy      = data.aws_iam_policy_document.bedrock_embeddings.json
}

resource "aws_iam_role_policy_attachment" "bedrock_embeddings" {
  role       = aws_iam_role.lambda_execution.name
  policy_arn = aws_iam_policy.bedrock_embeddings.arn
}

# Policy for S3 Vectors operations
data "aws_iam_policy_document" "s3vectors_search" {
  statement {
    effect = "Allow"
    actions = [
      "s3vectors:CreateIndex",
      "s3vectors:TagResource",
      "s3vectors:PutVectors",
      "s3vectors:DeleteVectors",
      "s3vectors:GetVectors",
      "s3vectors:QueryVectors",
      "s3vectors:ListIndexes"
    ]
    resources = [
      aws_s3vectors_vector_bucket.search_vectors.vector_bucket_arn,
      "${aws_s3vectors_vector_bucket.search_vectors.vector_bucket_arn}/*"
    ]
  }
}

resource "aws_iam_policy" "s3vectors_search" {
  name        = "${local.name_prefix}-s3vectors-search"
  description = "Allow S3 Vectors operations on search vectors bucket"
  policy      = data.aws_iam_policy_document.s3vectors_search.json
}

resource "aws_iam_role_policy_attachment" "s3vectors_search" {
  role       = aws_iam_role.lambda_execution.name
  policy_arn = aws_iam_policy.s3vectors_search.arn
}
