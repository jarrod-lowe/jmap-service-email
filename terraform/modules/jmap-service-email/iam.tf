# IAM roles and policies for Lambda execution

# Lambda execution role
resource "aws_iam_role" "lambda_execution" {
  name = "${local.name_prefix}-lambda-execution"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = "sts:AssumeRole"
        Effect = "Allow"
        Principal = {
          Service = "lambda.amazonaws.com"
        }
      }
    ]
  })
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

# Policy for writing plugin registration to core DynamoDB table
resource "aws_iam_policy" "dynamodb_plugin_write" {
  name        = "${local.name_prefix}-dynamodb-plugin-write"
  description = "Allow writing plugin registration to core DynamoDB table"

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "dynamodb:PutItem",
          "dynamodb:UpdateItem",
          "dynamodb:DeleteItem"
        ]
        Resource = data.aws_dynamodb_table.jmap_core.arn
        Condition = {
          "ForAllValues:StringLike" = {
            "dynamodb:LeadingKeys" = ["PLUGIN#"]
          }
        }
      }
    ]
  })
}

resource "aws_iam_role_policy_attachment" "dynamodb_plugin_write" {
  role       = aws_iam_role.lambda_execution.name
  policy_arn = aws_iam_policy.dynamodb_plugin_write.arn
}

# Policy for email data table operations
resource "aws_iam_policy" "dynamodb_email_data" {
  name        = "${local.name_prefix}-dynamodb-email-data"
  description = "Allow full access to email data DynamoDB table"

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
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
        Resource = [
          aws_dynamodb_table.email_data.arn,
          "${aws_dynamodb_table.email_data.arn}/index/*"
        ]
      }
    ]
  })
}

resource "aws_iam_role_policy_attachment" "dynamodb_email_data" {
  role       = aws_iam_role.lambda_execution.name
  policy_arn = aws_iam_policy.dynamodb_email_data.arn
}

# Policy for invoking core API Gateway (IAM-authenticated download endpoint)
resource "aws_iam_policy" "api_gateway_invoke" {
  name        = "${local.name_prefix}-api-gateway-invoke"
  description = "Allow invoking core API Gateway IAM-authenticated endpoints"

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect   = "Allow"
        Action   = "execute-api:Invoke"
        Resource = "${var.jmap_core_api_gateway_arn}/*/GET/download-iam/*"
      }
    ]
  })
}

resource "aws_iam_role_policy_attachment" "api_gateway_invoke" {
  role       = aws_iam_role.lambda_execution.name
  policy_arn = aws_iam_policy.api_gateway_invoke.arn
}
