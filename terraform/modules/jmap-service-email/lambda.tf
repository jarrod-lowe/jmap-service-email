# Lambda function for the placeholder (to be replaced with real email Lambdas)

resource "aws_lambda_function" "placeholder" {
  function_name = "${local.name_prefix}-placeholder"
  role          = aws_iam_role.lambda_execution.arn
  handler       = "bootstrap"
  runtime       = "provided.al2023"
  architectures = ["arm64"]
  memory_size   = var.lambda_memory_size
  timeout       = var.lambda_timeout

  filename         = "${path.module}/../../../build/placeholder/lambda.zip"
  source_code_hash = filebase64sha256("${path.module}/../../../build/placeholder/lambda.zip")

  layers = [local.adot_layer_arn]

  environment {
    variables = {
      OPENTELEMETRY_COLLECTOR_CONFIG_FILE = "/var/task/collector.yaml"
      AWS_LAMBDA_EXEC_WRAPPER             = "/opt/bootstrap"
    }
  }

  depends_on = [
    aws_cloudwatch_log_group.placeholder,
    aws_iam_role_policy_attachment.lambda_basic,
    aws_iam_role_policy_attachment.lambda_xray
  ]
}

# Permission for jmap-api Lambda to invoke this function
resource "aws_lambda_permission" "allow_jmap_core" {
  statement_id  = "AllowJMAPCoreInvoke"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.placeholder.function_name
  principal     = "lambda.amazonaws.com"
  source_arn    = "arn:aws:lambda:${data.aws_region.current.id}:${data.aws_caller_identity.current.account_id}:function:jmap-api-*"
}
