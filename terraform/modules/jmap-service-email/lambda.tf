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

# Permission for jmap-api Lambda to invoke placeholder function
resource "aws_lambda_permission" "allow_jmap_core_placeholder" {
  statement_id  = "AllowJMAPCoreInvoke"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.placeholder.function_name
  principal     = "lambda.amazonaws.com"
  source_arn    = "arn:aws:lambda:${data.aws_region.current.id}:${data.aws_caller_identity.current.account_id}:function:jmap-api-*"
}

# Lambda function for Email/import
resource "aws_lambda_function" "email_import" {
  function_name = "${local.name_prefix}-email-import"
  role          = aws_iam_role.lambda_execution.arn
  handler       = "bootstrap"
  runtime       = "provided.al2023"
  architectures = ["arm64"]
  memory_size   = var.lambda_memory_size
  timeout       = var.lambda_timeout

  filename         = "${path.module}/../../../build/email-import/lambda.zip"
  source_code_hash = filebase64sha256("${path.module}/../../../build/email-import/lambda.zip")

  layers = [local.adot_layer_arn]

  environment {
    variables = {
      OPENTELEMETRY_COLLECTOR_CONFIG_FILE = "/var/task/collector.yaml"
      AWS_LAMBDA_EXEC_WRAPPER             = "/opt/bootstrap"
      EMAIL_TABLE_NAME                    = aws_dynamodb_table.email_data.name
      CORE_API_URL                        = data.aws_ssm_parameter.jmap_api_gateway_invoke_url.value
    }
  }

  depends_on = [
    aws_cloudwatch_log_group.email_import,
    aws_iam_role_policy_attachment.lambda_basic,
    aws_iam_role_policy_attachment.lambda_xray,
    aws_iam_role_policy_attachment.dynamodb_email_data,
    aws_iam_role_policy_attachment.api_gateway_invoke
  ]
}

# Permission for jmap-api Lambda to invoke email-import function
resource "aws_lambda_permission" "allow_jmap_core_email_import" {
  statement_id  = "AllowJMAPCoreInvoke"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.email_import.function_name
  principal     = "lambda.amazonaws.com"
  source_arn    = "arn:aws:lambda:${data.aws_region.current.id}:${data.aws_caller_identity.current.account_id}:function:jmap-api-*"
}

# Lambda function for Email/get
resource "aws_lambda_function" "email_get" {
  function_name = "${local.name_prefix}-email-get"
  role          = aws_iam_role.lambda_execution.arn
  handler       = "bootstrap"
  runtime       = "provided.al2023"
  architectures = ["arm64"]
  memory_size   = var.lambda_memory_size
  timeout       = var.lambda_timeout

  filename         = "${path.module}/../../../build/email-get/lambda.zip"
  source_code_hash = filebase64sha256("${path.module}/../../../build/email-get/lambda.zip")

  layers = [local.adot_layer_arn]

  environment {
    variables = {
      OPENTELEMETRY_COLLECTOR_CONFIG_FILE = "/var/task/collector.yaml"
      AWS_LAMBDA_EXEC_WRAPPER             = "/opt/bootstrap"
      EMAIL_TABLE_NAME                    = aws_dynamodb_table.email_data.name
    }
  }

  depends_on = [
    aws_cloudwatch_log_group.email_get,
    aws_iam_role_policy_attachment.lambda_basic,
    aws_iam_role_policy_attachment.lambda_xray,
    aws_iam_role_policy_attachment.dynamodb_email_data
  ]
}

# Permission for jmap-api Lambda to invoke email-get function
resource "aws_lambda_permission" "allow_jmap_core_email_get" {
  statement_id  = "AllowJMAPCoreInvoke"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.email_get.function_name
  principal     = "lambda.amazonaws.com"
  source_arn    = "arn:aws:lambda:${data.aws_region.current.id}:${data.aws_caller_identity.current.account_id}:function:jmap-api-*"
}

# Lambda function for Mailbox/get
resource "aws_lambda_function" "mailbox_get" {
  function_name = "${local.name_prefix}-mailbox-get"
  role          = aws_iam_role.lambda_execution.arn
  handler       = "bootstrap"
  runtime       = "provided.al2023"
  architectures = ["arm64"]
  memory_size   = var.lambda_memory_size
  timeout       = var.lambda_timeout

  filename         = "${path.module}/../../../build/mailbox-get/lambda.zip"
  source_code_hash = filebase64sha256("${path.module}/../../../build/mailbox-get/lambda.zip")

  layers = [local.adot_layer_arn]

  environment {
    variables = {
      OPENTELEMETRY_COLLECTOR_CONFIG_FILE = "/var/task/collector.yaml"
      AWS_LAMBDA_EXEC_WRAPPER             = "/opt/bootstrap"
      EMAIL_TABLE_NAME                    = aws_dynamodb_table.email_data.name
    }
  }

  depends_on = [
    aws_cloudwatch_log_group.mailbox_get,
    aws_iam_role_policy_attachment.lambda_basic,
    aws_iam_role_policy_attachment.lambda_xray,
    aws_iam_role_policy_attachment.dynamodb_email_data
  ]
}

# Permission for jmap-api Lambda to invoke mailbox-get function
resource "aws_lambda_permission" "allow_jmap_core_mailbox_get" {
  statement_id  = "AllowJMAPCoreInvoke"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.mailbox_get.function_name
  principal     = "lambda.amazonaws.com"
  source_arn    = "arn:aws:lambda:${data.aws_region.current.id}:${data.aws_caller_identity.current.account_id}:function:jmap-api-*"
}

# Lambda function for Mailbox/set
resource "aws_lambda_function" "mailbox_set" {
  function_name = "${local.name_prefix}-mailbox-set"
  role          = aws_iam_role.lambda_execution.arn
  handler       = "bootstrap"
  runtime       = "provided.al2023"
  architectures = ["arm64"]
  memory_size   = var.lambda_memory_size
  timeout       = var.lambda_timeout

  filename         = "${path.module}/../../../build/mailbox-set/lambda.zip"
  source_code_hash = filebase64sha256("${path.module}/../../../build/mailbox-set/lambda.zip")

  layers = [local.adot_layer_arn]

  environment {
    variables = {
      OPENTELEMETRY_COLLECTOR_CONFIG_FILE = "/var/task/collector.yaml"
      AWS_LAMBDA_EXEC_WRAPPER             = "/opt/bootstrap"
      EMAIL_TABLE_NAME                    = aws_dynamodb_table.email_data.name
    }
  }

  depends_on = [
    aws_cloudwatch_log_group.mailbox_set,
    aws_iam_role_policy_attachment.lambda_basic,
    aws_iam_role_policy_attachment.lambda_xray,
    aws_iam_role_policy_attachment.dynamodb_email_data
  ]
}

# Permission for jmap-api Lambda to invoke mailbox-set function
resource "aws_lambda_permission" "allow_jmap_core_mailbox_set" {
  statement_id  = "AllowJMAPCoreInvoke"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.mailbox_set.function_name
  principal     = "lambda.amazonaws.com"
  source_arn    = "arn:aws:lambda:${data.aws_region.current.id}:${data.aws_caller_identity.current.account_id}:function:jmap-api-*"
}
