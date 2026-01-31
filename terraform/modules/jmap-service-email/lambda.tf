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
      BLOB_DELETE_QUEUE_URL               = aws_sqs_queue.blob_delete.url
    }
  }

  depends_on = [
    aws_cloudwatch_log_group.email_import,
    aws_iam_role_policy_attachment.lambda_basic,
    aws_iam_role_policy_attachment.lambda_xray,
    aws_iam_role_policy_attachment.dynamodb_email_data,
    aws_iam_role_policy_attachment.api_gateway_invoke,
    aws_iam_role_policy_attachment.sqs_blob_delete
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
      CORE_API_URL                        = data.aws_ssm_parameter.jmap_api_gateway_invoke_url.value
    }
  }

  depends_on = [
    aws_cloudwatch_log_group.email_get,
    aws_iam_role_policy_attachment.lambda_basic,
    aws_iam_role_policy_attachment.lambda_xray,
    aws_iam_role_policy_attachment.dynamodb_email_data,
    aws_iam_role_policy_attachment.api_gateway_invoke
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

# Lambda function for Email/query
resource "aws_lambda_function" "email_query" {
  function_name = "${local.name_prefix}-email-query"
  role          = aws_iam_role.lambda_execution.arn
  handler       = "bootstrap"
  runtime       = "provided.al2023"
  architectures = ["arm64"]
  memory_size   = var.lambda_memory_size
  timeout       = var.lambda_timeout

  filename         = "${path.module}/../../../build/email-query/lambda.zip"
  source_code_hash = filebase64sha256("${path.module}/../../../build/email-query/lambda.zip")

  layers = [local.adot_layer_arn]

  environment {
    variables = {
      OPENTELEMETRY_COLLECTOR_CONFIG_FILE = "/var/task/collector.yaml"
      AWS_LAMBDA_EXEC_WRAPPER             = "/opt/bootstrap"
      EMAIL_TABLE_NAME                    = aws_dynamodb_table.email_data.name
    }
  }

  depends_on = [
    aws_cloudwatch_log_group.email_query,
    aws_iam_role_policy_attachment.lambda_basic,
    aws_iam_role_policy_attachment.lambda_xray,
    aws_iam_role_policy_attachment.dynamodb_email_data
  ]
}

# Permission for jmap-api Lambda to invoke email-query function
resource "aws_lambda_permission" "allow_jmap_core_email_query" {
  statement_id  = "AllowJMAPCoreInvoke"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.email_query.function_name
  principal     = "lambda.amazonaws.com"
  source_arn    = "arn:aws:lambda:${data.aws_region.current.id}:${data.aws_caller_identity.current.account_id}:function:jmap-api-*"
}

# Lambda function for Email/set
resource "aws_lambda_function" "email_set" {
  function_name = "${local.name_prefix}-email-set"
  role          = aws_iam_role.lambda_execution.arn
  handler       = "bootstrap"
  runtime       = "provided.al2023"
  architectures = ["arm64"]
  memory_size   = var.lambda_memory_size
  timeout       = var.lambda_timeout

  filename         = "${path.module}/../../../build/email-set/lambda.zip"
  source_code_hash = filebase64sha256("${path.module}/../../../build/email-set/lambda.zip")

  layers = [local.adot_layer_arn]

  environment {
    variables = {
      OPENTELEMETRY_COLLECTOR_CONFIG_FILE = "/var/task/collector.yaml"
      AWS_LAMBDA_EXEC_WRAPPER             = "/opt/bootstrap"
      EMAIL_TABLE_NAME                    = aws_dynamodb_table.email_data.name
      CORE_API_URL                        = data.aws_ssm_parameter.jmap_api_gateway_invoke_url.value
      BLOB_DELETE_QUEUE_URL               = aws_sqs_queue.blob_delete.url
    }
  }

  depends_on = [
    aws_cloudwatch_log_group.email_set,
    aws_iam_role_policy_attachment.lambda_basic,
    aws_iam_role_policy_attachment.lambda_xray,
    aws_iam_role_policy_attachment.dynamodb_email_data,
    aws_iam_role_policy_attachment.api_gateway_invoke,
    aws_iam_role_policy_attachment.sqs_blob_delete
  ]
}

# Permission for jmap-api Lambda to invoke email-set function
resource "aws_lambda_permission" "allow_jmap_core_email_set" {
  statement_id  = "AllowJMAPCoreInvoke"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.email_set.function_name
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
      MAILBOX_CLEANUP_QUEUE_URL           = aws_sqs_queue.mailbox_cleanup.url
    }
  }

  depends_on = [
    aws_cloudwatch_log_group.mailbox_set,
    aws_iam_role_policy_attachment.lambda_basic,
    aws_iam_role_policy_attachment.lambda_xray,
    aws_iam_role_policy_attachment.dynamodb_email_data,
    aws_iam_role_policy_attachment.sqs_mailbox_cleanup
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

# Lambda function for Thread/get
resource "aws_lambda_function" "thread_get" {
  function_name = "${local.name_prefix}-thread-get"
  role          = aws_iam_role.lambda_execution.arn
  handler       = "bootstrap"
  runtime       = "provided.al2023"
  architectures = ["arm64"]
  memory_size   = var.lambda_memory_size
  timeout       = var.lambda_timeout

  filename         = "${path.module}/../../../build/thread-get/lambda.zip"
  source_code_hash = filebase64sha256("${path.module}/../../../build/thread-get/lambda.zip")

  layers = [local.adot_layer_arn]

  environment {
    variables = {
      OPENTELEMETRY_COLLECTOR_CONFIG_FILE = "/var/task/collector.yaml"
      AWS_LAMBDA_EXEC_WRAPPER             = "/opt/bootstrap"
      EMAIL_TABLE_NAME                    = aws_dynamodb_table.email_data.name
    }
  }

  depends_on = [
    aws_cloudwatch_log_group.thread_get,
    aws_iam_role_policy_attachment.lambda_basic,
    aws_iam_role_policy_attachment.lambda_xray,
    aws_iam_role_policy_attachment.dynamodb_email_data
  ]
}

# Permission for jmap-api Lambda to invoke thread-get function
resource "aws_lambda_permission" "allow_jmap_core_thread_get" {
  statement_id  = "AllowJMAPCoreInvoke"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.thread_get.function_name
  principal     = "lambda.amazonaws.com"
  source_arn    = "arn:aws:lambda:${data.aws_region.current.id}:${data.aws_caller_identity.current.account_id}:function:jmap-api-*"
}

# Lambda function for Email/changes
resource "aws_lambda_function" "email_changes" {
  function_name = "${local.name_prefix}-email-changes"
  role          = aws_iam_role.lambda_execution.arn
  handler       = "bootstrap"
  runtime       = "provided.al2023"
  architectures = ["arm64"]
  memory_size   = var.lambda_memory_size
  timeout       = var.lambda_timeout

  filename         = "${path.module}/../../../build/email-changes/lambda.zip"
  source_code_hash = filebase64sha256("${path.module}/../../../build/email-changes/lambda.zip")

  layers = [local.adot_layer_arn]

  environment {
    variables = {
      OPENTELEMETRY_COLLECTOR_CONFIG_FILE = "/var/task/collector.yaml"
      AWS_LAMBDA_EXEC_WRAPPER             = "/opt/bootstrap"
      EMAIL_TABLE_NAME                    = aws_dynamodb_table.email_data.name
    }
  }

  depends_on = [
    aws_cloudwatch_log_group.email_changes,
    aws_iam_role_policy_attachment.lambda_basic,
    aws_iam_role_policy_attachment.lambda_xray,
    aws_iam_role_policy_attachment.dynamodb_email_data
  ]
}

# Permission for jmap-api Lambda to invoke email-changes function
resource "aws_lambda_permission" "allow_jmap_core_email_changes" {
  statement_id  = "AllowJMAPCoreInvoke"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.email_changes.function_name
  principal     = "lambda.amazonaws.com"
  source_arn    = "arn:aws:lambda:${data.aws_region.current.id}:${data.aws_caller_identity.current.account_id}:function:jmap-api-*"
}

# Lambda function for Mailbox/changes
resource "aws_lambda_function" "mailbox_changes" {
  function_name = "${local.name_prefix}-mailbox-changes"
  role          = aws_iam_role.lambda_execution.arn
  handler       = "bootstrap"
  runtime       = "provided.al2023"
  architectures = ["arm64"]
  memory_size   = var.lambda_memory_size
  timeout       = var.lambda_timeout

  filename         = "${path.module}/../../../build/mailbox-changes/lambda.zip"
  source_code_hash = filebase64sha256("${path.module}/../../../build/mailbox-changes/lambda.zip")

  layers = [local.adot_layer_arn]

  environment {
    variables = {
      OPENTELEMETRY_COLLECTOR_CONFIG_FILE = "/var/task/collector.yaml"
      AWS_LAMBDA_EXEC_WRAPPER             = "/opt/bootstrap"
      EMAIL_TABLE_NAME                    = aws_dynamodb_table.email_data.name
    }
  }

  depends_on = [
    aws_cloudwatch_log_group.mailbox_changes,
    aws_iam_role_policy_attachment.lambda_basic,
    aws_iam_role_policy_attachment.lambda_xray,
    aws_iam_role_policy_attachment.dynamodb_email_data
  ]
}

# Permission for jmap-api Lambda to invoke mailbox-changes function
resource "aws_lambda_permission" "allow_jmap_core_mailbox_changes" {
  statement_id  = "AllowJMAPCoreInvoke"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.mailbox_changes.function_name
  principal     = "lambda.amazonaws.com"
  source_arn    = "arn:aws:lambda:${data.aws_region.current.id}:${data.aws_caller_identity.current.account_id}:function:jmap-api-*"
}

# Lambda function for Thread/changes
resource "aws_lambda_function" "thread_changes" {
  function_name = "${local.name_prefix}-thread-changes"
  role          = aws_iam_role.lambda_execution.arn
  handler       = "bootstrap"
  runtime       = "provided.al2023"
  architectures = ["arm64"]
  memory_size   = var.lambda_memory_size
  timeout       = var.lambda_timeout

  filename         = "${path.module}/../../../build/thread-changes/lambda.zip"
  source_code_hash = filebase64sha256("${path.module}/../../../build/thread-changes/lambda.zip")

  layers = [local.adot_layer_arn]

  environment {
    variables = {
      OPENTELEMETRY_COLLECTOR_CONFIG_FILE = "/var/task/collector.yaml"
      AWS_LAMBDA_EXEC_WRAPPER             = "/opt/bootstrap"
      EMAIL_TABLE_NAME                    = aws_dynamodb_table.email_data.name
    }
  }

  depends_on = [
    aws_cloudwatch_log_group.thread_changes,
    aws_iam_role_policy_attachment.lambda_basic,
    aws_iam_role_policy_attachment.lambda_xray,
    aws_iam_role_policy_attachment.dynamodb_email_data
  ]
}

# Permission for jmap-api Lambda to invoke thread-changes function
resource "aws_lambda_permission" "allow_jmap_core_thread_changes" {
  statement_id  = "AllowJMAPCoreInvoke"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.thread_changes.function_name
  principal     = "lambda.amazonaws.com"
  source_arn    = "arn:aws:lambda:${data.aws_region.current.id}:${data.aws_caller_identity.current.account_id}:function:jmap-api-*"
}

# Lambda function for blob-delete (SQS consumer)
resource "aws_lambda_function" "blob_delete" {
  function_name = "${local.name_prefix}-blob-delete"
  role          = aws_iam_role.lambda_execution.arn
  handler       = "bootstrap"
  runtime       = "provided.al2023"
  architectures = ["arm64"]
  memory_size   = var.lambda_memory_size
  timeout       = 60

  filename         = "${path.module}/../../../build/blob-delete/lambda.zip"
  source_code_hash = filebase64sha256("${path.module}/../../../build/blob-delete/lambda.zip")

  layers = [local.adot_layer_arn]

  environment {
    variables = {
      OPENTELEMETRY_COLLECTOR_CONFIG_FILE = "/var/task/collector.yaml"
      AWS_LAMBDA_EXEC_WRAPPER             = "/opt/bootstrap"
      CORE_API_URL                        = data.aws_ssm_parameter.jmap_api_gateway_invoke_url.value
    }
  }

  depends_on = [
    aws_cloudwatch_log_group.blob_delete,
    aws_iam_role_policy_attachment.lambda_basic,
    aws_iam_role_policy_attachment.lambda_xray,
    aws_iam_role_policy_attachment.api_gateway_invoke,
    aws_iam_role_policy_attachment.sqs_blob_delete
  ]
}

# SQS event source mapping for blob-delete Lambda
resource "aws_lambda_event_source_mapping" "blob_delete_sqs" {
  event_source_arn                   = aws_sqs_queue.blob_delete.arn
  function_name                      = aws_lambda_function.blob_delete.arn
  batch_size                         = 10
  function_response_types            = ["ReportBatchItemFailures"]
  maximum_batching_window_in_seconds = 5
}

# Lambda function for mailbox-cleanup (SQS consumer)
resource "aws_lambda_function" "mailbox_cleanup" {
  function_name = "${local.name_prefix}-mailbox-cleanup"
  role          = aws_iam_role.lambda_execution.arn
  handler       = "bootstrap"
  runtime       = "provided.al2023"
  architectures = ["arm64"]
  memory_size   = var.lambda_memory_size
  timeout       = 300

  filename         = "${path.module}/../../../build/mailbox-cleanup/lambda.zip"
  source_code_hash = filebase64sha256("${path.module}/../../../build/mailbox-cleanup/lambda.zip")

  layers = [local.adot_layer_arn]

  environment {
    variables = {
      OPENTELEMETRY_COLLECTOR_CONFIG_FILE = "/var/task/collector.yaml"
      AWS_LAMBDA_EXEC_WRAPPER             = "/opt/bootstrap"
      EMAIL_TABLE_NAME                    = aws_dynamodb_table.email_data.name
      BLOB_DELETE_QUEUE_URL               = aws_sqs_queue.blob_delete.url
    }
  }

  depends_on = [
    aws_cloudwatch_log_group.mailbox_cleanup,
    aws_iam_role_policy_attachment.lambda_basic,
    aws_iam_role_policy_attachment.lambda_xray,
    aws_iam_role_policy_attachment.dynamodb_email_data,
    aws_iam_role_policy_attachment.sqs_blob_delete,
    aws_iam_role_policy_attachment.sqs_mailbox_cleanup
  ]
}

# SQS event source mapping for mailbox-cleanup Lambda
resource "aws_lambda_event_source_mapping" "mailbox_cleanup_sqs" {
  event_source_arn                   = aws_sqs_queue.mailbox_cleanup.arn
  function_name                      = aws_lambda_function.mailbox_cleanup.arn
  batch_size                         = 1
  function_response_types            = ["ReportBatchItemFailures"]
  maximum_batching_window_in_seconds = 0
}

# Email Cleanup Lambda — DynamoDB Streams consumer for soft-deleted email cleanup
resource "aws_lambda_function" "email_cleanup" {
  function_name = "${local.name_prefix}-email-cleanup"
  role          = aws_iam_role.lambda_execution.arn
  handler       = "bootstrap"
  runtime       = "provided.al2023"
  architectures = ["arm64"]
  memory_size   = var.lambda_memory_size
  timeout       = 60

  filename         = "${path.module}/../../../build/email-cleanup/lambda.zip"
  source_code_hash = filebase64sha256("${path.module}/../../../build/email-cleanup/lambda.zip")

  layers = [local.adot_layer_arn]

  environment {
    variables = {
      OPENTELEMETRY_COLLECTOR_CONFIG_FILE = "/var/task/collector.yaml"
      AWS_LAMBDA_EXEC_WRAPPER             = "/opt/bootstrap"
      EMAIL_TABLE_NAME                    = aws_dynamodb_table.email_data.name
      BLOB_DELETE_QUEUE_URL               = aws_sqs_queue.blob_delete.url
    }
  }

  depends_on = [
    aws_cloudwatch_log_group.email_cleanup,
    aws_iam_role_policy_attachment.lambda_basic,
    aws_iam_role_policy_attachment.lambda_xray,
    aws_iam_role_policy_attachment.dynamodb_email_data,
    aws_iam_role_policy_attachment.sqs_blob_delete,
    aws_iam_role_policy_attachment.dynamodb_stream_email_data
  ]
}

resource "aws_lambda_event_source_mapping" "email_cleanup_stream" {
  event_source_arn  = aws_dynamodb_table.email_data.stream_arn
  function_name     = aws_lambda_function.email_cleanup.arn
  starting_position = "LATEST"
  batch_size        = 10

  maximum_batching_window_in_seconds = 5
  maximum_retry_attempts             = 3
  bisect_batch_on_function_error     = true

  destination_config {
    on_failure {
      destination_arn = aws_sqs_queue.email_cleanup_dlq.arn
    }
  }

  filter_criteria {
    filter {
      pattern = jsonencode({
        eventName = ["MODIFY"]
        dynamodb = {
          NewImage = {
            deletedAt = { S = [{ exists = true }] }
          }
          OldImage = {
            deletedAt = [{ exists = false }]
          }
        }
      })
    }
  }
}

# Lambda function for account-init (SQS consumer for account.created events)
resource "aws_lambda_function" "account_init" {
  function_name = "${local.name_prefix}-account-init"
  role          = aws_iam_role.lambda_execution.arn
  handler       = "bootstrap"
  runtime       = "provided.al2023"
  architectures = ["arm64"]
  memory_size   = var.lambda_memory_size
  timeout       = 60

  filename         = "${path.module}/../../../build/account-init/lambda.zip"
  source_code_hash = filebase64sha256("${path.module}/../../../build/account-init/lambda.zip")

  layers = [local.adot_layer_arn]

  environment {
    variables = {
      OPENTELEMETRY_COLLECTOR_CONFIG_FILE = "/var/task/collector.yaml"
      AWS_LAMBDA_EXEC_WRAPPER             = "/opt/bootstrap"
      EMAIL_TABLE_NAME                    = aws_dynamodb_table.email_data.name
    }
  }

  depends_on = [
    aws_cloudwatch_log_group.account_init,
    aws_iam_role_policy_attachment.lambda_basic,
    aws_iam_role_policy_attachment.lambda_xray,
    aws_iam_role_policy_attachment.dynamodb_email_data,
    aws_iam_role_policy_attachment.sqs_account_events
  ]
}

# SQS event source mapping for account-init Lambda
resource "aws_lambda_event_source_mapping" "account_init_sqs" {
  event_source_arn                   = aws_sqs_queue.account_events.arn
  function_name                      = aws_lambda_function.account_init.arn
  batch_size                         = 10
  function_response_types            = ["ReportBatchItemFailures"]
  maximum_batching_window_in_seconds = 5
}
