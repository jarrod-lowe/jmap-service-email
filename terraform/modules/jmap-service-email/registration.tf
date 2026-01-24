# Plugin registration in jmap-service-core DynamoDB table

resource "time_static" "plugin_registered" {}

resource "aws_dynamodb_table_item" "plugin_registration" {
  table_name = data.aws_ssm_parameter.jmap_table_name.value
  hash_key   = "pk"
  range_key  = "sk"

  item = jsonencode({
    pk       = { S = "PLUGIN#" }
    sk       = { S = "PLUGIN#${local.plugin_name}" }
    pluginId = { S = local.plugin_name }
    capabilities = {
      M = {
        "urn:ietf:params:jmap:mail" = {
          M = {
            maxMailboxesPerEmail = { NULL = true }
            maxMailboxDepth      = { N = "10" }
          }
        }
      }
    }
    methods = {
      M = {
        "Email/get" = {
          M = {
            invocationType = { S = "lambda-invoke" }
            invokeTarget   = { S = aws_lambda_function.email_get.arn }
          }
        }
        "Email/query" = {
          M = {
            invocationType = { S = "lambda-invoke" }
            invokeTarget   = { S = aws_lambda_function.email_query.arn }
          }
        }
        "Email/import" = {
          M = {
            invocationType = { S = "lambda-invoke" }
            invokeTarget   = { S = aws_lambda_function.email_import.arn }
          }
        }
        "Email/set" = {
          M = {
            invocationType = { S = "lambda-invoke" }
            invokeTarget   = { S = aws_lambda_function.email_set.arn }
          }
        }
        "Mailbox/get" = {
          M = {
            invocationType = { S = "lambda-invoke" }
            invokeTarget   = { S = aws_lambda_function.mailbox_get.arn }
          }
        }
        "Mailbox/set" = {
          M = {
            invocationType = { S = "lambda-invoke" }
            invokeTarget   = { S = aws_lambda_function.mailbox_set.arn }
          }
        }
        "Thread/get" = {
          M = {
            invocationType = { S = "lambda-invoke" }
            invokeTarget   = { S = aws_lambda_function.thread_get.arn }
          }
        }
        "Email/changes" = {
          M = {
            invocationType = { S = "lambda-invoke" }
            invokeTarget   = { S = aws_lambda_function.email_changes.arn }
          }
        }
        "Mailbox/changes" = {
          M = {
            invocationType = { S = "lambda-invoke" }
            invokeTarget   = { S = aws_lambda_function.mailbox_changes.arn }
          }
        }
        "Thread/changes" = {
          M = {
            invocationType = { S = "lambda-invoke" }
            invokeTarget   = { S = aws_lambda_function.thread_changes.arn }
          }
        }
      }
    }
    registeredAt = { S = time_static.plugin_registered.rfc3339 }
    version      = { S = var.plugin_version }
  })
}
