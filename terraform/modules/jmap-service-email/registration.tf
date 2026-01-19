# Plugin registration in jmap-service-core DynamoDB table

resource "aws_dynamodb_table_item" "plugin_registration" {
  table_name = var.jmap_core_table_name
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
            invokeTarget   = { S = aws_lambda_function.placeholder.arn }
          }
        }
        "Email/query" = {
          M = {
            invocationType = { S = "lambda-invoke" }
            invokeTarget   = { S = aws_lambda_function.placeholder.arn }
          }
        }
        "Email/import" = {
          M = {
            invocationType = { S = "lambda-invoke" }
            invokeTarget   = { S = aws_lambda_function.placeholder.arn }
          }
        }
      }
    }
    registeredAt = { S = timestamp() }
    version      = { S = var.plugin_version }
  })

  lifecycle {
    ignore_changes = [item]
  }
}
