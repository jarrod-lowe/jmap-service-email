# Discover core infrastructure via SSM Parameter Store
# See: jmap-service-core/docs/plugin-interface.md

locals {
  ssm_prefix = "/jmap-service-core/${var.environment}"
}

data "aws_ssm_parameter" "jmap_table_name" {
  name = "${local.ssm_prefix}/dynamodb-table-name"
}

data "aws_ssm_parameter" "jmap_table_arn" {
  name = "${local.ssm_prefix}/dynamodb-table-arn"
}

data "aws_ssm_parameter" "jmap_api_url" {
  name = "${local.ssm_prefix}/api-url"
}

data "aws_ssm_parameter" "jmap_api_gateway_execution_arn" {
  name = "${local.ssm_prefix}/api-gateway-execution-arn"
}
