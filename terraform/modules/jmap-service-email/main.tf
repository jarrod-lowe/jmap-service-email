# Main configuration for jmap-service-email module

data "aws_caller_identity" "current" {}

data "aws_region" "current" {}

locals {
  plugin_name = "email"
  name_prefix = "jmap-service-email-${var.environment}"

  # ADOT Lambda layer for OTel instrumentation (AWS-managed, cross-account)
  adot_account_id    = "901920570463"
  adot_layer_name    = "aws-otel-collector-arm64-ver-0-117-0"
  adot_layer_version = "1"
  adot_layer_arn     = "arn:aws:lambda:${data.aws_region.current.id}:${local.adot_account_id}:layer:${local.adot_layer_name}:${local.adot_layer_version}"
}
