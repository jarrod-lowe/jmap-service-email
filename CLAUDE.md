# JMAP Service Email Plugin - Development Guide

## Overview

This is a plugin for jmap-service-core that provides email capabilities (urn:ietf:params:jmap:mail). Unlike the core service, plugins are **not exposed via API Gateway** - they are invoked directly by the core's jmap-api Lambda.

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                        jmap-service-core                         │
│  ┌─────────────┐    ┌──────────────┐    ┌──────────────────┐   │
│  │ API Gateway │───>│   jmap-api   │───>│ Plugin Registry  │   │
│  └─────────────┘    │   Lambda     │    │   (DynamoDB)     │   │
│                     └──────┬───────┘    └──────────────────┘   │
└────────────────────────────┼───────────────────────────────────┘
                             │ Lambda Invoke
                             ▼
┌─────────────────────────────────────────────────────────────────┐
│                      jmap-service-email                          │
│  ┌──────────────────┐                                           │
│  │  placeholder     │  (returns serverFail - not implemented)   │
│  │  Lambda          │                                           │
│  └──────────────────┘                                           │
└─────────────────────────────────────────────────────────────────┘
```

## Plugin Invocation Contract

The core service invokes plugins with this JSON payload:

```json
{
  "requestId": "apigw-request-id",
  "callIndex": 0,
  "accountId": "user-123",
  "method": "Email/get",
  "args": { "ids": ["email-1"], "properties": ["id", "subject"] },
  "clientId": "c0"
}
```

Plugins respond with:

```json
{
  "methodResponse": {
    "name": "Email/get",
    "args": { "accountId": "user-123", "list": [...], "notFound": [] },
    "clientId": "c0"
  }
}
```

For errors:

```json
{
  "methodResponse": {
    "name": "error",
    "args": { "type": "serverFail", "description": "Not implemented" },
    "clientId": "c0"
  }
}
```

See jmap-service-core/docs/plugin-interface.md for the complete contract.

## Build Commands

```bash
make deps      # Initialize go.mod and fetch dependencies
make lint      # Run golangci-lint (required before commits)
make test      # Run Go unit tests
make build     # Compile all lambdas (linux/arm64)
make package   # Create Lambda deployment packages
```

## Terraform Commands

```bash
# Get core outputs first
cd ../jmap-service-core && AWS_PROFILE=ses-mail make outputs ENV=test

# Configure terraform.tfvars with jmap_core_table_name and jmap_core_table_arn

make init ENV=test      # Initialize Terraform
make plan ENV=test      # Create plan
make apply ENV=test     # Apply changes
make outputs ENV=test   # Show outputs
```

## Development Requirements

1. **TDD** - Write tests first, then implementation
2. **ARM64** - All Lambdas must be ARM64 (Graviton)
3. **Structured Logging** - Use slog with JSON handler
4. **OTel Tracing** - All Lambdas instrumented with X-Ray via ADOT layer
5. **Lint** - All code must pass golangci-lint

## Current Status

The repository contains infrastructure setup only:
- Placeholder Lambda that returns `serverFail` for all methods
- Plugin registration in core DynamoDB
- No actual email implementation yet

## Project Structure

```
jmap-service-email/
├── cmd/
│   └── placeholder/           # Placeholder Lambda (to be replaced)
│       └── main.go
├── terraform/
│   ├── modules/
│   │   └── jmap-service-email/
│   │       ├── main.tf
│   │       ├── variables.tf
│   │       ├── outputs.tf
│   │       ├── lambda.tf
│   │       ├── iam.tf
│   │       ├── cloudwatch.tf
│   │       ├── registration.tf
│   │       └── tfvars-backup.tf
│   └── environments/
│       ├── _shared/           # Shared terraform configs
│       ├── test/              # Test environment (symlinks)
│       └── prod/              # Prod environment (symlinks)
├── Makefile
├── collector.yaml             # ADOT config
├── CLAUDE.md                  # This file
└── README.md
```

## AWS Profile

All AWS commands use `AWS_PROFILE=ses-mail`. Ensure this profile is configured.
