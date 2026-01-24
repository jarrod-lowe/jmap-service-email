# JMAP Service Email Plugin - Development Guide

## Overview

This is a plugin for jmap-service-core that provides email capabilities (urn:ietf:params:jmap:mail). Unlike the core service, plugins are **not exposed via API Gateway** - they are invoked directly by the core's jmap-api Lambda.

## Reference Specifications

- [RFC 8621 - JMAP for Mail](docs/rfc8621-jmap-mail.txt) - The JSON Meta Application Protocol for Mail specification that this plugin implements

## Architecture

```plain
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
│  │  Method-specific │  (Email/*, Mailbox/*, Thread/* Lambdas)   │
│  │  Lambdas         │                                           │
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

jmap-service-core can usually be found in ../jmap-service-core. If changes are needed in jmap-service-core, provide a prompt for the user to give to an agent in the jmap-service-core repo; do not try to make changes there yourself.

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
# Ensure jmap-service-core is deployed first (provides SSM parameters)
make init ENV=test      # Initialize Terraform
make plan ENV=test      # Create plan
make apply ENV=test     # Apply changes
make outputs ENV=test   # Show outputs
```

Core infrastructure values (DynamoDB table, API URL, etc.) are automatically discovered via SSM Parameter Store from jmap-service-core.

## Development Requirements

1. **TDD** - Write tests first, then implementation. Use dependency inversion
2. **ARM64** - All Lambdas must be ARM64 (Graviton)
3. **Structured Logging** - Use slog with JSON handler
4. **OTel Tracing** - All Lambdas instrumented with X-Ray via ADOT layer
5. **Lint** - All code must pass golangci-lint

**ALWAYS** write into any plans to write or modify code that you must use the TDD superpower, **AND** that RED tests must SUCCESSFULLY fail (that is, compile, run, not panic, and return a failure).

## Current Status

The repository implements the following JMAP methods:

- `Email/get`, `Email/query`, `Email/set`, `Email/import`, `Email/changes`
- `Mailbox/get`, `Mailbox/set`, `Mailbox/changes`
- `Thread/get`, `Thread/changes`

See README.md for detailed capability documentation.

## Project Structure

```plain
jmap-service-email/
├── cmd/
│   ├── email-import/          # Email/import Lambda
│   ├── email-get/             # Email/get Lambda
│   ├── email-query/           # Email/query Lambda
│   ├── email-set/             # Email/set Lambda
│   ├── email-changes/         # Email/changes Lambda
│   ├── mailbox-get/           # Mailbox/get Lambda
│   ├── mailbox-set/           # Mailbox/set Lambda
│   ├── mailbox-changes/       # Mailbox/changes Lambda
│   ├── thread-get/            # Thread/get Lambda
│   └── thread-changes/        # Thread/changes Lambda
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
│   │       ├── ssm_discovery.tf   # Discovers core values via SSM
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
