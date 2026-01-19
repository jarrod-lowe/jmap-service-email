# JMAP Service Email Plugin

Email plugin for [jmap-service-core](https://github.com/jarrod-lowe/jmap-service-core), providing JMAP email capabilities.

## Status

**Infrastructure setup complete** - the plugin registration pipeline is functional but the email implementation is a placeholder that returns `serverFail` for all methods.

## Prerequisites

- Go 1.24+
- Terraform 1.0+
- AWS CLI configured with `ses-mail` profile
- golangci-lint
- jmap-service-core deployed to the target environment

## Quick Start

1. **Get core outputs** (needed for plugin registration):

   ```bash
   cd ../jmap-service-core
   AWS_PROFILE=ses-mail make outputs ENV=test
   ```

2. **Configure terraform.tfvars**:

   ```bash
   cp terraform/environments/_shared/terraform.tfvars.example \
      terraform/environments/test/terraform.tfvars
   # Edit with jmap_core_table_name and jmap_core_table_arn from step 1
   ```

3. **Build and deploy**:

   ```bash
   make deps
   make lint
   make test
   make build
   make package
   make init ENV=test
   make plan ENV=test
   make apply ENV=test
   ```

4. **Verify plugin registration**:

   ```bash
   AWS_PROFILE=ses-mail aws dynamodb get-item \
     --table-name <core-table-name> \
     --key '{"pk":{"S":"PLUGIN#"},"sk":{"S":"PLUGIN#email"}}'
   ```

## Make Targets

| Target | Description |
|--------|-------------|
| `make deps` | Initialize go.mod and fetch dependencies |
| `make build` | Compile all lambdas (linux/arm64) |
| `make package` | Create Lambda deployment packages |
| `make test` | Run Go unit tests |
| `make lint` | Run golangci-lint |
| `make init ENV=<env>` | Initialize Terraform |
| `make plan ENV=<env>` | Create Terraform plan |
| `make apply ENV=<env>` | Apply Terraform changes |
| `make outputs ENV=<env>` | Show Terraform outputs |
| `make clean ENV=<env>` | Clean Terraform files |
| `make clean-all ENV=<env>` | Clean all build artifacts |

## Project Structure

```plain
jmap-service-email/
├── cmd/
│   └── placeholder/           # Placeholder Lambda
├── terraform/
│   ├── modules/
│   │   └── jmap-service-email/
│   └── environments/
│       ├── _shared/
│       ├── test/
│       └── prod/
├── Makefile
├── collector.yaml
├── CLAUDE.md
└── README.md
```

## Capabilities

The plugin registers the following capability:

- `urn:ietf:params:jmap:mail`

Methods (all currently return `serverFail`):

- `Email/get`
- `Email/query`
- `Email/import`

## License

See LICENSE file.
