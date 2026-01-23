# JMAP Service Email Plugin

Email plugin for [jmap-service-core](https://github.com/jarrod-lowe/jmap-service-core), providing JMAP email capabilities.

## Status

**Partial implementation** - `Email/import` and `Email/get` are functional. Other methods return `serverFail`.

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
| ------ | ----------- |
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
│   ├── placeholder/           # Placeholder Lambda
│   ├── email-import/          # Email/import Lambda
│   └── email-get/             # Email/get Lambda
├── internal/
│   ├── email/                 # Email types, repository, parser
│   └── blob/                  # Blob API client
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

Methods:

- `Email/get` - Retrieve emails by ID with optional property filtering
- `Email/import` - Import RFC 5322 messages from blobs
- `Email/query` - (placeholder, returns `serverFail`)

## TODOs

The following enhancements are planned for future versions:

### Email/get

- **BatchGetItem**: Use `BatchGetItem` instead of sequential `GetItem` calls for multi-ID efficiency
- **bodyValues**: Implement content fetching from blob storage (currently returns `{}`)
- **header:\* properties**: Support arbitrary header property syntax (currently rejected with `invalidArguments`)
- **Additional properties**: Add `sender` and `bcc` fields

### Email/import

- **Threading**: Implement proper thread assignment based on References/In-Reply-To headers (currently uses email ID as thread ID)

### General

- **Email/query**: Implement query support with mailbox filtering, sorting, and pagination
- **Email/changes**: Implement state tracking for delta sync
- **Email/set**: Implement email mutations (keywords, mailbox assignments)

## License

See LICENSE file.
