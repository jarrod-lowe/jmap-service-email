# JMAP Service Email Plugin

Email plugin for [jmap-service-core](https://github.com/jarrod-lowe/jmap-service-core), providing JMAP email capabilities.

## Status

**Partial implementation** - `Email/import`, `Email/get`, `Email/query`, `Email/set`, `Email/changes`, `Mailbox/get`, `Mailbox/set`, `Mailbox/changes`, `Thread/get`, and `Thread/changes` are functional.

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
│   ├── email-import/          # Email/import Lambda
│   ├── email-get/             # Email/get Lambda
│   ├── email-query/           # Email/query Lambda
│   ├── email-set/             # Email/set Lambda
│   ├── email-changes/         # Email/changes Lambda
│   ├── mailbox-get/           # Mailbox/get Lambda
│   ├── mailbox-set/           # Mailbox/set Lambda
│   ├── mailbox-changes/       # Mailbox/changes Lambda
│   ├── thread-get/            # Thread/get Lambda
│   ├── thread-changes/        # Thread/changes Lambda
│   ├── blob-delete/           # Blob delete SQS consumer Lambda
│   ├── email-cleanup/         # DynamoDB Streams email cleanup Lambda
│   ├── email-index/           # Search indexing SQS consumer Lambda
│   ├── account-init/          # Account initialization SQS consumer Lambda
│   └── mailbox-cleanup/       # Mailbox cleanup SQS consumer Lambda
├── internal/
│   ├── email/                 # Email types, repository, parser
│   ├── headers/               # Header property parsing and form transformations
│   ├── mailbox/               # Mailbox types and repository
│   ├── state/                 # State tracking repository
│   ├── blob/                  # Blob API client
│   ├── blobdelete/            # Async blob deletion SQS publisher
│   ├── searchindex/           # Search index SQS publisher
│   ├── htmlstrip/             # Streaming HTML-to-text converter
│   ├── embeddings/            # Bedrock Titan Embeddings client
│   └── vectorstore/           # S3 Vectors client
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

- `Email/get` - Retrieve emails by ID with optional property filtering, including `header:*` properties
- `Email/import` - Import RFC 5322 messages from blobs
- `Email/query` - Query emails with `inMailbox` filter and `receivedAt` sorting
- `Email/set` - Update email properties (mailbox assignments, keywords) and destroy emails
- `Email/changes` - Get email changes since a given state (for delta sync)
- `Mailbox/get` - Retrieve mailboxes by ID or get all
- `Mailbox/set` - Create, update, and destroy mailboxes
- `Mailbox/changes` - Get mailbox changes since a given state (for delta sync)
- `Thread/get` - Retrieve threads by ID (returns emailIds in receivedAt order)
- `Thread/changes` - Get thread changes since a given state (for delta sync)

## Email/get bodyValues

The `bodyValues` property returns decoded text content from email body parts when requested via `fetchTextBodyValues`, `fetchHTMLBodyValues`, or `fetchAllBodyValues`.

### Configuration

| Variable | Environment | Default | Description |
|----------|-------------|---------|-------------|
| `max_body_value_bytes` | Terraform | 262144 (256KB) | Server-side maximum for `maxBodyValueBytes` |
| `MAX_BODY_VALUE_BYTES` | Lambda env | 262144 | Actual limit used at runtime |

### Memory Model

The implementation streams blob content through a charset decoder. Memory usage per body part is bounded by:

- **Decoded output buffer**: `maxBodyValueBytes + 1` (to detect truncation)
- **Transform reader buffer**: ~4KB internal buffer

Worst case for a single Email/get: ~15MB when fetching maximum-sized body values for multiple parts (bounded by Lambda memory).

### Charset Decoding

Body parts are decoded from their declared charset to UTF-8:

- Uses `golang.org/x/text/encoding/ianaindex` for charset lookup
- Empty charset defaults to US-ASCII
- Unknown charsets fall back to Latin-1 with `isEncodingProblem: true`
- Invalid byte sequences trigger Latin-1 fallback with `isEncodingProblem: true`

### Truncation

When decoded content exceeds `maxBodyValueBytes`:
- `value` contains the first `maxBodyValueBytes` bytes
- `isTruncated` is set to `true`

## Async Blob Deletion

When emails are destroyed via `Email/set` or when `Email/import` fails after uploading parts, blob IDs are published to an SQS queue for async deletion. A dedicated `blob-delete` Lambda consumes from this queue and calls `DELETE /delete-iam/{accountId}/{blobId}` for each blob.

```
Email/set destroy ──┐
                    ├──> SQS Queue ──> blob-delete Lambda ──> DELETE /delete-iam/...
Email/import fail ──┘         │
                              └──> DLQ (after 3 retries) ──> CloudWatch Alarm
```

**DLQ operational runbook:**
- The `blob-delete-dlq-depth` CloudWatch alarm fires when messages land in the DLQ
- Investigate DLQ messages via the AWS Console or `aws sqs receive-message`
- For transient failures: requeue messages from DLQ back to the main queue
- For persistent failures: investigate the blob service or message content

## Search Indexing

Emails are indexed for full-text search using vector embeddings stored in S3 Vectors. When an email is imported or destroyed, a message is published to an SQS queue, and a dedicated `email-index` Lambda processes it asynchronously.

```
Email/import ──┐
               ├──> SQS Queue ──> email-index Lambda ──> S3 Vectors
Email/set ─────┘         │              │
  (destroy)              │              ├── Fetch text parts from blob storage
                         │              ├── Strip HTML (if needed)
                         │              ├── Chunk text (~30K chars with overlap)
                         │              ├── Generate embeddings (Bedrock Titan v2)
                         │              └── Store/delete vectors
                         └──> DLQ (after 3 retries) ──> CloudWatch Alarm
```

- **Embedding model**: Amazon Titan Embeddings v2 (1024 dimensions, ~$0.00002/1K tokens)
- **Vector store**: AWS S3 Vectors, one index per account (`acct-{accountId}`)
- **Account limit**: 10,000 accounts per vector bucket (S3 Vectors index limit)
- **Chunking**: ~30K chars per chunk with 800-char overlap; subject/addresses prepended to each chunk
- **Metadata**: Each vector stores emailId, mailboxIds, keywords, receivedAt, from, to, subject, size for future hybrid filtering

See `docs/plans/search-indexing-design.md` for the full system design.

## DynamoDB Indexes

The email data table uses the following Local Secondary Indexes:

| Index | Sort Key Pattern | Purpose |
|-------|------------------|---------|
| LSI1 | `RCVD#{receivedAt}#{emailId}` | Query all emails sorted by receivedAt (Email/query without filter) |
| LSI2 | `MSGID#{messageId}` | Find email by Message-ID header (threading parent lookup) |
| LSI3 | `THREAD#{threadId}#RCVD#{receivedAt}#{emailId}` | Query all emails in a thread sorted by receivedAt (Thread/get) |

## TODOs

The following enhancements are planned for future versions:

### Email/get

- **BatchGetItem**: Use `BatchGetItem` instead of sequential `GetItem` calls for multi-ID efficiency
- **header:\* caching**: Header data is fetched on-demand from blob storage; consider caching for repeated requests

### Email/query

- **canCalculateChanges**: Always returns `false`; `Email/queryChanges` not implemented
- **Additional filters**: Only `inMailbox` is supported; other filters return `unsupportedFilter`
- **Additional sorts**: Only `receivedAt` is supported; other sorts return `unsupportedSort`
- **Anchor pagination**: Anchor validation is implemented but anchor-based querying is not yet functional

### Threading

- **References header**: Currently only `In-Reply-To` is used for threading; `References` header could improve thread grouping
- **Subject matching**: Implement subject-based threading for emails without `In-Reply-To` header
- **Out-of-order delivery**: Thread merging when reply arrives before parent is not implemented (creates fragmented threads)
- **Auto-delete threads**: Threads are not automatically deleted when they have no remaining emails

### Mailbox

- **Thread counts are stubbed**: `totalThreads` equals `totalEmails`, `unreadThreads` equals `unreadEmails`
- **Mailbox name uniqueness not enforced**: Multiple mailboxes can have the same name
- **Hierarchical mailboxes not supported**: `parentId` is always `null`; attempts to set `parentId` to non-null return `invalidProperties`
- **onDestroyRemoveEmails**: Not fully implemented; only checks if mailbox is empty

### Email/set

- **Create**: `Email/set create` is not supported; use `Email/import` to create emails

### General

- **Mailbox/query**: Implement mailbox query support
- **Email/queryChanges**: Implement query result change tracking

## License

See LICENSE file.
