# Search Indexing Design

## Overview

Full-text search indexing for JMAP Email. Extracts body text from emails,
generates vector embeddings via Bedrock Titan Embeddings v2, and stores them in
S3 Vectors for future semantic search queries.

## Architecture

```
Email/import ──┐
               ├──> SQS (search-index) ──> email-index Lambda
Email/set ─────┘                                │
                                                ├── Read email from DynamoDB
                                                ├── Fetch text/* parts from blob storage
                                                ├── Strip HTML, compose text
                                                ├── Chunk if needed (>8K tokens)
                                                ├── Call Bedrock Titan Embeddings v2
                                                └── Write to S3 Vectors (acct-{accountId})
```

Email/import publishes an `"index"` message after successful import.
Email/set publishes `"index"` after create and `"delete"` after destroy.

## How It Works

### Text Extraction

1. Read email record from DynamoDB to get bodyStructure, textBody, htmlBody
2. Identify text/* part blobIds — prefer textBody parts, fall back to htmlBody
3. Stream each part blob via blob HTTP client
4. For text/html parts: pipe through streaming HTML stripper (golang.org/x/net/html
   tokenizer — extracts text nodes, alt/title attributes, skips style/script)
5. Compose header prefix: Subject, From, To, Cc, Bcc

### Chunking

- Chunk text into ~30K character segments (~7,500 tokens for Titan v2)
- 800 character overlap between chunks for context continuity
- Header prefix prepended to every chunk
- Chunks processed one at a time (streaming, low memory)
- Peak memory per part: one chunk buffer (~30KB) + overlap (~1KB) + tokenizer state

### Embedding Generation

- Model: Amazon Titan Embeddings v2 (`amazon.titan-embed-text-v2:0`)
- Output: 1024-dimension float32 vector
- Cost: ~$0.00002 per 1K tokens
- Input limit: 8,192 tokens per embedding call

### Vector Storage

- Each chunk immediately written to S3 Vectors after embedding generation
- Vector keys: `{emailId}#0`, `{emailId}#1`, etc. (sequential across all parts)
- After indexing, email DynamoDB record updated with `searchChunks: N`

### Deletion

1. Read `searchChunks` from DynamoDB email record
2. Delete vector keys `{emailId}#0` through `{emailId}#(N-1)` from S3 Vectors
3. No DynamoDB update needed — email record is being deleted anyway

## S3 Vectors Structure

- One `aws_s3vectors_vector_bucket` per environment
- One index per account: `acct-{accountId}` (created lazily by Lambda)
- Index config: 1024 dimensions, cosine distance metric
- Indexes cached in-memory within Lambda invocation

### Vector Metadata Schema

Each vector stores metadata for future hybrid filtering:

| Field | Type | Description |
|-------|------|-------------|
| emailId | string | Email identifier |
| mailboxIds | string list | Current mailbox memberships |
| keywords | string list | Email keywords ($seen, $flagged, etc.) |
| hasAttachment | boolean | Whether email has attachments |
| receivedAt | string | ISO 8601 timestamp |
| size | number | Email size in bytes |
| subject | string | Email subject |
| from | string | Sender email address |
| to | string | Recipient email addresses |
| chunkIndex | number | Chunk position within email |

## Known Limits and Scaling

| Limit | Value | Impact |
|-------|-------|--------|
| S3 Vectors indexes per bucket | 10,000 | Max ~10K accounts per bucket. If exceeded, need multiple buckets with routing. |
| S3 Vectors vectors per index | 2 billion | Unlikely to hit for a single account |
| Titan v2 input tokens | 8,192 per call | Emails chunked at ~30K chars to stay within limit |
| S3 Vectors query latency | 100–800ms | Sub-second but not instant |
| S3 Vectors query cost | Scales with index size | Per-account indexes keep this low |
| Embedding cost | ~$0.00002/1K tokens | Negligible for typical email volumes |

## SQS Message Format

```json
{
  "accountId": "user-123",
  "emailId": "email-456",
  "action": "index",
  "apiUrl": "https://api.example.com"
}
```

Action is `"index"` (create/update) or `"delete"` (remove vectors).

## Queue Configuration

- Main queue + DLQ with 14-day retention
- `maxReceiveCount: 3` redrive policy
- `visibility_timeout_seconds: 300` (5 min, enough for Bedrock + blob fetches)
- Batch size 10, batching window 5s
- CloudWatch alarm on DLQ depth

## Future Considerations

- PDF/attachment text extraction (e.g., Apache Tika or similar)
- Keyword search layer alongside semantic search
- Structured DynamoDB filters for non-vector filter conditions
- Multiple vector buckets if account count exceeds 10K
- Reindexing workflow for schema changes
