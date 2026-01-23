# Blob Client Design

## Overview

The blob client fetches RFC5322 email content from jmap-service-core for Email/import operations.

## Flow

```
email-import Lambda
    │
    ├─ SigV4-signed GET ──► API Gateway /download-iam/{accountId}/{blobId}
    │                              │
    │                              ▼
    │                       302 redirect (CloudFront signed URL)
    │                              │
    └─ (auto-follow) ────────────► CloudFront ──► S3 blob
                                        │
                                        ▼
                                   blob bytes
```

## Key Decisions

| Decision | Choice | Rationale |
|----------|--------|-----------|
| Authentication | IAM SigV4 signing | Consistent with AWS patterns, uses Lambda execution role |
| Signing implementation | Custom `http.RoundTripper` with `aws-sdk-go-v2` signer | Clean integration, auto-signs all requests |
| Redirect handling | Standard `http.Client` behavior | Library strips `Authorization` header on cross-host redirect; CloudFront URL has its own signature |
| Retry policy | 2 retries, 100ms exponential backoff | Fail fast for per-email error reporting |
| Testing approach | Dependency inversion with `HTTPDoer` interface | Test our logic, not standard library internals |

## Package Structure

```
internal/blob/
├── client.go      # HTTPBlobClient, HTTPDoer interface, errors
├── client_test.go # Tests with fake HTTPDoer
└── transport.go   # SigV4 signing RoundTripper
```

## Types

### HTTPDoer Interface

```go
// HTTPDoer abstracts HTTP client operations for dependency inversion
type HTTPDoer interface {
    Do(req *http.Request) (*http.Response, error)
}
```

### SigV4 Transport

```go
type sigV4Transport struct {
    wrapped     http.RoundTripper
    signer      *v4.Signer
    credentials aws.CredentialsProvider
    region      string
}

func (t *sigV4Transport) RoundTrip(req *http.Request) (*http.Response, error) {
    signedReq := t.sign(req)
    return t.wrapped.RoundTrip(signedReq)
}
```

Signs all requests. Standard `http.Client` redirect behavior strips the `Authorization` header when following redirects to different hosts, so CloudFront requests use the URL-based signature.

### HTTPBlobClient

```go
type HTTPBlobClient struct {
    baseURL    string
    httpClient HTTPDoer
    maxRetries int           // 2
    baseDelay  time.Duration // 100ms
}

func (c *HTTPBlobClient) FetchBlob(ctx context.Context, accountID, blobID string) ([]byte, error)
```

## URL Format

```
{CORE_API_URL}/download-iam/{accountId}/{blobId}
```

Example: `https://api.jmap.example.com/download-iam/user-123/blob-456`

## Error Handling

### Retry Policy

- **Retry:** Network errors, HTTP 5xx
- **Don't retry:** HTTP 4xx (client errors)
- **Max retries:** 2 (3 total attempts)
- **Backoff:** 100ms, 200ms (exponential)

### Error Mapping

| HTTP Status | Error | JMAP Error Type |
|-------------|-------|-----------------|
| 404 | `ErrBlobNotFound` | `blobNotFound` |
| 403 | `ErrForbidden` | `forbidden` |
| 5xx / network | `ErrServerFail` | `serverFail` |

## Configuration

| Environment Variable | Description |
|---------------------|-------------|
| `CORE_API_URL` | Base URL for jmap-service-core API Gateway |
| `AWS_REGION` | AWS region (from Lambda environment) |
| Credentials | From Lambda execution role (automatic) |

## Testing Strategy

Tests use a fake `HTTPDoer` implementation, not HTTP mocks.

**What we test:**
- Correct URL construction from accountID + blobID
- Retry logic: retries on 5xx, doesn't retry on 4xx
- Error mapping: status codes to typed errors
- Response body reading

**What we don't test:**
- Redirect following (http.Client's responsibility)
- SigV4 signing correctness (AWS SDK's responsibility)
