package blob

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"io"
	"net/http"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	v4 "github.com/aws/aws-sdk-go-v2/aws/signer/v4"
)

// SigV4Transport is an http.RoundTripper that signs requests with AWS SigV4.
type SigV4Transport struct {
	wrapped     http.RoundTripper
	credentials aws.CredentialsProvider
	region      string
	signer      *v4.Signer
}

// NewSigV4Transport creates a new SigV4Transport.
func NewSigV4Transport(wrapped http.RoundTripper, credentials aws.CredentialsProvider, region string) *SigV4Transport {
	return &SigV4Transport{
		wrapped:     wrapped,
		credentials: credentials,
		region:      region,
		signer:      v4.NewSigner(),
	}
}

// RoundTrip implements http.RoundTripper.
func (t *SigV4Transport) RoundTrip(req *http.Request) (*http.Response, error) {
	ctx := req.Context()
	if ctx == nil {
		ctx = context.Background()
	}

	creds, err := t.credentials.Retrieve(ctx)
	if err != nil {
		return nil, err
	}

	// Clone the request to avoid modifying the original
	signedReq := req.Clone(ctx)

	var payloadHashHex string
	if signedReq.Body == nil || signedReq.Body == http.NoBody {
		h := sha256.Sum256(nil)
		payloadHashHex = hex.EncodeToString(h[:])
	} else {
		bodyBytes, err := io.ReadAll(signedReq.Body)
		if err != nil {
			return nil, err
		}
		signedReq.Body.Close()
		h := sha256.Sum256(bodyBytes)
		payloadHashHex = hex.EncodeToString(h[:])
		signedReq.Body = io.NopCloser(bytes.NewReader(bodyBytes))
		signedReq.ContentLength = int64(len(bodyBytes))
	}

	// Sign the request
	err = t.signer.SignHTTP(ctx, creds, signedReq, payloadHashHex, "execute-api", t.region, time.Now())
	if err != nil {
		return nil, err
	}

	return t.wrapped.RoundTrip(signedReq)
}
