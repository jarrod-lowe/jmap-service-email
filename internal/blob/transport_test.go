package blob

import (
	"context"
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
)

// fakeRoundTripper implements http.RoundTripper for testing.
type fakeRoundTripper struct {
	roundTripFunc func(req *http.Request) (*http.Response, error)
}

func (f *fakeRoundTripper) RoundTrip(req *http.Request) (*http.Response, error) {
	if f.roundTripFunc != nil {
		return f.roundTripFunc(req)
	}
	return &http.Response{StatusCode: http.StatusOK, Body: http.NoBody}, nil
}

// fakeCredentialsProvider implements aws.CredentialsProvider for testing.
type fakeCredentialsProvider struct{}

func (f *fakeCredentialsProvider) Retrieve(ctx context.Context) (aws.Credentials, error) {
	return aws.Credentials{
		AccessKeyID:     "AKIAIOSFODNN7EXAMPLE",
		SecretAccessKey: "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
		SessionToken:    "test-session-token",
	}, nil
}

func TestSigV4Transport_AddsAuthorizationHeader(t *testing.T) {
	var capturedReq *http.Request
	fakeRT := &fakeRoundTripper{
		roundTripFunc: func(req *http.Request) (*http.Response, error) {
			capturedReq = req
			return &http.Response{
				StatusCode: http.StatusOK,
				Body:       http.NoBody,
			}, nil
		},
	}

	transport := NewSigV4Transport(fakeRT, &fakeCredentialsProvider{}, "us-east-1")

	req, _ := http.NewRequest(http.MethodGet, "https://api.example.com/download-iam/user-123/blob-456", nil)
	_, err := transport.RoundTrip(req)
	if err != nil {
		t.Fatalf("RoundTrip error = %v, want nil", err)
	}

	if capturedReq == nil {
		t.Fatal("wrapped transport was not called")
	}

	authHeader := capturedReq.Header.Get("Authorization")
	if authHeader == "" {
		t.Error("Authorization header not set")
	}
	// AWS SigV4 headers start with "AWS4-HMAC-SHA256"
	if len(authHeader) < 16 || authHeader[:16] != "AWS4-HMAC-SHA256" {
		t.Errorf("Authorization header = %q, want to start with AWS4-HMAC-SHA256", authHeader)
	}
}

func TestSigV4Transport_HashesRequestBody(t *testing.T) {
	t.Run("no body GET", func(t *testing.T) {
		var capturedReq *http.Request
		fakeRT := &fakeRoundTripper{
			roundTripFunc: func(req *http.Request) (*http.Response, error) {
				capturedReq = req
				return &http.Response{
					StatusCode: http.StatusOK,
					Body:       http.NoBody,
				}, nil
			},
		}

		transport := NewSigV4Transport(fakeRT, &fakeCredentialsProvider{}, "us-east-1")

		req, _ := http.NewRequest(http.MethodGet, "https://api.example.com/download-iam/user-123/blob-456", nil)
		_, err := transport.RoundTrip(req)
		if err != nil {
			t.Fatalf("RoundTrip error = %v, want nil", err)
		}

		if capturedReq == nil {
			t.Fatal("wrapped transport was not called")
		}

		// X-Amz-Content-Sha256 should not be set (no UNSIGNED-PAYLOAD)
		if v := capturedReq.Header.Get("X-Amz-Content-Sha256"); v != "" {
			t.Errorf("X-Amz-Content-Sha256 = %q, want empty (header should not be set)", v)
		}

		// Authorization header must still be present
		if capturedReq.Header.Get("Authorization") == "" {
			t.Error("Authorization header not set")
		}
	})

	t.Run("with body POST", func(t *testing.T) {
		const bodyContent = `{"blobId":"blob-123","size":42}`

		var capturedReq *http.Request
		fakeRT := &fakeRoundTripper{
			roundTripFunc: func(req *http.Request) (*http.Response, error) {
				capturedReq = req
				return &http.Response{
					StatusCode: http.StatusOK,
					Body:       http.NoBody,
				}, nil
			},
		}

		transport := NewSigV4Transport(fakeRT, &fakeCredentialsProvider{}, "us-east-1")

		req, _ := http.NewRequest(http.MethodPost, "https://api.example.com/jmap-iam/user-123", strings.NewReader(bodyContent))
		req.Header.Set("Content-Type", "application/json")
		_, err := transport.RoundTrip(req)
		if err != nil {
			t.Fatalf("RoundTrip error = %v, want nil", err)
		}

		if capturedReq == nil {
			t.Fatal("wrapped transport was not called")
		}

		// Body must still be readable after signing (was buffered)
		gotBody, err := io.ReadAll(capturedReq.Body)
		if err != nil {
			t.Fatalf("reading body: %v", err)
		}
		if string(gotBody) != bodyContent {
			t.Errorf("body = %q, want %q", string(gotBody), bodyContent)
		}

		// ContentLength must be set correctly
		if capturedReq.ContentLength != int64(len(bodyContent)) {
			t.Errorf("ContentLength = %d, want %d", capturedReq.ContentLength, len(bodyContent))
		}

		// Authorization header must be present
		if capturedReq.Header.Get("Authorization") == "" {
			t.Error("Authorization header not set")
		}

		// X-Amz-Content-Sha256 should not be set
		if v := capturedReq.Header.Get("X-Amz-Content-Sha256"); v != "" {
			t.Errorf("X-Amz-Content-Sha256 = %q, want empty (header should not be set)", v)
		}
	})
}

func TestSigV4Transport_CallsWrappedTransport(t *testing.T) {
	called := false
	fakeRT := &fakeRoundTripper{
		roundTripFunc: func(req *http.Request) (*http.Response, error) {
			called = true
			return &http.Response{
				StatusCode: http.StatusOK,
				Body:       http.NoBody,
			}, nil
		},
	}

	transport := NewSigV4Transport(fakeRT, &fakeCredentialsProvider{}, "us-east-1")

	req, _ := http.NewRequest(http.MethodGet, "https://api.example.com/test", nil)
	_, _ = transport.RoundTrip(req)

	if !called {
		t.Error("wrapped transport was not called")
	}
}
