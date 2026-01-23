package blob

import (
	"context"
	"net/http"
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
