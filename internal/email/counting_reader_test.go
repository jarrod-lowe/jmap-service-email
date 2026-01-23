package email

import (
	"bytes"
	"io"
	"testing"
)

func TestCountingReader_CountsBytes(t *testing.T) {
	data := []byte("hello world")
	cr := NewCountingReader(bytes.NewReader(data))

	// Read all data
	buf := make([]byte, len(data))
	n, err := io.ReadFull(cr, buf)
	if err != nil {
		t.Fatalf("ReadFull error = %v", err)
	}
	if n != len(data) {
		t.Errorf("n = %d, want %d", n, len(data))
	}
	if cr.BytesRead() != int64(len(data)) {
		t.Errorf("BytesRead() = %d, want %d", cr.BytesRead(), len(data))
	}
}

func TestCountingReader_IncrementalReads(t *testing.T) {
	data := []byte("0123456789")
	cr := NewCountingReader(bytes.NewReader(data))

	// Read 3 bytes
	buf := make([]byte, 3)
	_, err := io.ReadFull(cr, buf)
	if err != nil {
		t.Fatalf("first ReadFull error = %v", err)
	}
	if cr.BytesRead() != 3 {
		t.Errorf("BytesRead() after first read = %d, want 3", cr.BytesRead())
	}

	// Read 5 more bytes
	buf = make([]byte, 5)
	_, err = io.ReadFull(cr, buf)
	if err != nil {
		t.Fatalf("second ReadFull error = %v", err)
	}
	if cr.BytesRead() != 8 {
		t.Errorf("BytesRead() after second read = %d, want 8", cr.BytesRead())
	}

	// Read remaining 2 bytes
	buf = make([]byte, 2)
	_, err = io.ReadFull(cr, buf)
	if err != nil {
		t.Fatalf("third ReadFull error = %v", err)
	}
	if cr.BytesRead() != 10 {
		t.Errorf("BytesRead() after third read = %d, want 10", cr.BytesRead())
	}
}

func TestCountingReader_StartsAtZero(t *testing.T) {
	cr := NewCountingReader(bytes.NewReader([]byte("test")))

	if cr.BytesRead() != 0 {
		t.Errorf("BytesRead() before any reads = %d, want 0", cr.BytesRead())
	}
}

func TestCountingReader_PropagatesEOF(t *testing.T) {
	data := []byte("short")
	cr := NewCountingReader(bytes.NewReader(data))

	buf := make([]byte, 100)
	n, err := cr.Read(buf)
	if err != nil {
		t.Fatalf("first Read error = %v (expected nil)", err)
	}
	if n != len(data) {
		t.Errorf("n = %d, want %d", n, len(data))
	}

	// Next read should return EOF
	n, err = cr.Read(buf)
	if err != io.EOF {
		t.Errorf("second Read error = %v, want io.EOF", err)
	}
	if n != 0 {
		t.Errorf("n = %d, want 0", n)
	}
}

func TestCountingReader_EmptyReader(t *testing.T) {
	cr := NewCountingReader(bytes.NewReader(nil))

	buf := make([]byte, 10)
	n, err := cr.Read(buf)
	if err != io.EOF {
		t.Errorf("Read error = %v, want io.EOF", err)
	}
	if n != 0 {
		t.Errorf("n = %d, want 0", n)
	}
	if cr.BytesRead() != 0 {
		t.Errorf("BytesRead() = %d, want 0", cr.BytesRead())
	}
}
