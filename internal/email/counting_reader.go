package email

import "io"

// CountingReader wraps an io.Reader and tracks the number of bytes read.
type CountingReader struct {
	r     io.Reader
	count int64
}

// NewCountingReader creates a new CountingReader wrapping the given reader.
func NewCountingReader(r io.Reader) *CountingReader {
	return &CountingReader{r: r}
}

// Read reads from the underlying reader and updates the byte count.
func (c *CountingReader) Read(p []byte) (n int, err error) {
	n, err = c.r.Read(p)
	c.count += int64(n)
	return n, err
}

// BytesRead returns the total number of bytes read so far.
func (c *CountingReader) BytesRead() int64 {
	return c.count
}
