// Package pool provides buffer pooling for zero-allocation message handling.
package pool

import (
	"bytes"
	"sync"
)

const (
	// DefaultBufferSize is the default size of pooled buffers
	DefaultBufferSize = 4096
	// MaxPoolSize limits the number of buffers in the pool
	MaxPoolSize = 1024
)

var (
	// BufferPool is a pool of *bytes.Buffer for reuse
	BufferPool = sync.Pool{
		New: func() any {
			return bytes.NewBuffer(make([]byte, 0, DefaultBufferSize))
		},
	}

	// ByteSlicePool is a pool of []byte slices for zero-copy operations
	ByteSlicePool = sync.Pool{
		New: func() any {
			buf := make([]byte, DefaultBufferSize)
			return &buf
		},
	}
)

// GetBuffer retrieves a buffer from the pool.
// Caller must call PutBuffer when done.
func GetBuffer() *bytes.Buffer {
	return BufferPool.Get().(*bytes.Buffer)
}

// PutBuffer returns a buffer to the pool.
func PutBuffer(buf *bytes.Buffer) {
	buf.Reset()
	BufferPool.Put(buf)
}

// GetByteSlice retrieves a byte slice from the pool.
// Caller must call PutByteSlice when done.
func GetByteSlice() *[]byte {
	return ByteSlicePool.Get().(*[]byte)
}

// PutByteSlice returns a byte slice to the pool.
func PutByteSlice(slice *[]byte) {
	*slice = (*slice)[:0] // Reset length but keep capacity
	ByteSlicePool.Put(slice)
}

// FramePool manages pre-allocated frame structures for encoding/decoding.
type FramePool struct {
	pool sync.Pool
}

// NewFramePool creates a new frame pool with the given initial capacity.
func NewFramePool(capacity int) *FramePool {
	return &FramePool{
		pool: sync.Pool{
			New: func() any {
				return make([]byte, 0, capacity)
			},
		},
	}
}

// Get retrieves a frame buffer from the pool.
func (fp *FramePool) Get() []byte {
	return fp.pool.Get().([]byte)
}

// Put returns a frame buffer to the pool.
func (fp *FramePool) Put(frame []byte) {
	frame = frame[:0] // Reset length
	fp.pool.Put(frame)
}
