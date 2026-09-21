package internal

import "sync"

// BufferSize 32KB adalah sweet spot antara throughput dan memory per koneksi.
const BufferSize = 32 * 1024

var bufPool = sync.Pool{
	New: func() any {
		return make([]byte, BufferSize)
	},
}

// GetBuffer mengembalikan buffer 32KB dari pool.
// Caller WAJIB memanggil PutBuffer setelah selesai.
func GetBuffer() []byte {
	return bufPool.Get().([]byte)
}

// PutBuffer mengembalikan buffer ke pool.
// Buffer dengan kapasitas kurang dari BufferSize akan diabaikan.
func PutBuffer(b []byte) {
	if cap(b) < BufferSize {
		return
	}
	bufPool.Put(b[:BufferSize])
}