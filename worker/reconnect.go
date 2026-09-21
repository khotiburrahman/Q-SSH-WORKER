package worker

import (
	"context"
	"math/rand"
	"time"
)

// ReconnectPolicy menyimpan status durasi backoff untuk satu siklus kegagalan.
type ReconnectPolicy struct {
	baseDelay float64
	maxDelay  time.Duration
	attempts  int
}

// NewReconnectPolicy menginisialisasi kebijakan koneksi ulang.
func NewReconnectPolicy(base time.Duration, max time.Duration) *ReconnectPolicy {
	return &ReconnectPolicy{
		baseDelay: base.Seconds(),
		maxDelay:  max,
		attempts:  0,
	}
}

// GetDelay menghitung durasi tunggu berikutnya
// menggunakan Exponential Backoff + Jitter.
func (p *ReconnectPolicy) GetDelay() time.Duration {
	p.attempts++

	temp := p.baseDelay * float64(uint(1)<<uint(p.attempts-1))

	if temp > p.maxDelay.Seconds() {
		temp = p.maxDelay.Seconds()
	}

	jitter := 0.5 + rand.Float64()*0.5
	finalDelay := temp * jitter

	return time.Duration(finalDelay * float64(time.Second))
}

// Sleep menghitung delay lalu menunggu.
// Akan langsung return jika ctx dibatalkan (misal SIGTERM).
func (p *ReconnectPolicy) Sleep(ctx context.Context) error {
	delay := p.GetDelay()

	select {
	case <-time.After(delay):
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Reset mengembalikan hitungan kegagalan ke nol saat koneksi resmi sukses.
func (p *ReconnectPolicy) Reset() {
	p.attempts = 0
}