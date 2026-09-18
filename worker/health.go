package worker

import (
	"context"
	"time"

	"github.com/QcomWrt/Q-SSH-WORKER/logger"
	gossh "golang.org/x/crypto/ssh"
)

const (
	HealthHealthy  = "HEALTHY"
	HealthDegraded = "DEGRADED"
	HealthDead     = "DEAD"

	healthInterval = 30 * time.Second
	healthTimeout  = 5 * time.Second

	// Jika channel SSH berhasil dibuka tetapi membutuhkan
	// lebih dari nilai ini, worker dianggap sedang degraded.
	degradedLatency = 1500 * time.Millisecond

	// Dua kegagalan berturut-turut tetap diperlukan
	// sebelum worker dianggap benar-benar DEAD.
	maxHealthFailures = 2
)

// HealthResult adalah hasil satu kali pemeriksaan worker.
type HealthResult struct {
	Status   string
	Latency  time.Duration
	Failures int
	Err      error
}

// MonitorHealth memeriksa kualitas koneksi SSH secara berkala.
//
// HEALTHY:
//   channel SSH berhasil dibuka dengan latency normal.
//
// DEGRADED:
//   channel SSH masih berhasil dibuka tetapi latency tinggi.
//
// DEAD:
//   channel SSH gagal dibuka beberapa kali berturut-turut.
//
// Penting:
// DEGRADED tidak menyebabkan worker dimatikan.
// Hanya DEAD yang dikirim ke manager sebagai kondisi fatal.
func MonitorHealth(
	ctx context.Context,
	sshClient *gossh.Client,
	resultChan chan<- HealthResult,
) {
	ticker := time.NewTicker(healthInterval)
	defer ticker.Stop()

	failureCount := 0

	for {
		select {
		case <-ctx.Done():
			return

		case <-ticker.C:
			result := checkHealth(
				ctx,
				sshClient,
				&failureCount,
			)

			select {
			case resultChan <- result:
			case <-ctx.Done():
				return
			}

			if result.Status == HealthDead {
				logger.StatusError(
					"Koneksi SSH benar-benar gagal; worker akan direconnect.",
				)
				return
			}
		}
	}
}

// checkHealth melakukan satu kali pengecekan channel SSH.
func checkHealth(
	ctx context.Context,
	sshClient *gossh.Client,
	failureCount *int,
) HealthResult {

	start := time.Now()

	// ssh.Client.Dial tidak menerima context.
	// Karena itu kita jalankan dalam goroutine dan memberikan
	// batas waktu maksimum di sisi monitor.
	done := make(chan error, 1)

	go func() {
		conn, err := sshClient.Dial(
			"tcp",
			"1.1.1.1:80",
		)

		if err == nil && conn != nil {
			_ = conn.Close()
		}

		done <- err
	}()

	var err error

	select {
	case <-ctx.Done():
		return HealthResult{
			Status:   HealthDead,
			Failures: *failureCount,
			Err:      ctx.Err(),
		}

	case <-time.After(healthTimeout):
		err = context.DeadlineExceeded

	case err = <-done:
	}

	latency := time.Since(start)

	// ==========================================================
	// GAGAL
	// ==========================================================

	if err != nil {
		*failureCount++

		status := HealthDegraded

		if *failureCount >= maxHealthFailures {
			status = HealthDead
		}

		return HealthResult{
			Status:   status,
			Latency:  latency,
			Failures: *failureCount,
			Err:      err,
		}
	}

	// ==========================================================
	// BERHASIL
	// ==========================================================

	// Satu keberhasilan langsung mereset failure counter.
	*failureCount = 0

	status := HealthHealthy

	if latency >= degradedLatency {
		status = HealthDegraded
	}

	return HealthResult{
		Status:   status,
		Latency:  latency,
		Failures: 0,
		Err:      nil,
	}
}