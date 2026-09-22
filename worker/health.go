package worker

import (
	"context"
	"errors"
	"time"

	"github.com/QcomWrt/Q-SSH-WORKER/logger"
	gossh "golang.org/x/crypto/ssh"
)

const (
	healthInterval    = 20 * time.Second
	healthMaxFailures = 3
	probeTimeout      = 10 * time.Second
)

// MonitorHealth membuka channel SSH baru ke DNS publik tiap interval.
//
// Ini mengecek 3 hal sekaligus:
//  1. SSH session masih hidup
//  2. Channel subsystem masih bisa dibuka
//  3. Jalur end-to-end (router → Cloudflare → VPS) masih tembus
//
// Kalau salah satu rusak (termasuk Cloudflare silent drop), probe gagal.
func MonitorHealth(
	ctx context.Context,
	sshClient *gossh.Client,
	failChan chan<- bool,
) {
	ticker := time.NewTicker(healthInterval)
	defer ticker.Stop()

	failureCount := 0

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if sshClient == nil {
				return
			}

			err := probeSSH(ctx, sshClient)
			if err == nil {
				if failureCount > 0 {
					logger.Info("[HEALTH] probe recovered")
				}
				failureCount = 0
				continue
			}

			failureCount++
			logger.Warn(
				"[HEALTH] probe failed (%d/%d): %v",
				failureCount,
				healthMaxFailures,
				err,
			)

			if failureCount >= healthMaxFailures {
				logger.Errorf(
					"[HEALTH] koneksi dianggap mati setelah %d kegagalan berturut",
					failureCount,
				)
				failChan <- true
				return
			}
		}
	}
}

// probeSSH buka channel SSH ke 1.1.1.1:53 lalu langsung tutup.
//
// Pakai port DNS karena ringan, hampir selalu terbuka, dan
// tidak butuh handshake SSH tambahan.
func probeSSH(ctx context.Context, client *gossh.Client) error {
	done := make(chan error, 1)

	go func() {
		conn, err := client.Dial("tcp", "1.1.1.1:53")
		if err == nil {
			_ = conn.Close()
		}
		done <- err
	}()

	select {
	case err := <-done:
		return err
	case <-time.After(probeTimeout):
		return errors.New("probe timeout")
	case <-ctx.Done():
		return ctx.Err()
	}
}