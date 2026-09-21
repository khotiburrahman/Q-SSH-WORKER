package worker

import (
	"context"
	"time"

	"github.com/QcomWrt/Q-SSH-WORKER/logger"
	gossh "golang.org/x/crypto/ssh"
)

const (
	healthInterval     = 15 * time.Second
	healthMaxFailures  = 3
	keepaliveTimeout   = 10 * time.Second
)

// MonitorHealth melakukan ping berkala ke server SSH menggunakan
// protokol keepalive@openssh.com.
//
// Jika SSH benar-benar mati gantung, ia akan mengirim sinyal true ke failChan.
// Kegagalan ini TIDAK dipicu oleh gangguan internet keluar dari VPS,
// hanya oleh SSH session itu sendiri.
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
			logger.Debug(
				"[HEALTH] tick at %s failure=%d",
				time.Now().Format(time.RFC3339),
				failureCount,
			)

			if sshClient == nil {
				return
			}

			done := make(chan error, 1)
			go func() {
				_, _, err := sshClient.SendRequest(
					"keepalive@openssh.com",
					true,
					nil,
				)
				done <- err
			}()

			select {
			case err := <-done:
				if err == nil {
					failureCount = 0
					continue
				}

				failureCount++
				logger.Warn(
					"[HEALTH] keepalive failed (%d/%d): %v",
					failureCount,
					healthMaxFailures,
					err,
				)
			case <-time.After(keepaliveTimeout):
				failureCount++
				logger.Warn(
					"[HEALTH] keepalive timeout (%d/%d)",
					failureCount,
					healthMaxFailures,
				)
			}

			if failureCount >= healthMaxFailures {
				logger.Errorf(
					"[HEALTH] SSH keepalive failed %d times, connection considered dead",
					failureCount,
				)
				failChan <- true
				return
			}
		}
	}
}