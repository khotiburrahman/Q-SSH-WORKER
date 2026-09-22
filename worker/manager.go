package worker

import (
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"strings"
	"time"

	"github.com/QcomWrt/Q-SSH-WORKER/config"
	"github.com/QcomWrt/Q-SSH-WORKER/debug"
	"github.com/QcomWrt/Q-SSH-WORKER/logger"
	"github.com/QcomWrt/Q-SSH-WORKER/network"
	"github.com/QcomWrt/Q-SSH-WORKER/socks"
	workerssh "github.com/QcomWrt/Q-SSH-WORKER/ssh"
	"github.com/QcomWrt/Q-SSH-WORKER/transport"
	"github.com/QcomWrt/Q-SSH-WORKER/transport/response"
)

// ErrAuthFailed menandakan kredensial SSH / payload benar-benar ditolak
// oleh server (bukan karena transport rusak).
var ErrAuthFailed = errors.New("auth_failed")

// isSSHAuthError HANYA return true kalau server SSH benar-benar menolak
// kredensial. Error transport seperti EOF, connection reset, atau
// "overflow reading version string" BUKAN auth error.
//
// golang.org/x/crypto/ssh membungkus SEMUA error handshake dengan prefix
// "ssh: handshake failed: ...", jadi tidak boleh match hanya dari kata
// "handshake".
func isSSHAuthError(err error) bool {
	if err == nil {
		return false
	}
	s := strings.ToLower(err.Error())

	// Auth failure sejati dari x/crypto/ssh
	if strings.Contains(s, "unable to authenticate") {
		return true
	}
	if strings.Contains(s, "no supported methods remain") {
		return true
	}
	if strings.Contains(s, "permission denied") {
		return true
	}

	// Auth failure dari Dropbear/OpenSSH
	if strings.Contains(s, "auth failed") {
		return true
	}
	if strings.Contains(s, "authentication failed") {
		return true
	}

	return false
}

// StartWorker menjalankan worker dengan loop reconnect internal.
//
// Hanya return kalau:
//   - Kredensial benar-benar ditolak (return ErrAuthFailed)
//   - Context dibatalkan (return ctx.Err())
//
// Selain itu, worker akan terus reconnect sendiri tanpa perlu
// master spawn ulang.
func StartWorker(ctx context.Context, cfg *config.Config) error {
	policy := NewReconnectPolicy(2*time.Second, 30*time.Second)

	for {
		err := runOnce(ctx, cfg)

		if err == nil {
			// Seharusnya tidak terjadi, tapi kalau terjadi, ulang.
			continue
		}

		// Kalau auth gagal, jangan ulang — kembalikan ke master.
		if errors.Is(err, ErrAuthFailed) {
			return err
		}

		// Kalau context dibatalkan, keluar bersih.
		if ctx.Err() != nil {
			return ctx.Err()
		}

		logger.Warn("[WORKER] koneksi terputus: %v. Reconnect dalam sebentar...", err)

		if sleepErr := policy.Sleep(ctx); sleepErr != nil {
			return sleepErr
		}
	}
}

// runOnce menjalankan satu siklus hidup worker: dial, transport, SSH,
// SOCKS listen. Return error begitu salah satu tahap gagal atau
// health check mendeteksi koneksi mati.
func runOnce(ctx context.Context, cfg *config.Config) error {
	n, err := network.New(cfg)
	if err != nil {
		return fmt.Errorf("network init failed: %w", err)
	}

	var conn net.Conn

	if cfg.Proxy.Host != "" {
		logger.ProxyConnecting()
	} else {
		logger.TCPConnecting()
	}

	// ---- INITIAL NETWORK DIAL ----
	dialCtx, dialCancel := context.WithTimeout(ctx, 10*time.Second)
	conn, err = n.Dial(dialCtx)
	dialCancel()

	if err != nil {
		return fmt.Errorf("dial failed: %w", err)
	}

	// ---- OBSERVED CONNECTION ----
	workerStats := &TrafficStats{}
	conn = NewObservedConn(conn, workerStats)

	// ---- TRANSPORT ----
	wrappedConn, err := transport.Wrap(cfg, conn)
	if err != nil {
		_ = conn.Close()

		if errors.Is(err, response.ErrAuthFailed) {
			logger.Errorf("[FATAL] Payload AUTH_FAILED: kredensial ditolak gateway HTTP")
			return ErrAuthFailed
		}

		logger.ProxyError(err)
		return fmt.Errorf("transport failed: %w", err)
	}

	conn = wrappedConn

	if cfg.Proxy.Host != "" {
		debug.Proxy(cfg.Proxy.Host, cfg.Proxy.Port, cfg.SSH.Host, cfg.SSH.Port)
		logger.ProxyConnected()
	}

	// ---- SSH HANDSHAKE ----
	logger.SSHConnecting()

	client, err := workerssh.Dial(cfg, conn)
	if err != nil {
		logger.SSHError(err)
		_ = conn.Close()

		// Hanya return ErrAuthFailed kalau BENAR-BENAR auth error.
		// Kalau transport rusak (Cloudflare drop, EOF, reset),
		// return error biasa supaya loop reconnect.
		if isSSHAuthError(err) {
			logger.Errorf("[FATAL] Kredensial SSH ditolak server")
			return ErrAuthFailed
		}

		return fmt.Errorf("ssh handshake failed: %w", err)
	}

	logger.SSHConnected()

	// ---- REMOTE IP DEBUG ----
	remoteIP := cfg.SSH.Host
	if ips, lookupErr := net.LookupIP(cfg.SSH.Host); lookupErr == nil && len(ips) > 0 {
		for _, ip := range ips {
			if ip.To4() != nil {
				remoteIP = ip.String()
				break
			}
		}
	}

	remoteAddrStr := fmt.Sprintf("%s:%d", remoteIP, cfg.SSH.Port)
	debug.SSHNetworkDetails(cfg.Network.Type, remoteAddrStr, conn.RemoteAddr(), conn.LocalAddr())

	logger.StatusConnected()

	// ---- CONNECTION REGISTRY ----
	workerID := os.Getenv("QTUN_TARGET_PORT")
	if workerID == "" {
		workerID = "unknown"
	}

	connectionRegistry := socks.NewConnectionRegistry(workerID)

	logger.Info("[WORKER %s] registry init (PID=%d)", workerID, os.Getpid())

	// ---- SOCKS SERVER ----
	socksErrChan := make(chan error, 1)

	go func() {
		if envPort := os.Getenv("QTUN_TARGET_PORT"); envPort != "" {
			var dynamicPort int
			if _, scanErr := fmt.Sscanf(envPort, "%d", &dynamicPort); scanErr == nil && dynamicPort > 0 {
				cfg.Listen.Port = dynamicPort
			}
		}

		socksErrChan <- socks.ListenAndServe(cfg, client, connectionRegistry)
	}()

	// ---- HEALTH MONITOR ----
	healthCtx, cancelHealth := context.WithCancel(ctx)
	defer cancelHealth()

	healthFailChan := make(chan bool, 1)
	go MonitorHealth(healthCtx, client, healthFailChan)

	// ---- WAIT FOR FAILURE ----
	var fatalErr error

	select {
	case err := <-socksErrChan:
		fatalErr = fmt.Errorf("socks listener stopped: %w", err)
	case <-healthFailChan:
		fatalErr = errors.New("health check: koneksi mati")
	case <-ctx.Done():
		fatalErr = ctx.Err()
	}

	logger.Info("[WORKER %s] shutdown: %v", workerID, fatalErr)

	cancelHealth()
	connectionRegistry.CloseAll("worker_shutdown")

	if client != nil {
		_ = client.Close()
	}
	if conn != nil {
		_ = conn.Close()
	}

	logger.Info("[WORKER %s] shutdown selesai", workerID)

	return fatalErr
}