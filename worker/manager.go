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

// ErrAuthFailed menandakan kredensial SSH / payload ditolak server.
// Main akan menerjemahkan error ini menjadi exit code 5.
var ErrAuthFailed = errors.New("auth_failed")

// isSSHAuthError memeriksa apakah error dari SSH handshake adalah auth error.
func isSSHAuthError(err error) bool {
	if err == nil {
		return false
	}
	s := strings.ToLower(err.Error())

	return strings.Contains(s, "handshake") ||
		strings.Contains(s, "auth") ||
		strings.Contains(s, "credential") ||
		strings.Contains(s, "password") ||
		strings.Contains(s, "sign") ||
		strings.Contains(s, "illegal") ||
		strings.Contains(s, "rejected")
}

// StartWorker mengelola satu worker SSH sampai gagal.
func StartWorker(ctx context.Context, cfg *config.Config) error {
	reconnectPolicy := NewReconnectPolicy(
		2*time.Second,
		30*time.Second,
	)

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

	// ==============================================================
	// INITIAL NETWORK DIAL
	// ==============================================================

	for {
		dialCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
		conn, err = n.Dial(dialCtx)
		cancel()

		if err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}

			if sleepErr := reconnectPolicy.Sleep(ctx); sleepErr != nil {
				return sleepErr
			}
			continue
		}

		break
	}

	reconnectPolicy.Reset()

	// ==============================================================
	// OBSERVED CONNECTION
	// ==============================================================

	workerStats := &TrafficStats{}
	conn = NewObservedConn(conn, workerStats)

	// ==============================================================
	// TRANSPORT
	// ==============================================================

	wrappedConn, err := transport.Wrap(cfg, conn)
	if err != nil {
		if conn != nil {
			_ = conn.Close()
		}

		if errors.Is(err, response.ErrAuthFailed) {
			logger.Errorf("[FATAL] Payload AUTH_FAILED: kredensial ditolak oleh gateway HTTP")
			return ErrAuthFailed
		}

		logger.ProxyError(err)
		return err
	}

	conn = wrappedConn

	if cfg.Proxy.Host != "" {
		debug.Proxy(
			cfg.Proxy.Host,
			cfg.Proxy.Port,
			cfg.SSH.Host,
			cfg.SSH.Port,
		)
		logger.ProxyConnected()
	}

	logger.SSHConnecting()

	// ==============================================================
	// SSH HANDSHAKE
	// ==============================================================

	client, err := workerssh.Dial(cfg, conn)
	if err != nil {
		logger.SSHError(err)

		if isSSHAuthError(err) {
			logger.Errorf("[FATAL] Kredensial SSH ditolak oleh server Dropbear/OpenSSH")
			return ErrAuthFailed
		}

		return err
	}

	logger.SSHConnected()

	// ==============================================================
	// REMOTE IP DEBUG
	// ==============================================================

	remoteIP := cfg.SSH.Host
	if ips, err := net.LookupIP(cfg.SSH.Host); err == nil && len(ips) > 0 {
		for _, ip := range ips {
			if ip.To4() != nil {
				remoteIP = ip.String()
				break
			}
		}
	}

	remoteAddrStr := fmt.Sprintf("%s:%d", remoteIP, cfg.SSH.Port)

	debug.SSHNetworkDetails(
		cfg.Network.Type,
		remoteAddrStr,
		conn.RemoteAddr(),
		conn.LocalAddr(),
	)

	logger.StatusConnected()

	// ==============================================================
	// CONNECTION REGISTRY
	// ==============================================================

	workerID := os.Getenv("QTUN_TARGET_PORT")
	if workerID == "" {
		workerID = "unknown"
	}

	connectionRegistry := socks.NewConnectionRegistry(workerID)

	logger.Info(
		"[WORKER %s] Connection registry initialized (PID=%d)",
		workerID,
		os.Getpid(),
	)

	// ==============================================================
	// SOCKS SERVER
	// ==============================================================

	socksErrChan := make(chan error, 1)

	go func() {
		if envPort := os.Getenv("QTUN_TARGET_PORT"); envPort != "" {
			var dynamicPort int
			if _, err := fmt.Sscanf(envPort, "%d", &dynamicPort); err == nil &&
				dynamicPort > 0 {
				cfg.Listen.Port = dynamicPort
			}
		}

		socksErrChan <- socks.ListenAndServe(
			cfg,
			client,
			connectionRegistry,
		)
	}()

	// ==============================================================
	// HEALTH MONITOR
	// ==============================================================

	healthCtx, cancelHealth := context.WithCancel(ctx)
	defer cancelHealth()

	healthFailChan := make(chan bool, 1)

	go MonitorHealth(healthCtx, client, healthFailChan)

	// ==============================================================
	// WAIT FOR FAILURE / SHUTDOWN
	// ==============================================================

	var fatalErr error

	select {
	case err := <-socksErrChan:
		fatalErr = fmt.Errorf("socks5 server stopped: %v", err)
	case <-healthFailChan:
		fatalErr = errors.New("ssh keepalive failed, connection considered dead")
	case <-ctx.Done():
		fatalErr = ctx.Err()
	}

	logger.Info(
		"[WORKER %s] Shutdown initiated: %v",
		workerID,
		fatalErr,
	)

	cancelHealth()
	connectionRegistry.CloseAll("worker_shutdown")

	if client != nil {
		_ = client.Close()
	}

	if conn != nil {
		_ = conn.Close()
	}

	logger.Info(
		"[WORKER %s] Shutdown complete (PID=%d)",
		workerID,
		os.Getpid(),
	)

	return fatalErr
}