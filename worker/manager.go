package worker

import (
	"context"
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
)

// StartWorker mengelola satu worker SSH.
func StartWorker(cfg *config.Config) error {

	reconnectPolicy := NewReconnectPolicy(
		2*time.Second,
		30*time.Second,
	)

	n, err := network.New(cfg)

	if err != nil {
		return fmt.Errorf(
			"network init failed: %w",
			err,
		)
	}

	var conn net.Conn

	// ==============================================================
	// LOG START CONNECTION
	// ==============================================================

	if cfg.Proxy.Host != "" {
		logger.ProxyConnecting()
	} else {
		logger.TCPConnecting()
	}

	// ==============================================================
	// INITIAL NETWORK DIAL
	// ==============================================================

	for {
		ctx, cancel := context.WithTimeout(
			context.Background(),
			10*time.Second,
		)

		conn, err = n.Dial(ctx)

		cancel()

		if err != nil {
			delay := reconnectPolicy.GetDelay()

			logger.StatusError(
				fmt.Sprintf(
					"Connection failed: %v. Retrying in %v...",
					err,
					delay.Round(time.Second),
				),
			)

			time.Sleep(delay)

			continue
		}

		break
	}

	reconnectPolicy.Reset()

	// ==============================================================
	// OBSERVED CONNECTION
	// ==============================================================

	workerStats := &TrafficStats{}

	conn = NewObservedConn(
		conn,
		workerStats,
	)

	// ==============================================================
	// TRANSPORT
	// ==============================================================

	wrappedConn, err := transport.Wrap(
		cfg,
		conn,
	)

	if err != nil {
		if conn != nil {
			_ = conn.Close()
		}

		logger.ProxyError(err)

		return err
	}

	conn = wrappedConn

	// ==============================================================
	// TRANSPORT DEBUG
	// ==============================================================

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

	client, err := workerssh.Dial(
		cfg,
		conn,
	)

	if err != nil {
		logger.SSHError(err)

		errStr := strings.ToLower(
			err.Error(),
		)

		if strings.Contains(errStr, "handshake") ||
			strings.Contains(errStr, "auth") ||
			strings.Contains(errStr, "credential") ||
			strings.Contains(errStr, "password") ||
			strings.Contains(errStr, "sign") ||
			strings.Contains(errStr, "illegal") ||
			strings.Contains(errStr, "rejected") {

			println(
				"\n🛑 [FATAL - AGENT KILLED] Kredensial SSH ditolak oleh server Dropbear!",
			)

			println(
				"💡 Info: Akun sudah expired atau password salah. Memaksa mematikan Master Process...",
			)

			os.Exit(5)
		}

		return err
	}

	logger.SSHConnected()

	// ==============================================================
	// REMOTE IP DEBUG
	// ==============================================================

	remoteIP := cfg.SSH.Host

	if ips, err := net.LookupIP(
		cfg.SSH.Host,
	); err == nil && len(ips) > 0 {

		for _, ip := range ips {
			if ip.To4() != nil {
				remoteIP = ip.String()
				break
			}
		}
	}

	remoteAddrStr := fmt.Sprintf(
		"%s:%d",
		remoteIP,
		cfg.SSH.Port,
	)

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

	workerID := os.Getenv(
		"QTUN_TARGET_PORT",
	)

	if workerID == "" {
		workerID = "unknown"
	}

	connectionRegistry := socks.NewConnectionRegistry(
		workerID,
	)

	fmt.Printf(
		"[WORKER %s] Connection registry initialized (PID=%d)\n",
		workerID,
		os.Getpid(),
	)

	// ==============================================================
	// SOCKS SERVER
	// ==============================================================

	socksErrChan := make(chan error, 1)

	go func() {

		// ==========================================================
		// DYNAMIC PORT
		// ==========================================================

		if envPort := os.Getenv(
			"QTUN_TARGET_PORT",
		); envPort != "" {

			var dynamicPort int

			if _, err := fmt.Sscanf(
				envPort,
				"%d",
				&dynamicPort,
			); err == nil &&
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

	healthCtx, cancelHealth := context.WithCancel(
		context.Background(),
	)

	healthFailChan := make(chan bool, 1)

	go MonitorHealth(
		healthCtx,
		client,
		healthFailChan,
	)

	// ==============================================================
	// WAIT FOR FAILURE
	// ==============================================================

	var fatalErr error

	select {

	case err := <-socksErrChan:

		fatalErr = fmt.Errorf(
			"socks5 server stopped: %v",
			err,
		)

	case <-healthFailChan:

		fatalErr = fmt.Errorf(
			"koneksi internet mati gantung dideteksi oleh health monitor",
		)
	}

	// ==============================================================
	// WORKER SHUTDOWN
	//
	// Urutan:
	//
	// 1. Stop health monitor
	// 2. Close seluruh koneksi HP
	// 3. Close SSH client
	// 4. Close transport
	// 5. Return ke master
	// ==============================================================

	fmt.Printf(
		"[WORKER %s] Shutdown initiated: %v\n",
		workerID,
		fatalErr,
	)

	cancelHealth()

	// ==============================================================
	// CLOSE SEMUA KONEKSI SOCKS
	// ==============================================================

	connectionRegistry.CloseAll(
		"worker_shutdown",
	)

	// ==============================================================
	// CLOSE SSH CLIENT
	// ==============================================================

	if client != nil {
		_ = client.Close()
	}

	// ==============================================================
	// CLOSE TRANSPORT
	// ==============================================================

	if conn != nil {
		_ = conn.Close()
	}

	fmt.Printf(
		"[WORKER %s] Shutdown complete (PID=%d)\n",
		workerID,
		os.Getpid(),
	)

	return fatalErr
}