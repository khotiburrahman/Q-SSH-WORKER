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

	defer cancelHealth()

	healthResultChan := make(chan HealthResult, 4)

	go MonitorHealth(
		healthCtx,
		client,
		healthResultChan,
	)

	// ==============================================================
	// TRAFFIC MONITOR
	// ==============================================================

	statsCtx, cancelStats := context.WithCancel(
		context.Background(),
	)

	defer cancelStats()

	// Snapshot pertama.
	previousStats := workerStats.Snapshot()

	go func() {
		ticker := time.NewTicker(
			10 * time.Second,
		)

		defer ticker.Stop()

		for {
			select {
			case <-statsCtx.Done():
				return

			case <-ticker.C:
				rate, current := workerStats.Rate(
					previousStats,
				)

				previousStats = current

				active := connectionRegistry.ActiveCount()

				fmt.Printf(
					"[WORKER %s] STATS active=%d RX=%.2f KB/s TX=%.2f KB/s\n",
					workerID,
					active,
					rate.RxBytesPerSec/1024,
					rate.TxBytesPerSec/1024,
				)
			}
		}
	}()

	// ==============================================================
	// WAIT FOR FAILURE / HEALTH STATE
	// ==============================================================

	var fatalErr error

	for {
		select {

		case err := <-socksErrChan:

			fatalErr = fmt.Errorf(
				"socks5 server stopped: %v",
				err,
			)

			goto shutdown

		case health := <-healthResultChan:

			switch health.Status {

			case HealthHealthy:

				fmt.Printf(
					"[WORKER %s] HEALTHY latency=%v\n",
					workerID,
					health.Latency.Round(time.Millisecond),
				)

			case HealthDegraded:

				if health.Err != nil {
					fmt.Printf(
						"[WORKER %s] DEGRADED latency=%v failures=%d err=%v\n",
						workerID,
						health.Latency.Round(time.Millisecond),
						health.Failures,
						health.Err,
					)
				} else {
					fmt.Printf(
						"[WORKER %s] DEGRADED latency=%v active=%d\n",
						workerID,
						health.Latency.Round(time.Millisecond),
						connectionRegistry.ActiveCount(),
					)
				}

				// PENTING:
				//
				// Jangan CloseAll().
				// Jangan tutup SSH.
				// Koneksi yang sedang berjalan dibiarkan.
				//
				// Pada tahap berikutnya status DEGRADED
				// dapat dipakai untuk mencegah koneksi BARU
				// masuk ke worker ini.

				continue

			case HealthDead:

				fatalErr = fmt.Errorf(
					"koneksi SSH mati: failures=%d latency=%v err=%v",
					health.Failures,
					health.Latency.Round(time.Millisecond),
					health.Err,
				)

				goto shutdown
			}
		}
	}

	// ==============================================================
	// WORKER SHUTDOWN
	//
	// Urutan:
	//
	// 1. Stop health monitor
	// 2. Stop stats monitor
	// 3. Close seluruh koneksi HP
	// 4. Close SSH client
	// 5. Close transport
	// 6. Return ke master
	// ==============================================================

shutdown:

	fmt.Printf(
		"[WORKER %s] Shutdown initiated: %v\n",
		workerID,
		fatalErr,
	)

	cancelHealth()
	cancelStats()

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