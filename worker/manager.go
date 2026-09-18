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

// StartWorker mengelola inisialisasi tunggal dengan kendali reconnect internal di awal dial.
//
// Jika terjadi kegagalan fatal di tengah jalan saat terowongan aktif, ia akan keluar
// agar siklus recovery diambil alih penuh oleh watchdog.sh OpenWrt atau Master Manager biner.
func StartWorker(cfg *config.Config) error {
	// Inisialisasi kebijakan jeda koneksi ulang untuk mengamankan proses dial awal
	reconnectPolicy := NewReconnectPolicy(2*time.Second, 30*time.Second)

	n, err := network.New(cfg)
	if err != nil {
		return fmt.Errorf("network init failed: %w", err)
	}

	var conn net.Conn

	// ======================================================================
	// CETAK LOG HANYA 1 KALI SAAT START DI LUAR LOOP
	// ======================================================================
	if cfg.Proxy.Host != "" {
		logger.ProxyConnecting()
	} else {
		logger.TCPConnecting()
	}

	// Loop khusus pemicu dial awal sampai sukses terhubung
	for {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)

		// 1. Dial Connection
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

		// Keluar dari loop jika koneksi soket dasar berhasil terbentuk
		break
	}

	// Reset hitungan kegagalan backoff karena dial dasar sukses
	reconnectPolicy.Reset()

	// 2. Bungkus koneksi dengan observer statistik Rx/Tx
	workerStats := &TrafficStats{}
	conn = NewObservedConn(conn, workerStats)

	// ======================================================================
	// 3. TRANSPORT LAYER INJECTION
	// ======================================================================
	wrappedConn, err := transport.Wrap(cfg, conn)
	if err != nil {
		// Jika handshake payload gagal/ditutup CDN,
		// pastikan socket dasar ditutup
		if conn != nil {
			conn.Close()
		}

		logger.ProxyError(err)

		return err
	}

	// Salin koneksi yang berhasil dimanipulasi ke variabel utama
	conn = wrappedConn

	// ======================================================================
	// AMAN DARI LOG GANDA:
	// DICETAK HANYA SETELAH JABAT TANGAN PAYLOAD SUKSES
	// ======================================================================
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

	// 4. Jabat Tangan / Handshake Protokol SSH
	client, err := workerssh.Dial(cfg, conn)
	if err != nil {
		logger.SSHError(err)

		// INTERSEPSI ERROR KREDENTIAL SSH
		errStr := strings.ToLower(err.Error())

		if strings.Contains(errStr, "handshake") ||
			strings.Contains(errStr, "auth") ||
			strings.Contains(errStr, "credential") ||
			strings.Contains(errStr, "password") ||
			strings.Contains(errStr, "sign") ||
			strings.Contains(errStr, "illegal") ||
			strings.Contains(errStr, "rejected") {

			println("\n🛑 [FATAL - AGENT KILLED] Kredensial SSH ditolak oleh server Dropbear!")
			println("💡 Info: Akun sudah expired atau password salah. Memaksa mematikan Master Process...")

			os.Exit(5)
		}

		return err
	}

	logger.SSHConnected()

	// Ambil IP SSH untuk keperluan cetak log debug
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

	// ======================================================================
	// 5. CONNECTION REGISTRY
	//
	// Registry menyimpan semua koneksi HP yang sedang aktif pada worker ini.
	// Ketika worker mati, seluruh koneksi lokal tersebut akan ditutup.
	// ======================================================================

	workerID := os.Getenv("QTUN_TARGET_PORT")

	if workerID == "" {
		workerID = "unknown"
	}

	connectionRegistry := socks.NewConnectionRegistry(workerID)

	fmt.Printf(
		"[WORKER %s] Connection registry initialized (PID=%d)\n",
		workerID,
		os.Getpid(),
	)

	// ======================================================================
	// 6. JALANKAN SERVER SOCKS5
	// ======================================================================

	socksErrChan := make(chan error, 1)

	go func() {
		// Periksa apakah Master Manager mengirimkan port spesifik
		// via Environment Variable
		if envPort := os.Getenv("QTUN_TARGET_PORT"); envPort != "" {
			var dynamicPort int

			// Parse string port menjadi integer
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

	// ======================================================================
	// 7. JALANKAN LIVENESS CHECK VIA MonitorHealth
	// ======================================================================

	healthCtx, cancelHealth := context.WithCancel(context.Background())
	healthFailChan := make(chan bool, 1)

	go MonitorHealth(
		healthCtx,
		client,
		healthFailChan,
	)

	// Menahan proses tetap hidup melayani data.
	// Jika salah satu pemicu aktif, matikan worker.
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

	// ======================================================================
	// 8. SHUTDOWN WORKER
	//
	// URUTANNYA PENTING:
	//
	// 1. Stop health monitor
	// 2. Tutup semua koneksi HP
	// 3. Tutup SSH client
	// 4. Tutup transport connection
	// 5. Return agar Master melakukan respawn
	// ======================================================================

	fmt.Printf(
		"[WORKER %s] Shutdown initiated: %v\n",
		workerID,
		fatalErr,
	)

	cancelHealth()

	// Tutup seluruh koneksi HP yang masih aktif.
	// Ini bagian penting untuk mencegah socket lama
	// tetap menggantung ketika worker mati.
	connectionRegistry.CloseAll("worker_shutdown")

	// Setelah koneksi lokal ditutup, tutup SSH.
	if client != nil {
		client.Close()
	}

	// Terakhir tutup koneksi transport utama.
	if conn != nil {
		conn.Close()
	}

	fmt.Printf(
		"[WORKER %s] Shutdown complete (PID=%d)\n",
		workerID,
		os.Getpid(),
	)

	// Kembalikan error agar ditangkap main.go
	// untuk memicu respawn worker.
	return fatalErr
}