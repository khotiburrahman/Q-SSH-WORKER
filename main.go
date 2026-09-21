package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"net"
	"os"
	"os/exec"
	"os/signal"
	"runtime"
	"syscall"
	"time"

	"github.com/QcomWrt/Q-SSH-WORKER/config"
	"github.com/QcomWrt/Q-SSH-WORKER/debug"
	"github.com/QcomWrt/Q-SSH-WORKER/logger"
	"github.com/QcomWrt/Q-SSH-WORKER/version"
	"github.com/QcomWrt/Q-SSH-WORKER/worker"
)

func main() {
	ctx, stop := signal.NotifyContext(
		context.Background(),
		os.Interrupt,
		syscall.SIGTERM,
	)
	defer stop()

	var (
		dialPath         string
		checkPath        string
		showEndpointPath string
		showVersion      bool
		forceDebug       bool
		isChild          bool
	)

	flag.StringVar(&dialPath, "dial", "", "Jalur ke file konfigurasi JSON untuk terhubung ke SSH")
	flag.StringVar(&checkPath, "check", "", "Hanya memvalidasi sintaks file konfigurasi JSON")
	flag.StringVar(&showEndpointPath, "show-endpoint", "", "Ambil detail IP Server VPS untuk keperluan routing bypass")
	flag.BoolVar(&showVersion, "version", false, "Menampilkan informasi versi biner Q-SSH-WORKER")
	flag.BoolVar(&forceDebug, "debug", false, "Memaksa mengaktifkan mode debug secara manual via CLI")
	flag.BoolVar(&isChild, "child", false, "Flag internal penanda proses child-worker")

	flag.Parse()

	// 1. HANDLER: --version
	if showVersion {
		fmt.Printf("Q-SSH-WORKER\n")
		fmt.Printf("Version : %s\n", version.Version)
		fmt.Printf("Commit  : %s\n", version.Commit)
		fmt.Printf("Build   : %s\n", version.BuildDate)
		fmt.Printf("Go      : %s\n", runtime.Version())
		os.Exit(0)
	}

	// 2. HANDLER: --check
	if checkPath != "" {
		_, err := config.Load(checkPath)
		if err != nil {
			fmt.Printf("[CHECK ERROR] File konfigurasi kotor/invalid: %v\n", err)
			os.Exit(1)
		}
		fmt.Println("[SUCCESS] File konfigurasi valid.")
		os.Exit(0)
	}

	// 3. HANDLER: --show-endpoint
	if showEndpointPath != "" {
		cfg, err := config.Load(showEndpointPath)
		if err != nil {
			fmt.Printf("[ERROR] Gagal memuat config: %v\n", err)
			os.Exit(1)
		}

		ips, err := net.LookupIP(cfg.SSH.Host)
		if err != nil {
			fmt.Printf("[ERROR] Gagal resolve DNS host %s: %v\n", cfg.SSH.Host, err)
			os.Exit(1)
		}

		var targetIP string
		for _, ip := range ips {
			if ip.To4() != nil {
				targetIP = ip.String()
				break
			}
		}

		if targetIP == "" {
			fmt.Println("[ERROR] IP IPv4 tidak ditemukan untuk host tersebut.")
			os.Exit(1)
		}

		fmt.Printf("%s\n", targetIP)
		os.Exit(0)
	}

	// 4. HANDLER: --dial
	if dialPath == "" {
		fmt.Println("Gunakan perintah:")
		fmt.Println("  ./Q-SSH-WORKER --dial <file.json>")
		fmt.Println("  ./Q-SSH-WORKER --show-endpoint <file.json>")
		os.Exit(1)
	}

	cfg, err := config.Load(dialPath)
	if err != nil {
		fmt.Printf("Gagal memuat konfigurasi: %v\n", err)
		os.Exit(1)
	}

	if forceDebug {
		debug.Enable = true
		logger.DebugEnable = true
	}

	// ======================================================================
	// JALUR A: CHILD ATAU SINGLE WORKER
	// ======================================================================

	if isChild || !cfg.Worker.Enable {
		err := worker.StartWorker(ctx, cfg)

		if err == nil {
			return
		}

		if errors.Is(err, worker.ErrAuthFailed) {
			fmt.Println("\n🛑 [FATAL - AGENT KILLED] Kredensial SSH/payload ditolak server.")
			os.Exit(5)
		}

		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			fmt.Println("[SHUTDOWN] Worker dihentikan oleh signal.")
			return
		}

		fmt.Printf("Worker Error: %v\n", err)
		os.Exit(1)
	}

	// ======================================================================
	// JALUR B: MASTER MANAGER
	// ======================================================================

	fmt.Printf("👑 Q-SSH-WORKER bertindak sebagai Master Manager (Menjaga %d Workers)\n", cfg.Worker.Workers)

	binPath, err := os.Executable()
	if err != nil {
		fmt.Printf("Gagal mendeteksi executable path biner: %v\n", err)
		os.Exit(1)
	}

	childFatal := make(chan error, 1)

	for i := 0; i < cfg.Worker.Workers; i++ {
		targetPort := cfg.Worker.StartPort + i

		go func(port int) {
			for {
				select {
				case <-ctx.Done():
					return
				default:
				}

				fmt.Printf("[MASTER] Spawning Child Worker untuk port %d...\n", port)

				args := []string{"--dial", dialPath, "--child"}
				if forceDebug {
					args = append(args, "--debug")
				}

				cmd := exec.CommandContext(ctx, binPath, args...)

				// Kirim SIGTERM dulu, bukan SIGKILL, supaya child bisa cleanup.
				cmd.Cancel = func() error {
					if cmd.Process == nil {
						return nil
					}
					return cmd.Process.Signal(syscall.SIGTERM)
				}
				cmd.WaitDelay = 5 * time.Second

				cmd.Env = append(
					os.Environ(),
					fmt.Sprintf("QTUN_TARGET_PORT=%d", port),
				)

				cmd.Stdout = os.Stdout
				cmd.Stderr = os.Stderr

				err := cmd.Run()

				if ctx.Err() != nil {
					return
				}

				if err != nil {
					if exitError, ok := err.(*exec.ExitError); ok {
						if exitError.ExitCode() == 5 {
							fmt.Printf(
								"\n❌ [MASTER] Child worker port %d exit 5 (auth failed). Menghentikan Master Manager.\n",
								port,
							)

							select {
							case childFatal <- err:
							default:
							}
							return
						}
					}
				}

				fmt.Printf(
					"⚠️ Worker port %d terputus gantung (EOF/Mati)! Membangunkan ulang dalam 3 detik...\n",
					port,
				)

				select {
				case <-time.After(3 * time.Second):
				case <-ctx.Done():
					return
				}
			}
		}(targetPort)

		// Jeda antar spawn awal
		select {
		case <-time.After(3 * time.Second):
		case <-ctx.Done():
			return
		}
	}

	select {
	case <-ctx.Done():
		fmt.Println("[MASTER] Signal diterima, menghentikan semua child worker...")
	case err := <-childFatal:
		fmt.Printf("[MASTER] Fatal error dari child: %v. Keluar.\n", err)
		stop()
		time.Sleep(500 * time.Millisecond)
		os.Exit(1)
	}
}