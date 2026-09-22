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
		logPath          string
		showVersion      bool
		forceDebug       bool
		isChild          bool
	)

	flag.StringVar(&dialPath, "dial", "", "Jalur ke file konfigurasi JSON")
	flag.StringVar(&checkPath, "check", "", "Validasi file konfigurasi JSON")
	flag.StringVar(&showEndpointPath, "show-endpoint", "", "Tampilkan IP server SSH")
	flag.StringVar(&logPath, "log", "", "File log tujuan")
	flag.BoolVar(&showVersion, "version", false, "Info versi")
	flag.BoolVar(&forceDebug, "debug", false, "Aktifkan mode debug")
	flag.BoolVar(&isChild, "child", false, "Flag internal child worker")
	flag.Parse()

	if showVersion {
		fmt.Printf("Q-SSH-WORKER\nVersion : %s\nCommit  : %s\nBuild   : %s\nGo      : %s\n",
			version.Version, version.Commit, version.BuildDate, runtime.Version())
		os.Exit(0)
	}

	if checkPath != "" {
		_, err := config.Load(checkPath)
		if err != nil {
			fmt.Printf("[CHECK ERROR] %v\n", err)
			os.Exit(1)
		}
		fmt.Println("[SUCCESS] Config valid.")
		os.Exit(0)
	}

	if showEndpointPath != "" {
		cfg, err := config.Load(showEndpointPath)
		if err != nil {
			fmt.Printf("[ERROR] %v\n", err)
			os.Exit(1)
		}
		ips, err := net.LookupIP(cfg.SSH.Host)
		if err != nil {
			fmt.Printf("[ERROR] %v\n", err)
			os.Exit(1)
		}
		for _, ip := range ips {
			if ip.To4() != nil {
				fmt.Println(ip.String())
				os.Exit(0)
			}
		}
		fmt.Println("[ERROR] Tidak ada IPv4")
		os.Exit(1)
	}

	if dialPath == "" {
		fmt.Println("Gunakan: ./Q-SSH-WORKER --dial <file.json> [--log <file.log>]")
		os.Exit(1)
	}

	cfg, err := config.Load(dialPath)
	if err != nil {
		fmt.Printf("Gagal memuat konfigurasi: %v\n", err)
		os.Exit(1)
	}

	if logPath != "" {
		if err := logger.RedirectStdio(logPath); err != nil {
			fmt.Fprintf(os.Stderr, "Gagal membuka log %s: %v\n", logPath, err)
			os.Exit(1)
		}
	}

	if forceDebug {
		debug.Enable = true
		logger.DebugEnable = true
	}

	// ---- SIGUSR1: reopen log setelah rotate ----
	go func() {
		sigusr := make(chan os.Signal, 1)
		signal.Notify(sigusr, syscall.SIGUSR1)
		for range sigusr {
			if err := logger.Reopen(); err != nil {
				logger.Errorf("[LOG] gagal reopen: %v", err)
			}
		}
	}()

	// ---- SINGLE WORKER / CHILD ----
	if isChild || !cfg.Worker.Enable {
		err := worker.StartWorker(ctx, cfg)

		if err == nil {
			return
		}

		if errors.Is(err, worker.ErrAuthFailed) {
			logger.Errorf("[FATAL] Kredensial ditolak server. Keluar dengan kode 5.")
			os.Exit(5)
		}

		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			logger.Info("[SHUTDOWN] Worker dihentikan signal.")
			return
		}

		logger.Errorf("Worker exit: %v", err)
		os.Exit(1)
	}

	// ---- MASTER MANAGER ----
	logger.Info("👑 Master Manager (mengelola %d workers)", cfg.Worker.Workers)

	binPath, err := os.Executable()
	if err != nil {
		logger.Errorf("Gagal dapat path biner: %v", err)
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

				logger.Info("[MASTER] Spawning worker port %d", port)

				args := []string{"--dial", dialPath, "--child"}
				if forceDebug {
					args = append(args, "--debug")
				}
				if logPath != "" {
					args = append(args, "--log", logPath)
				}

				cmd := exec.CommandContext(ctx, binPath, args...)
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

				spawnStart := time.Now()
				err := cmd.Run()
				aliveFor := time.Since(spawnStart)

				if ctx.Err() != nil {
					return
				}

				if err != nil {
					if exitErr, ok := err.(*exec.ExitError); ok {
						if exitErr.ExitCode() == 5 {
							logger.Errorf(
								"❌ Child worker port %d exit 5 (auth gagal). Menghentikan master.",
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

				// Backoff adaptif: kalau anak hidup lama, respawn cepat.
				// Kalau anak cepat mati, tunggu lebih lama.
				var delay time.Duration
				switch {
				case aliveFor > 5*time.Minute:
					delay = 3 * time.Second
				case aliveFor > 30*time.Second:
					delay = 5 * time.Second
				case aliveFor > 5*time.Second:
					delay = 15 * time.Second
				default:
					delay = 30 * time.Second
				}

				logger.Warn(
					"⚠️ Worker port %d mati setelah %v. Respawn dalam %v.",
					port, aliveFor.Round(time.Second), delay,
				)

				select {
				case <-time.After(delay):
				case <-ctx.Done():
					return
				}
			}
		}(targetPort)

		select {
		case <-time.After(3 * time.Second):
		case <-ctx.Done():
			return
		}
	}

	select {
	case <-ctx.Done():
		logger.Info("[MASTER] Signal diterima, menghentikan semua worker...")
	case err := <-childFatal:
		logger.Errorf("[MASTER] Fatal dari child: %v. Keluar.", err)
		stop()
		time.Sleep(500 * time.Millisecond)
		os.Exit(1)
	}
}