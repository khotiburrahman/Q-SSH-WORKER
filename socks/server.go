package socks

import (
	"context"
	"fmt"
	"net"
	"os"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/QcomWrt/Q-SSH-WORKER/config"
	"github.com/QcomWrt/Q-SSH-WORKER/logger"
	gossh "golang.org/x/crypto/ssh"
)

// ConnectionRegistry menyimpan seluruh koneksi SOCKS aktif milik satu worker.
type ConnectionRegistry struct {
	mu       sync.Mutex
	workerID string
	pid      int
	counter  uint64
	conns    map[string]net.Conn
}

// NewConnectionRegistry membuat registry baru untuk satu worker.
func NewConnectionRegistry(workerID string) *ConnectionRegistry {
	return &ConnectionRegistry{
		workerID: workerID,
		pid:      os.Getpid(),
		conns:    make(map[string]net.Conn),
	}
}

// NewConnectionID membuat ID unik untuk setiap koneksi.
func (r *ConnectionRegistry) NewConnectionID() string {
	counter := atomic.AddUint64(&r.counter, 1)
	return fmt.Sprintf("W%s-P%d-C%06d", r.workerID, r.pid, counter)
}

// Add mendaftarkan koneksi aktif.
func (r *ConnectionRegistry) Add(id string, conn net.Conn) {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.conns[id] = conn
	logger.Debug("[%s] REGISTRY ADD active=%d", id, len(r.conns))
}

// Remove menghapus koneksi dari registry.
func (r *ConnectionRegistry) Remove(id string) {
	r.mu.Lock()
	defer r.mu.Unlock()

	delete(r.conns, id)
	logger.Debug("[%s] REGISTRY REMOVE active=%d", id, len(r.conns))
}

// CloseAll menutup seluruh koneksi aktif.
func (r *ConnectionRegistry) CloseAll(reason string) {
	r.mu.Lock()

	connections := make(map[string]net.Conn, len(r.conns))
	for id, conn := range r.conns {
		connections[id] = conn
	}

	r.mu.Unlock()

	if len(connections) == 0 {
		return
	}

	logger.Info("[WORKER %s] Closing %d active SOCKS connections, reason=%s",
		r.workerID, len(connections), reason)

	for id, conn := range connections {
		logger.Debug("[%s] CLIENT CLOSE reason=%s", id, reason)
		_ = conn.Close()
	}
}

// ActiveCount mengembalikan jumlah koneksi aktif.
func (r *ConnectionRegistry) ActiveCount() int {
	r.mu.Lock()
	defer r.mu.Unlock()

	return len(r.conns)
}

// ListenAndServe menjalankan SOCKS5 server.
//
// Listener akan ditutup otomatis ketika ctx dibatalkan, sehingga
// port tidak tertinggal saat worker loop reconnect.
func ListenAndServe(
	ctx context.Context,
	cfg *config.Config,
	sshClient *gossh.Client,
	registry *ConnectionRegistry,
) error {
	listenAddr := net.JoinHostPort(
		cfg.Listen.Host,
		strconv.Itoa(cfg.Listen.Port),
	)

	// Retry bind beberapa kali untuk mengatasi TIME_WAIT atau
	// listener lama yang belum benar-benar close.
	var listener net.Listener
	var err error

	for attempt := 0; attempt < 5; attempt++ {
		listener, err = net.Listen("tcp", listenAddr)
		if err == nil {
			break
		}

		logger.Warn(
			"[SOCKS] bind %s gagal (attempt %d/5): %v",
			listenAddr, attempt+1, err,
		)

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(2 * time.Second):
		}
	}

	if err != nil {
		return fmt.Errorf("bind %s failed: %w", listenAddr, err)
	}
	defer listener.Close()

	// Tutup listener saat ctx dibatalkan
	go func() {
		<-ctx.Done()
		_ = listener.Close()
	}()

	logger.SOCKS5Listening(listenAddr)

	logger.Info("[SOCKS] Listening on %s PID=%d worker=%s",
		listenAddr, os.Getpid(), registry.workerID)

	for {
		clientConn, err := listener.Accept()
		if err != nil {
			// Kalau ctx dibatalkan, return ctx.Err() bersih
			select {
			case <-ctx.Done():
				return ctx.Err()
			default:
			}
			return fmt.Errorf("accept failed: %w", err)
		}

		connectionID := registry.NewConnectionID()
		registry.Add(connectionID, clientConn)

		logger.Debug("[%s] ACCEPT remote=%s local=%s",
			connectionID, clientConn.RemoteAddr(), clientConn.LocalAddr())

		go HandleRequest(clientConn, sshClient, connectionID, registry)
	}
}