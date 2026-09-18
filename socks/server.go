package socks

import (
	"fmt"
	"net"
	"os"
	"strconv"
	"sync"
	"sync/atomic"

	"github.com/QcomWrt/Q-SSH-WORKER/config"
	"github.com/QcomWrt/Q-SSH-WORKER/logger"
	gossh "golang.org/x/crypto/ssh"
)

// ConnectionRegistry menyimpan seluruh koneksi SOCKS aktif
// milik satu worker.
type ConnectionRegistry struct {
	mu        sync.Mutex
	workerID  string
	pid       int
	counter   uint64
	conns     map[string]net.Conn
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
//
// Format:
//
//	W<worker/port>-P<pid>-C<counter>
//
// Contoh:
//
//	W1081-P4217-C000001
func (r *ConnectionRegistry) NewConnectionID() string {
	counter := atomic.AddUint64(&r.counter, 1)

	return fmt.Sprintf(
		"W%s-P%d-C%06d",
		r.workerID,
		r.pid,
		counter,
	)
}

// Add mendaftarkan koneksi aktif.
func (r *ConnectionRegistry) Add(id string, conn net.Conn) {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.conns[id] = conn

	fmt.Printf(
		"[%s] REGISTRY ADD active=%d\n",
		id,
		len(r.conns),
	)
}

// Remove menghapus koneksi dari registry.
func (r *ConnectionRegistry) Remove(id string) {
	r.mu.Lock()
	defer r.mu.Unlock()

	delete(r.conns, id)

	fmt.Printf(
		"[%s] REGISTRY REMOVE active=%d\n",
		id,
		len(r.conns),
	)
}

// CloseAll menutup seluruh koneksi aktif.
//
// Fungsi ini dipanggil ketika worker akan shutdown.
func (r *ConnectionRegistry) CloseAll(reason string) {
	r.mu.Lock()

	// Salin daftar koneksi terlebih dahulu.
	// Jangan melakukan Close sambil mutex masih menjadi
	// satu-satunya sumber akses jika Close memicu callback
	// lain yang mencoba Remove().
	connections := make(map[string]net.Conn, len(r.conns))

	for id, conn := range r.conns {
		connections[id] = conn
	}

	r.mu.Unlock()

	fmt.Printf(
		"[WORKER %s] Closing %d active SOCKS connections, reason=%s\n",
		r.workerID,
		len(connections),
		reason,
	)

	for id, conn := range connections {
		fmt.Printf(
			"[%s] CLIENT CLOSE reason=%s\n",
			id,
			reason,
		)

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
func ListenAndServe(
	cfg *config.Config,
	sshClient *gossh.Client,
	registry *ConnectionRegistry,
) error {

	listenAddr := net.JoinHostPort(
		cfg.Listen.Host,
		strconv.Itoa(cfg.Listen.Port),
	)

	listener, err := net.Listen("tcp", listenAddr)
	if err != nil {
		return err
	}

	defer listener.Close()

	logger.SOCKS5Listening(listenAddr)

	fmt.Printf(
		"[SOCKS] Listening on %s PID=%d worker=%s\n",
		listenAddr,
		os.Getpid(),
		registry.workerID,
	)

	for {
		clientConn, err := listener.Accept()

		if err != nil {
			continue
		}

		// ==============================================================
		// CONNECTION ID DIBUAT TEPAT SAAT ACCEPT BERHASIL
		// ==============================================================

		connectionID := registry.NewConnectionID()

		registry.Add(
			connectionID,
			clientConn,
		)

		fmt.Printf(
			"[%s] ACCEPT remote=%s local=%s\n",
			connectionID,
			clientConn.RemoteAddr(),
			clientConn.LocalAddr(),
		)

		go HandleRequest(
			clientConn,
			sshClient,
			connectionID,
			registry,
		)
	}
}