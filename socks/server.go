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

type ConnectionRegistry struct {
	mu       sync.Mutex
	workerID string
	pid      int
	counter  uint64
	conns    map[string]net.Conn
}

func NewConnectionRegistry(workerID string) *ConnectionRegistry {
	return &ConnectionRegistry{
		workerID: workerID,
		pid:      os.Getpid(),
		conns:    make(map[string]net.Conn),
	}
}

func (r *ConnectionRegistry) NewConnectionID() string {
	counter := atomic.AddUint64(&r.counter, 1)
	return fmt.Sprintf("W%s-P%d-C%06d", r.workerID, r.pid, counter)
}

func (r *ConnectionRegistry) Add(id string, conn net.Conn) {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.conns[id] = conn
	logger.Debug("[%s] REGISTRY ADD active=%d", id, len(r.conns))
}

func (r *ConnectionRegistry) Remove(id string) {
	r.mu.Lock()
	defer r.mu.Unlock()

	delete(r.conns, id)
	logger.Debug("[%s] REGISTRY REMOVE active=%d", id, len(r.conns))
}

func (r *ConnectionRegistry) CloseAll(reason string) {
	r.mu.Lock()

	connections := make(map[string]net.Conn, len(r.conns))
	for id, conn := range r.conns {
		connections[id] = conn
	}

	r.mu.Unlock()

	logger.Info("[WORKER %s] Closing %d active SOCKS connections, reason=%s",
		r.workerID, len(connections), reason)

	for id, conn := range connections {
		logger.Debug("[%s] CLIENT CLOSE reason=%s", id, reason)
		_ = conn.Close()
	}
}

func (r *ConnectionRegistry) ActiveCount() int {
	r.mu.Lock()
	defer r.mu.Unlock()

	return len(r.conns)
}

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

	logger.Info("[SOCKS] Listening on %s PID=%d worker=%s",
		listenAddr, os.Getpid(), registry.workerID)

	for {
		clientConn, err := listener.Accept()
		if err != nil {
			return fmt.Errorf("accept failed: %w", err)
		}

		connectionID := registry.NewConnectionID()
		registry.Add(connectionID, clientConn)

		logger.Debug("[%s] ACCEPT remote=%s local=%s",
			connectionID, clientConn.RemoteAddr(), clientConn.LocalAddr())

		go HandleRequest(clientConn, sshClient, connectionID, registry)
	}
}