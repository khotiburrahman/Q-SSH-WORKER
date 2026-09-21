package socks

import (
	"fmt"
	"io"
	"net"
	"strconv"
	"time"

	"github.com/QcomWrt/Q-SSH-WORKER/logger"
	gossh "golang.org/x/crypto/ssh"
)

func HandleRequest(
	clientConn net.Conn,
	sshClient *gossh.Client,
	connectionID string,
	registry *ConnectionRegistry,
) {
	defer func() {
		logger.Debug("[%s] CLIENT CLOSE reason=handler_end", connectionID)
		_ = clientConn.Close()
		registry.Remove(connectionID)
		logger.Debug("[%s] REQUEST END", connectionID)
	}()

	logger.Debug("[%s] REQUEST START remote=%s", connectionID, clientConn.RemoteAddr())

	const handshakeTimeout = 15 * time.Second

	if err := clientConn.SetDeadline(time.Now().Add(handshakeTimeout)); err != nil {
		logger.Debug("[%s] SOCKS DEADLINE SET ERROR: %v", connectionID, err)
	}

	buf := make([]byte, 256)

	// TAHAP 1: NEGOSIASI AUTENTIKASI
	if _, err := io.ReadFull(clientConn, buf[:2]); err != nil {
		logger.Debug("[%s] SOCKS AUTH READ ERROR: %v", connectionID, err)
		return
	}

	if buf[0] != 0x05 {
		logger.Debug("[%s] SOCKS INVALID VERSION=%d", connectionID, buf[0])
		return
	}

	numMethods := int(buf[1])
	if numMethods == 0 || numMethods > 255 {
		logger.Debug("[%s] SOCKS INVALID METHODS=%d", connectionID, numMethods)
		return
	}

	if _, err := io.ReadFull(clientConn, buf[:numMethods]); err != nil {
		logger.Debug("[%s] SOCKS METHODS READ ERROR: %v", connectionID, err)
		return
	}

	if _, err := clientConn.Write([]byte{0x05, 0x00}); err != nil {
		logger.Debug("[%s] SOCKS AUTH RESPONSE ERROR: %v", connectionID, err)
		return
	}

	// TAHAP 2: MEMBACA REQUEST
	if _, err := io.ReadFull(clientConn, buf[:4]); err != nil {
		logger.Debug("[%s] SOCKS REQUEST HEADER ERROR: %v", connectionID, err)
		return
	}

	if buf[0] != 0x05 {
		logger.Debug("[%s] SOCKS REQUEST INVALID VERSION=%d", connectionID, buf[0])
		return
	}

	cmd := buf[1]
	atyp := buf[3]

	if cmd != 0x01 {
		logger.Debug("[%s] SOCKS UNSUPPORTED CMD=0x%02x", connectionID, cmd)
		_, _ = clientConn.Write([]byte{0x05, 0x07, 0x00, 0x01, 0, 0, 0, 0, 0, 0})
		return
	}

	var targetHost string

	switch atyp {
	case 0x01:
		if _, err := io.ReadFull(clientConn, buf[:4]); err != nil {
			logger.Debug("[%s] SOCKS IPV4 READ ERROR: %v", connectionID, err)
			return
		}
		targetHost = net.IP(buf[:4]).String()

	case 0x03:
		if _, err := io.ReadFull(clientConn, buf[:1]); err != nil {
			logger.Debug("[%s] SOCKS DOMAIN LENGTH ERROR: %v", connectionID, err)
			return
		}

		domainLen := int(buf[0])
		if domainLen == 0 || domainLen > 255 {
			logger.Debug("[%s] SOCKS INVALID DOMAIN LENGTH=%d", connectionID, domainLen)
			return
		}

		if _, err := io.ReadFull(clientConn, buf[:domainLen]); err != nil {
			logger.Debug("[%s] SOCKS DOMAIN READ ERROR: %v", connectionID, err)
			return
		}
		targetHost = string(buf[:domainLen])

	default:
		logger.Debug("[%s] SOCKS UNSUPPORTED ADDRESS TYPE=0x%02x", connectionID, atyp)
		_, _ = clientConn.Write([]byte{0x05, 0x08, 0x00, 0x01, 0, 0, 0, 0, 0, 0})
		return
	}

	if _, err := io.ReadFull(clientConn, buf[:2]); err != nil {
		logger.Debug("[%s] SOCKS PORT READ ERROR: %v", connectionID, err)
		return
	}

	targetPort := int(buf[0])<<8 | int(buf[1])
	targetAddr := net.JoinHostPort(targetHost, strconv.Itoa(targetPort))

	logger.Debug("[%s] TARGET %s", connectionID, targetAddr)

	logger.Debug("[%s] SSH CHANNEL OPEN", connectionID)

	sshConn, err := sshClient.Dial("tcp", targetAddr)
	if err != nil {
		logger.Debug("[%s] SSH CHANNEL ERROR target=%s err=%v", connectionID, targetAddr, err)
		_, _ = clientConn.Write([]byte{0x05, 0x03, 0x00, 0x01, 0, 0, 0, 0, 0, 0})
		return
	}

	defer func() {
		logger.Debug("[%s] SSH CHANNEL CLOSE", connectionID)
		_ = sshConn.Close()
	}()

	logger.Debug("[%s] SSH CHANNEL OPENED target=%s", connectionID, targetAddr)

	if err := clientConn.SetDeadline(time.Time{}); err != nil {
		logger.Debug("[%s] SOCKS DEADLINE CLEAR ERROR: %v", connectionID, err)
	}

	if _, err := clientConn.Write([]byte{0x05, 0x00, 0x00, 0x01, 0, 0, 0, 0, 0, 0}); err != nil {
		logger.Debug("[%s] SOCKS SUCCESS RESPONSE ERROR: %v", connectionID, err)
		return
	}

	logger.Debug("[%s] RELAY START", connectionID)

	Relay(clientConn, sshConn, connectionID)

	logger.Debug("[%s] RELAY RETURNED", connectionID)
}

// dummy agar fmt tetap dipakai kalau ada error path
var _ = fmt.Sprintf