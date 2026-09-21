package socks

import (
	"io"
	"net"

	"github.com/QcomWrt/Q-SSH-WORKER/internal"
	"github.com/QcomWrt/Q-SSH-WORKER/logger"
)

type relayResult struct {
	direction string
	bytes     int64
	err       error
}

func Relay(
	client net.Conn,
	remote net.Conn,
	connectionID string,
) {
	resultChan := make(chan relayResult, 2)

	logger.Debug("[%s] RELAY START client=%s remote=%s",
		connectionID, client.RemoteAddr(), remote.RemoteAddr())

	copyWithPool := func(dst net.Conn, src net.Conn, direction string) {
		logger.Debug("[%s] RELAY COPY START direction=%s", connectionID, direction)

		buf := internal.GetBuffer()
		defer internal.PutBuffer(buf)

		n, err := io.CopyBuffer(dst, src, buf)

		resultChan <- relayResult{direction: direction, bytes: n, err: err}
	}

	go copyWithPool(remote, client, "CLIENT_TO_REMOTE")
	go copyWithPool(client, remote, "REMOTE_TO_CLIENT")

	first := <-resultChan
	logger.Debug("[%s] RELAY FIRST END direction=%s bytes=%d err=%v",
		connectionID, first.direction, first.bytes, first.err)

	logger.Debug("[%s] RELAY FORCE CLOSE BOTH DIRECTIONS", connectionID)

	_ = client.Close()
	_ = remote.Close()

	second := <-resultChan
	logger.Debug("[%s] RELAY SECOND END direction=%s bytes=%d err=%v",
		connectionID, second.direction, second.bytes, second.err)

	logger.Debug("[%s] RELAY END", connectionID)
}