package socks

import (
	"fmt"
	"io"
	"net"
)

// relayResult menyimpan hasil satu arah relay.
type relayResult struct {
	direction string
	bytes     int64
	err       error
}

// Relay melakukan penyalinan data timbal balik
// secara bidirectional.
func Relay(
	client net.Conn,
	remote net.Conn,
	connectionID string,
) {

	resultChan := make(chan relayResult, 2)

	fmt.Printf(
		"[%s] RELAY START client=%s remote=%s\n",
		connectionID,
		client.RemoteAddr(),
		remote.RemoteAddr(),
	)

	// ==============================================================
	// ARAH 1:
	// HP / CLIENT → SSH → VPS → INTERNET
	// ==============================================================

	go func() {
		n, err := io.Copy(
			remote,
			client,
		)

		resultChan <- relayResult{
			direction: "CLIENT_TO_REMOTE",
			bytes:     n,
			err:       err,
		}
	}()

	// ==============================================================
	// ARAH 2:
	// INTERNET → VPS → SSH → HP / CLIENT
	// ==============================================================

	go func() {
		n, err := io.Copy(
			client,
			remote,
		)

		resultChan <- relayResult{
			direction: "REMOTE_TO_CLIENT",
			bytes:     n,
			err:       err,
		}
	}()

	// ==============================================================
	// TUNGGU ARAH PERTAMA SELESAI
	// ==============================================================

	first := <-resultChan

	fmt.Printf(
		"[%s] RELAY FIRST END direction=%s bytes=%d err=%v\n",
		connectionID,
		first.direction,
		first.bytes,
		first.err,
	)

	// ==============================================================
	// PENTING:
	//
	// Jika salah satu arah selesai, paksa kedua socket ditutup.
	//
	// Ini memastikan goroutine io.Copy arah lainnya
	// tidak menggantung selamanya.
	// ==============================================================

	fmt.Printf(
		"[%s] RELAY FORCE CLOSE BOTH DIRECTIONS\n",
		connectionID,
	)

	_ = client.Close()
	_ = remote.Close()

	// ==============================================================
	// TUNGGU ARAH KEDUA BENAR-BENAR SELESAI
	// ==============================================================

	second := <-resultChan

	fmt.Printf(
		"[%s] RELAY SECOND END direction=%s bytes=%d err=%v\n",
		connectionID,
		second.direction,
		second.bytes,
		second.err,
	)

	fmt.Printf(
		"[%s] RELAY END\n",
		connectionID,
	)
}