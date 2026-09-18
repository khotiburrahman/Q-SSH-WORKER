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

// Relay melakukan relay data dua arah antara client SOCKS
// dan SSH channel.
//
// Lifecycle:
//
//	client <-> SSH channel
//	    │
//	    ├── CLIENT_TO_REMOTE
//	    │
//	    └── REMOTE_TO_CLIENT
//
// Jika salah satu arah selesai/error, kedua koneksi ditutup
// agar arah lainnya tidak menggantung.
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
	// CLIENT → SSH → VPS → INTERNET
	// ==============================================================

	go func() {
		fmt.Printf(
			"[%s] RELAY COPY START direction=CLIENT_TO_REMOTE\n",
			connectionID,
		)

		n, err := io.Copy(remote, client)

		resultChan <- relayResult{
			direction: "CLIENT_TO_REMOTE",
			bytes:     n,
			err:       err,
		}
	}()

	// ==============================================================
	// ARAH 2:
	// INTERNET → VPS → SSH → CLIENT
	// ==============================================================

	go func() {
		fmt.Printf(
			"[%s] RELAY COPY START direction=REMOTE_TO_CLIENT\n",
			connectionID,
		)

		n, err := io.Copy(client, remote)

		resultChan <- relayResult{
			direction: "REMOTE_TO_CLIENT",
			bytes:     n,
			err:       err,
		}
	}()

	// ==============================================================
	// TUNGGU ARAH PERTAMA
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
	// TUTUP KEDUA SISI
	//
	// Ini memaksa io.Copy arah kedua keluar dari blocking read/write.
	// ==============================================================

	fmt.Printf(
		"[%s] RELAY FORCE CLOSE BOTH DIRECTIONS\n",
		connectionID,
	)

	_ = client.Close()
	_ = remote.Close()

	// ==============================================================
	// TUNGGU ARAH KEDUA
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