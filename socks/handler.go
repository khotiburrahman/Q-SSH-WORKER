package socks

import (
	"fmt"
	"io"
	"net"
	"strconv"
	"time"

	gossh "golang.org/x/crypto/ssh"
)

// HandleRequest memproses handshake SOCKS5
// dan menghubungkannya ke terowongan SSH.
func HandleRequest(
	clientConn net.Conn,
	sshClient *gossh.Client,
	connectionID string,
	registry *ConnectionRegistry,
) {
	defer func() {
		fmt.Printf(
			"[%s] CLIENT CLOSE reason=handler_end\n",
			connectionID,
		)

		_ = clientConn.Close()

		registry.Remove(connectionID)

		fmt.Printf(
			"[%s] REQUEST END\n",
			connectionID,
		)
	}()

	fmt.Printf(
		"[%s] REQUEST START remote=%s\n",
		connectionID,
		clientConn.RemoteAddr(),
	)

	// ==============================================================
	// DEADLINE KHUSUS SOCKS HANDSHAKE
	//
	// Hanya berlaku sebelum koneksi masuk ke relay.
	// Setelah SSH channel berhasil dan response SOCKS dikirim,
	// deadline dihapus.
	// ==============================================================

	const handshakeTimeout = 15 * time.Second

	if err := clientConn.SetDeadline(
		time.Now().Add(handshakeTimeout),
	); err != nil {
		fmt.Printf(
			"[%s] SOCKS DEADLINE SET ERROR: %v\n",
			connectionID,
			err,
		)
	}

	buf := make([]byte, 256)

	// ==============================================================
	// TAHAP 1: NEGOSIASI AUTENTIKASI
	// ==============================================================

	if _, err := io.ReadFull(clientConn, buf[:2]); err != nil {
		fmt.Printf(
			"[%s] SOCKS AUTH READ ERROR: %v\n",
			connectionID,
			err,
		)

		return
	}

	if buf[0] != 0x05 {
		fmt.Printf(
			"[%s] SOCKS INVALID VERSION=%d\n",
			connectionID,
			buf[0],
		)

		return
	}

	numMethods := int(buf[1])

	if numMethods == 0 || numMethods > 255 {
		fmt.Printf(
			"[%s] SOCKS INVALID METHODS=%d\n",
			connectionID,
			numMethods,
		)

		return
	}

	if _, err := io.ReadFull(clientConn, buf[:numMethods]); err != nil {
		fmt.Printf(
			"[%s] SOCKS METHODS READ ERROR: %v\n",
			connectionID,
			err,
		)

		return
	}

	// NO AUTHENTICATION REQUIRED
	if _, err := clientConn.Write([]byte{
		0x05,
		0x00,
	}); err != nil {
		fmt.Printf(
			"[%s] SOCKS AUTH RESPONSE ERROR: %v\n",
			connectionID,
			err,
		)

		return
	}

	// ==============================================================
	// TAHAP 2: MEMBACA REQUEST PERINTAH
	// ==============================================================

	if _, err := io.ReadFull(clientConn, buf[:4]); err != nil {
		fmt.Printf(
			"[%s] SOCKS REQUEST HEADER ERROR: %v\n",
			connectionID,
			err,
		)

		return
	}

	if buf[0] != 0x05 {
		fmt.Printf(
			"[%s] SOCKS REQUEST INVALID VERSION=%d\n",
			connectionID,
			buf[0],
		)

		return
	}

	cmd := buf[1]
	atyp := buf[3]

	if cmd != 0x01 {
		fmt.Printf(
			"[%s] SOCKS UNSUPPORTED CMD=0x%02x\n",
			connectionID,
			cmd,
		)

		_, _ = clientConn.Write([]byte{
			0x05,
			0x07,
			0x00,
			0x01,
			0,
			0,
			0,
			0,
			0,
			0,
		})

		return
	}

	var targetHost string

	switch atyp {

	case 0x01:
		// ==========================================================
		// IPv4
		// ==========================================================

		if _, err := io.ReadFull(clientConn, buf[:4]); err != nil {
			fmt.Printf(
				"[%s] SOCKS IPV4 READ ERROR: %v\n",
				connectionID,
				err,
			)

			return
		}

		targetHost = net.IP(buf[:4]).String()

	case 0x03:
		// ==========================================================
		// DOMAIN
		// ==========================================================

		if _, err := io.ReadFull(clientConn, buf[:1]); err != nil {
			fmt.Printf(
				"[%s] SOCKS DOMAIN LENGTH ERROR: %v\n",
				connectionID,
				err,
			)

			return
		}

		domainLen := int(buf[0])

		if domainLen == 0 || domainLen > 255 {
			fmt.Printf(
				"[%s] SOCKS INVALID DOMAIN LENGTH=%d\n",
				connectionID,
				domainLen,
			)

			return
		}

		if _, err := io.ReadFull(
			clientConn,
			buf[:domainLen],
		); err != nil {
			fmt.Printf(
				"[%s] SOCKS DOMAIN READ ERROR: %v\n",
				connectionID,
				err,
			)

			return
		}

		targetHost = string(buf[:domainLen])

	default:
		fmt.Printf(
			"[%s] SOCKS UNSUPPORTED ADDRESS TYPE=0x%02x\n",
			connectionID,
			atyp,
		)

		_, _ = clientConn.Write([]byte{
			0x05,
			0x08,
			0x00,
			0x01,
			0,
			0,
			0,
			0,
			0,
			0,
		})

		return
	}

	// ==============================================================
	// MEMBACA PORT
	// ==============================================================

	if _, err := io.ReadFull(clientConn, buf[:2]); err != nil {
		fmt.Printf(
			"[%s] SOCKS PORT READ ERROR: %v\n",
			connectionID,
			err,
		)

		return
	}

	targetPort := int(buf[0])<<8 | int(buf[1])

	targetAddr := net.JoinHostPort(
		targetHost,
		strconv.Itoa(targetPort),
	)

	fmt.Printf(
		"[%s] TARGET %s\n",
		connectionID,
		targetAddr,
	)

	// ==============================================================
	// TAHAP 3: DIAL VIA SSH
	// ==============================================================

	fmt.Printf(
		"[%s] SSH CHANNEL OPEN\n",
		connectionID,
	)

	sshConn, err := sshClient.Dial(
		"tcp",
		targetAddr,
	)

	if err != nil {
		fmt.Printf(
			"[%s] SSH CHANNEL ERROR target=%s err=%v\n",
			connectionID,
			targetAddr,
			err,
		)

		_, _ = clientConn.Write([]byte{
			0x05,
			0x03,
			0x00,
			0x01,
			0,
			0,
			0,
			0,
			0,
			0,
		})

		return
	}

	defer func() {
		fmt.Printf(
			"[%s] SSH CHANNEL CLOSE\n",
			connectionID,
		)

		_ = sshConn.Close()
	}()

	fmt.Printf(
		"[%s] SSH CHANNEL OPENED target=%s\n",
		connectionID,
		targetAddr,
	)

	// ==============================================================
	// PENTING:
	// Hapus deadline handshake.
	//
	// Setelah titik ini koneksi boleh idle selama apa pun.
	// ==============================================================

	if err := clientConn.SetDeadline(time.Time{}); err != nil {
		fmt.Printf(
			"[%s] SOCKS DEADLINE CLEAR ERROR: %v\n",
			connectionID,
			err,
		)
	}

	// ==============================================================
	// KIRIM RESPONSE SOCKS SUCCESS
	// ==============================================================

	if _, err := clientConn.Write([]byte{
		0x05,
		0x00,
		0x00,
		0x01,
		0,
		0,
		0,
		0,
		0,
		0,
	}); err != nil {
		fmt.Printf(
			"[%s] SOCKS SUCCESS RESPONSE ERROR: %v\n",
			connectionID,
			err,
		)

		return
	}

	// ==============================================================
	// TAHAP 4: RELAY
	// ==============================================================

	fmt.Printf(
		"[%s] RELAY START\n",
		connectionID,
	)

	Relay(
		clientConn,
		sshConn,
		connectionID,
	)

	fmt.Printf(
		"[%s] RELAY RETURNED\n",
		connectionID,
	)
}