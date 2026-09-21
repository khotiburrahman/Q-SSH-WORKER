package response

import (
	"bufio"
	"errors"
	"io"
	"net"
	"strconv"
	"strings"
	"time"

	"github.com/QcomWrt/Q-SSH-WORKER/config"
	"github.com/QcomWrt/Q-SSH-WORKER/debug"
	"github.com/QcomWrt/Q-SSH-WORKER/internal"
)

// ErrAuthFailed menandakan kredensial ditolak oleh server (baik dari payload HTTP
// maupun dari SSH handshake). Worker manager akan menerjemahkan ini menjadi
// exit code 5 agar Master dapat mematikan seluruh worker sekaligus.
var ErrAuthFailed = errors.New("auth_failed")

func Once(cfg *config.Config, conn net.Conn, secondPart string) (net.Conn, error) {
	reader := bufio.NewReader(conn)

	// 1. Baca baris status pertama (HTTP Status Line)
	line, err := reader.ReadString('\n')
	if err != nil {
		return nil, err
	}
	line = strings.TrimRight(line, "\r\n")

	parts := strings.SplitN(line, " ", 3)
	version := "HTTP/1.1"
	statusCode := 0
	status := ""
	if len(parts) >= 2 {
		version = parts[0]
		statusCode, _ = strconv.Atoi(parts[1])
		if len(parts) == 3 {
			status = parts[2]
		}
	} else {
		status = line
	}

	// 2. Baca dan kumpulkan seluruh HTTP Headers
	headers := make(map[string]string)
	for {
		hl, err := reader.ReadString('\n')
		if err != nil {
			return nil, err
		}
		trimmedHl := strings.TrimRight(hl, "\r\n")
		if trimmedHl == "" {
			break
		}

		if idx := strings.Index(trimmedHl, ":"); idx != -1 {
			k := strings.TrimSpace(trimmedHl[:idx])
			v := strings.TrimSpace(trimmedHl[idx+1:])
			headers[k] = v
		}
	}

	debug.Response(version, statusCode, status, headers)

	// SKENARIO A: RESPONS LANGSUNG SUKSES (101 / 200)
	if statusCode == 101 || statusCode == 200 {
		if debug.Enable {
			debug.Println("[SUCCESS] Terhubung ke gerbang WebSocket! Menguras header respons...")
			debug.Println("[SUCCESS] Buffer HTTP Bersih! Membaca Banner SSH...")
		}

		sshBanner, err := reader.ReadString('\n')
		if err != nil {
			return nil, err
		}

		debug.SSHBanner(sshBanner)

		stream := io.MultiReader(strings.NewReader(sshBanner), reader, conn)
		return internal.NewBufferedConn(conn, stream), nil
	}

	// SKENARIO B: RESPONS PENGALIHAN MULTI-RESPONSE (301 / 302)
	if statusCode == 301 || statusCode == 302 {
		if debug.Enable {
			debug.Println("[TUNNEL TRICK] Mendeteksi status 301, menguras header pengalihan...")
		}

		if secondPart != "" {
			if debug.Enable {
				debug.Println("[TUNNEL TRICK] Mengirimkan sisa payload hasil [split]...")
			}
			if _, err := conn.Write([]byte(secondPart)); err != nil {
				return nil, err
			}
		}

		_ = conn.SetReadDeadline(time.Now().Add(5 * time.Second))
		if debug.Enable {
			debug.Println("[SWEEPING] Menyapu sisa bodi kotor untuk mencari gerbang biner SSH...")
		}

		var collectedBody strings.Builder

		for {
			peekLine, err := reader.ReadString('\n')
			if err != nil {
				_ = conn.SetReadDeadline(time.Time{})

				fullBody := strings.ToLower(collectedBody.String())

				if strings.Contains(fullBody, "expired") ||
					strings.Contains(fullBody, "reject") ||
					strings.Contains(fullBody, "auth") {

					debug.EvaluateRejectResponse(statusCode, status, headers, conn, reader)
					return conn, ErrAuthFailed
				}

				if err == io.EOF {
					if debug.Enable {
						debug.Println("[TUNNEL TRICK] Koneksi diputus Cloudflare (EOF). Mencoba meneruskan reader...")
					}

					if reader.Buffered() > 0 {
						stream := io.MultiReader(reader, conn)
						return internal.NewBufferedConn(conn, stream), nil
					}

					return nil, err
				}

				return nil, err
			}

			collectedBody.WriteString(peekLine)

			if strings.HasPrefix(peekLine, "SSH-") {
				_ = conn.SetReadDeadline(time.Time{})

				debug.SSHBanner(peekLine)

				stream := io.MultiReader(strings.NewReader(peekLine), reader, conn)
				return internal.NewBufferedConn(conn, stream), nil
			}
		}
	}

	// SKENARIO C: RESPONS ERROR PENOLAKAN DARI SERVER (400, 403, 503, dll)
	if statusCode >= 400 && statusCode < 600 {
		debug.EvaluateRejectResponse(statusCode, status, headers, conn, reader)
		return conn, ErrAuthFailed
	}

	if reader.Buffered() > 0 {
		stream := io.MultiReader(reader, conn)
		return internal.NewBufferedConn(conn, stream), nil
	}
	return conn, nil
}