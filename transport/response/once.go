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

func Once(cfg *config.Config, conn net.Conn, secondPart string) (net.Conn, error) {
	reader := bufio.NewReader(conn)

	// 1. Baca baris status pertama (HTTP Status Line)
	line, err := reader.ReadString('\n')
	if err != nil {
		return nil, err
	}
	line = strings.TrimRight(line, "\r\n")

	// Ekstrak komponen Status Line (Contoh: "HTTP/1.1 301 Moved Permanently")
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

	// 2. Baca dan kumpulkan seluruh HTTP Headers ke dalam map
	headers := make(map[string]string)
	var headerLines []string
	for {
		hl, err := reader.ReadString('\n')
		if err != nil {
			return nil, err
		}
		trimmedHl := strings.TrimRight(hl, "\r\n")
		if trimmedHl == "" {
			break // Batas akhir header didapatkan (\r\n\r\n)
		}
		headerLines = append(headerLines, hl) // Simpan untuk dikuras nanti jika diperlukan

		// Masukkan ke map key-value
		if idx := strings.Index(trimmedHl, ":"); idx != -1 {
			k := strings.TrimSpace(trimmedHl[:idx])
			v := strings.TrimSpace(trimmedHl[idx+1:])
			headers[k] = v
		}
	}

	// ======================================================================
	// 🟢 PANGGIL SUB-SYSTEM DEBUG RESPONSE ASLI MILIKMU
	// ======================================================================
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
				
				// 1. Cek mutlak jika ada indikasi akun mati/expired di dalam bodi respon
				if strings.Contains(fullBody, "expired") || strings.Contains(fullBody, "reject") || strings.Contains(fullBody, "auth") {
					debug.EvaluateRejectResponse(statusCode, status, headers, conn, reader)
					return conn, errors.New("AUTH_FAILED")
				}

				// 2. Jika akun aktif tapi tetap EOF karena Cloudflare nge-drop koneksi (seperti log kamu)
				if err == io.EOF {
					// Jika di dalam bodi chunked Cloudflare tidak ada info expired, artinya bug host/payload-mu yang mental
					if debug.Enable {
						debug.Println("[TUNNEL TRICK] Koneksi diputus Cloudflare (EOF). Mencoba meneruskan reader...")
					}
					
					// Paksa bungkus sisa buffer yang ada, jangan langsung matikan worker
					if reader.Buffered() > 0 {
						stream := io.MultiReader(reader, conn)
						return internal.NewBufferedConn(conn, stream), nil
					}
					
					// Jika buffer kosong total dan tidak ada indikasi expired, return eror murni agar master tahu ini masalah jaringan/payload
					return nil, err
				}
				
				return nil, err
			}

			collectedBody.WriteString(peekLine)

			// Jika banner SSH ditemukan di sela-sela pembersihan bodi
			if strings.HasPrefix(peekLine, "SSH-") {
				_ = conn.SetReadDeadline(time.Time{})

				debug.SSHBanner(peekLine)

				stream := io.MultiReader(strings.NewReader(peekLine), reader, conn)
				return internal.NewBufferedConn(conn, stream), nil
			}
		}
	}

	// ======================================================================
	// 🛑 SKENARIO C: RESPONS ERROR PENOLAKAN DARI SERVER (400, 403, 503, dll)
	// ======================================================================
	if statusCode >= 400 && statusCode < 600 {

		// 🟢 Panggil fungsi terpusat di package debug. Bersih dan tidak berceceran!
		debug.EvaluateRejectResponse(statusCode, status, headers, conn, reader)

		// Kembalikan objek koneksi dan error otentikasi murni ke Master Manager
		return conn, errors.New("AUTH_FAILED")
	}

	if reader.Buffered() > 0 {
		stream := io.MultiReader(reader, conn)
		return internal.NewBufferedConn(conn, stream), nil
	}
	return conn, nil
}