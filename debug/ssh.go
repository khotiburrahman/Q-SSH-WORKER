package debug

import (
	"fmt"
	"io"
	"net"
	"strconv"
	"strings"
	"time"
)

// SSHBanner mencetak banner versi biner dari server SSH target
func SSHBanner(banner string) {
	if !Enable {
		return
	}

	Separator()
	Println("========== SSH BANNER ==========")
	Println(strings.TrimRight(banner, "\r\n"))
	Println("================================")
}

// SSHServerMessage mencetak pesan selamat datang / MOTD tambahan dari VPS jika ada
func SSHServerMessage(msg string) {
	if !Enable {
		return
	}

	Separator()
	Println("========== SSH SERVER MESSAGE ==========")
	Printf("%s", msg) 
	Println("========================================")
}

// EvaluateRejectResponse memeriksa HTTP error dari server, menguras body, dan mendeteksi status expired.
func EvaluateRejectResponse(statusCode int, status string, headers map[string]string, conn net.Conn, reader io.Reader) {
	if !Enable {
		return
	}

	Println(fmt.Sprintf("[PROXY REJECT] Server menolak koneksi dengan status HTTP %d", statusCode))

	// Set deadline singkat agar proses debug tidak membuat biner hang/membeku jika server macet
	_ = conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	defer conn.SetReadDeadline(time.Time{}) 

	var bodyBuilder strings.Builder

	// Ekstrasi body berdasarkan headers
	if contentLengthStr, ok := headers["Content-Length"]; ok {
		if cLen, err := strconv.Atoi(contentLengthStr); err == nil && cLen > 0 {
			bodyBytes := make([]byte, cLen)
			_, _ = io.ReadFull(reader, bodyBytes)
			bodyBuilder.Write(bodyBytes)
		}
	} else if headers["Transfer-Encoding"] == "chunked" {
		buf := make([]byte, 1024)
		if n, err := reader.Read(buf); err == nil && n > 0 {
			bodyBuilder.Write(buf[:n])
		}
	} else {
		// Fallback pembacaan stream langsung
		buf := make([]byte, 1024)
		if n, err := reader.Read(buf); err == nil && n > 0 {
			bodyBuilder.Write(buf[:n])
		}
	}

	serverMessage := bodyBuilder.String()
	if serverMessage != "" {
		SSHServerMessage(serverMessage)
		
		// Deteksi cerdas kata kunci expired untuk memberikan alert tambahan ke user OpenWrt
		lowerMsg := strings.ToLower(serverMessage)
		if strings.Contains(lowerMsg, "expired") || strings.Contains(lowerMsg, "auth fail") || strings.Contains(lowerMsg, "invalid") {
			Println("🛑 [ALERT] Analisis Debug: Kredensial ditolak. Akun kemungkinan besar EXPIRED!")
		}
	} else {
		// Balasan jika server pelit data
		fallbackMsg := fmt.Sprintf("Server mengembalikan HTTP %d %s tanpa rincian teks.\nKemungkinan besar akun EXPIRED atau Payload/Bug ditolak oleh Firewall VPS.\n", statusCode, status)
		SSHServerMessage(fallbackMsg)
	}
}