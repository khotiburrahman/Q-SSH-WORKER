package proxy

import (
    "fmt"
    "net"

    "github.com/QcomWrt/Q-SSH-WORKER/config"
    "github.com/QcomWrt/Q-SSH-WORKER/debug"
    "github.com/QcomWrt/Q-SSH-WORKER/network/dialer" // 🟢 Panggil paket resolve buatanmu
)

// resolveHost adalah fungsi pembantu internal untuk mendeteksi domain dan mengubahnya jadi IP murni
func resolveHost(host string) string {
    // Jika host kosong atau sudah berupa IP murni, langsung kembalikan
    if host == "" || net.ParseIP(host) != nil {
        return host
    }

    // Panggil dialer.Resolve yang mengembalikan ([]string, error)
    ips, err := dialer.Resolve(host)
    if err == nil && len(ips) > 0 {
        for _, ipStr := range ips {
            // Utamakan IPv4 agar pas dengan routing iptables OpenWrt
            if parsedIP := net.ParseIP(ipStr); parsedIP != nil && parsedIP.To4() != nil {
                if debug.Enable {
                    fmt.Printf("[PROXY RESOLVER] Domain '%s' -> IP '%s'\n", host, ipStr)
                }
                return ipStr
            }
        }
    }

    if debug.Enable && err != nil {
        fmt.Printf("[PROXY RESOLVER WARNING] Gagal resolve '%s': %v\n", host, err)
    }
    
    return host // Fallback ke host asli jika gagal resolve
}

func Connect(cfg *config.Config, conn net.Conn) (net.Conn, error) {
    // 🟢 Bersihkan host SSH dan Proxy langsung sebelum masuk ke proses network/debug
    // cfg.SSH.Host = resolveHost(cfg.SSH.Host)
    cfg.Proxy.Host = resolveHost(cfg.Proxy.Host)

    // Sekarang debug.Proxy akan mencetak IP murni yang sudah siap pakai
    debug.Proxy(
        cfg.Proxy.Host,
        cfg.Proxy.Port,
        cfg.SSH.Host,
        cfg.SSH.Port,
    )

    return conn, nil
}