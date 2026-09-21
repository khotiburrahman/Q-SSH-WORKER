package logger

import (
	"os"
	"sync"

	"golang.org/x/sys/unix"
)

var (
	redirectMu   sync.Mutex
	redirectPath string
)

// RedirectStdio mengarahkan FD 1 (stdout) dan FD 2 (stderr) ke file.
//
// Setelah fungsi ini sukses, semua fmt.Print*, logger.*, dan debug.*
// otomatis masuk ke file, tanpa perlu ubah kode mereka.
//
// File dibuka dengan O_APPEND, jadi aman kalau multiproses menulis
// ke file yang sama (misal Master + beberapa Child Worker).
//
// Menggunakan golang.org/x/sys/unix.Dup2 yang kompatibel dengan
// linux/amd64, linux/arm64, linux/armv7, dan android/arm64.
func RedirectStdio(path string) error {
	redirectMu.Lock()
	defer redirectMu.Unlock()

	if path == "" {
		return nil
	}

	f, err := os.OpenFile(
		path,
		os.O_CREATE|os.O_WRONLY|os.O_APPEND,
		0644,
	)
	if err != nil {
		return err
	}
	defer f.Close()

	fd := int(f.Fd())

	if err := unix.Dup2(fd, 1); err != nil {
		return err
	}

	if err := unix.Dup2(fd, 2); err != nil {
		return err
	}

	redirectPath = path

	return nil
}

// Reopen membuka ulang file log yang sama.
//
// Dipakai untuk logrotate: setelah file di-rename, kirim SIGUSR1
// ke proses, lalu Reopen akan mengganti FD 1 dan 2 ke file baru
// dengan path yang sama.
func Reopen() error {
	redirectMu.Lock()
	path := redirectPath
	redirectMu.Unlock()

	if path == "" {
		return nil
	}

	return RedirectStdio(path)
}

// LogPath mengembalikan path log yang sedang aktif.
// Mengembalikan string kosong jika RedirectStdio belum dipanggil.
func LogPath() string {
	redirectMu.Lock()
	defer redirectMu.Unlock()

	return redirectPath
}