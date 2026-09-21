package logger

import "fmt"

var (
	Enable      = true
	DebugEnable = false
)

func emit(msg string) {
	if !Enable {
		return
	}
	fmt.Println(msg)
}

// Debug hanya tampil jika Enable dan DebugEnable bernilai true.
// Dipakai di jalur panas (SOCKS handler, relay, dll).
func Debug(format string, a ...any) {
	if !Enable || !DebugEnable {
		return
	}
	fmt.Printf("[DEBUG] "+format+"\n", a...)
}

// Info selalu tampil jika Enable true.
func Info(format string, a ...any) {
	if !Enable {
		return
	}
	fmt.Printf(format+"\n", a...)
}

// Warn selalu tampil jika Enable true, diberi prefix [WARN].
func Warn(format string, a ...any) {
	if !Enable {
		return
	}
	fmt.Printf("[WARN] "+format+"\n", a...)
}

// Errorf selalu tampil jika Enable true, diberi prefix [ERROR].
// Tidak bertabrakan dengan logger.Error(err error).
func Errorf(format string, a ...any) {
	if !Enable {
		return
	}
	fmt.Printf("[ERROR] "+format+"\n", a...)
}