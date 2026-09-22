package logger

import (
	"fmt"
	"time"
)

var (
	Enable      = true
	DebugEnable = false
)

func ts() string {
	return time.Now().Format("2006-01-02 15:04:05")
}

func emit(msg string) {
	if !Enable {
		return
	}
	fmt.Printf("[%s] %s\n", ts(), msg)
}

// Debug hanya tampil jika Enable dan DebugEnable bernilai true.
func Debug(format string, a ...any) {
	if !Enable || !DebugEnable {
		return
	}
	fmt.Printf("[%s] [DEBUG] "+format+"\n", append([]any{ts()}, a...)...)
}

// Info selalu tampil jika Enable true.
func Info(format string, a ...any) {
	if !Enable {
		return
	}
	fmt.Printf("[%s] "+format+"\n", append([]any{ts()}, a...)...)
}

// Warn selalu tampil jika Enable true.
func Warn(format string, a ...any) {
	if !Enable {
		return
	}
	fmt.Printf("[%s] [WARN] "+format+"\n", append([]any{ts()}, a...)...)
}

// Errorf selalu tampil jika Enable true.
func Errorf(format string, a ...any) {
	if !Enable {
		return
	}
	fmt.Printf("[%s] [ERROR] "+format+"\n", append([]any{ts()}, a...)...)
}