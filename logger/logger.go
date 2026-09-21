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

func Debug(format string, a ...any) {
	if !Enable || !DebugEnable {
		return
	}
	fmt.Printf("[DEBUG] "+format+"\n", a...)
}

func Info(format string, a ...any) {
	if !Enable {
		return
	}
	fmt.Printf(format+"\n", a...)
}

func Warn(format string, a ...any) {
	if !Enable {
		return
	}
	fmt.Printf("[WARN] "+format+"\n", a...)
}

func Errorf(format string, a ...any) {
	if !Enable {
		return
	}
	fmt.Printf("[ERROR] "+format+"\n", a...)
}