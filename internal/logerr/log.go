package logerr

import (
	"log"
	"runtime/debug"
)

// Errorf 把错误原因打到标准错误，并附上当前 goroutine 的调用堆栈。
// Errorf writes an error to stderr and appends the current goroutine stack.
func Errorf(format string, args ...any) {
	log.Printf(format, args...)
	// Agent: 堆栈只含调用位置。调用方不得把 wandb key、密码或证书正文放进 format。
	log.Printf("stack:\n%s", debug.Stack())
}
