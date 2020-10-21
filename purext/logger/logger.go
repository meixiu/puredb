package logger

import (
	"fmt"
	"log"
	"os"
	"path/filepath"
	"runtime"
	"time"
)

type (
	logger struct {
		rawLog *log.Logger
		level  int
		prefix string
	}
)

var (
	callerDepth   = 3
	levelFlags    = []string{"DEBUG", "INFO", "WARN", "ERROR", "PANIC", "FATAL"}
	defaultLogger = New()
)

const (
	DEBUG int = iota
	INFO
	WARN
	ERROR
	PANIC
	FATAL
	OFF
)

// 初始化日志
func New() *logger {
	rawLog := log.New(os.Stderr, "", log.LstdFlags)
	rawLog.SetFlags(0)
	return &logger{
		rawLog: rawLog,
	}
}

func (l *logger) SetLevel(level int) {
	l.level = level
}

// 打印调试信息
func (l *logger) Debug(v ...interface{}) {
	if l.level > DEBUG {
		return
	}
	l.setPrefix(DEBUG)
	l.rawLog.Println(v...)
}

// 打印调试信息f
func (l *logger) Debugf(format string, v ...interface{}) {
	if l.level > DEBUG {
		return
	}
	l.setPrefix(DEBUG)
	l.rawLog.Printf(format, v...)
}

// 打印警告信息
func (l *logger) Warn(v ...interface{}) {
	if l.level > WARN {
		return
	}
	l.setPrefix(WARN)
	l.rawLog.Println(v...)
}

// 打印警告信息f
func (l *logger) Warnf(format string, v ...interface{}) {
	if l.level > WARN {
		return
	}
	l.setPrefix(WARN)
	l.rawLog.Printf(format, v...)
}

// 打印错误信息
func (l *logger) Error(v ...interface{}) {
	if l.level > ERROR {
		return
	}
	l.setPrefix(ERROR)
	l.rawLog.Println(v...)
}

// 打印错误信息f
func (l *logger) Errorf(format string, v ...interface{}) {
	if l.level > ERROR {
		return
	}
	l.setPrefix(ERROR)
	l.rawLog.Printf(format, v...)
}

// 打印异常
func (l *logger) Panic(v ...interface{}) {
	if l.level > PANIC {
		return
	}
	l.setPrefix(PANIC)
	l.rawLog.Panic(v...)
}

// 打印异常f
func (l *logger) Panicf(format string, v ...interface{}) {
	if l.level > PANIC {
		return
	}
	l.setPrefix(PANIC)
	l.rawLog.Panicf(format, v...)
}

// 打印致命错误
func (l *logger) Fatal(v ...interface{}) {
	if l.level > FATAL {
		return
	}
	l.setPrefix(FATAL)
	l.rawLog.Fatal(v...)
}

// 打印致命错误f
func (l *logger) Fatalf(format string, v ...interface{}) {
	if l.level > FATAL {
		return
	}
	l.setPrefix(FATAL)
	l.rawLog.Fatalf(format, v...)
}

// 打印信息
func (l *logger) Info(v ...interface{}) {
	if l.level > INFO {
		return
	}
	l.setPrefix(INFO)
	l.rawLog.Println(v...)
}

// 打印信息
func (l *logger) Infof(format string, v ...interface{}) {
	if l.level > INFO {
		return
	}
	l.setPrefix(INFO)
	l.rawLog.Printf(format, v...)
}

func (l *logger) setPrefix(level int) {
	t := time.Now()
	flag := levelFlags[level]
	_, file, line, ok := runtime.Caller(callerDepth)
	if ok {
		file = filepath.Base(file)
	}
	logPrefix := fmt.Sprintf("[%s] %v | %s:%d | ", flag, t.Format("2006/01/02 15:04:05"), file, line)
	l.rawLog.SetPrefix(logPrefix)
}

// 设置日志等级
func SetLevel(level int) {
	defaultLogger.SetLevel(level)
}

// 打印调试信息
func Debug(v ...interface{}) {
	defaultLogger.Debug(v...)
}

// 打印调试信息f
func Debugf(format string, v ...interface{}) {
	defaultLogger.Debugf(format, v...)
}

// 打印警告信息
func Warn(v ...interface{}) {
	defaultLogger.Warn(v...)
}

// 打印警告信息f
func Warnf(format string, v ...interface{}) {
	defaultLogger.Warnf(format, v...)
}

// 打印错误信息
func Error(v ...interface{}) {
	defaultLogger.Error(v...)
}

// 打印错误信息f
func Errorf(format string, v ...interface{}) {
	defaultLogger.Errorf(format, v...)
}

// 打印异常
func Panic(v ...interface{}) {
	defaultLogger.Panic(v...)
}

// 打印异常f
func Panicf(format string, v ...interface{}) {
	defaultLogger.Panicf(format, v...)
}

// 打印致命错误
func Fatal(v ...interface{}) {
	defaultLogger.Fatal(v...)
}

// 打印致命错误f
func Fatalf(format string, v ...interface{}) {
	defaultLogger.Fatalf(format, v...)
}

// 打印信息
func Info(v ...interface{}) {
	defaultLogger.Info(v...)
}

// 打印信息
func Infof(format string, v ...interface{}) {
	defaultLogger.Infof(format, v...)
}
