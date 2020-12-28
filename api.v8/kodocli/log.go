package kodocli

import (
	"fmt"
	"log"
	"os"
)

type Ilog interface {
	Debug(v ...interface{})
	Info(v ...interface{})
	Warn(v ...interface{})
	Error(v ...interface{})
	Fatal(v ...interface{})
	//Println(v ...interface{})
}

type level int

const (
	LOG_LEVEL_DEBUG = iota
	LOG_LEVEL_INFO
	LOG_LEVEL_WARN
	LOG_LEVEL_ERROR
	LOG_LEVEL_FATAL
)

type logger struct {
	level level
}

func (l *logger) Debug(v ...interface{}) {
	if l.level > LOG_LEVEL_DEBUG {
		return
	}
	log.Output(2, fmt.Sprintln(v...))
}

func (l *logger) Info(v ...interface{}) {
	if l.level > LOG_LEVEL_INFO {
		return
	}
	log.Output(2, fmt.Sprintln(v...))
}

func (l *logger) Warn(v ...interface{}) {
	if l.level > LOG_LEVEL_WARN {
		return
	}
	log.Output(2, fmt.Sprintln(v...))
}

func (l *logger) Error(v ...interface{}) {
	if l.level > LOG_LEVEL_ERROR {
		return
	}
	log.Output(2, fmt.Sprintln(v...))
}

func (l *logger) Fatal(v ...interface{}) {
	if l.level > LOG_LEVEL_FATAL {
		return
	}
	log.Output(2, fmt.Sprintln(v...))
	os.Exit(1)
}

//func (l *logger) Println(v ...interface{}) {
//	log.Output(2, fmt.Sprintln(v...))
//}

func (l *logger) SetLevel(level level) {
	l.level = level
}

func NewLogger() *logger {
	var l = &logger{}
	l.level = LOG_LEVEL_INFO
	return l
}

// elog is embedded logger
var elog Ilog

func SetLogger(logger Ilog) {
	elog = logger
}

func init() {
	if elog == nil {
		elog = NewLogger()
	}
}
