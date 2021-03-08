package logger

type Logger interface {
	Debug(v ...interface{})
	Info(v ...interface{})
	Warn(v ...interface{})
	Error(v ...interface{})
	Fatal(v ...interface{})
}

type DefaultLogger struct{}

func (l *DefaultLogger) Debug(v ...interface{}) {}
func (l *DefaultLogger) Info(v ...interface{})  {}
func (l *DefaultLogger) Warn(v ...interface{})  {}
func (l *DefaultLogger) Error(v ...interface{}) {}
func (l *DefaultLogger) Fatal(v ...interface{}) {}
