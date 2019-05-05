package rabbeasy

// Logger specifies interface for loggers implementations
type Logger interface {
	Errorf(msg string, v ...interface{})
	Warnf(msg string, v ...interface{})
	Infof(msg string, v ...interface{})
	Debugf(msg string, v ...interface{})
	Tracef(msg string, v ...interface{})
}
