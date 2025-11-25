package util

import (
	"fmt"
	"io"
	"log"
	"os"
	"strings"
)

// LogLevel represents the logging level
type LogLevel int

const (
	LogLevelOff LogLevel = iota
	LogLevelFatal
	LogLevelError
	LogLevelWarn
	LogLevelInfo
	LogLevelDebug
)

// String returns the string representation of the log level
func (l LogLevel) String() string {
	switch l {
	case LogLevelOff:
		return "OFF"
	case LogLevelFatal:
		return "FATAL"
	case LogLevelError:
		return "ERROR"
	case LogLevelWarn:
		return "WARN"
	case LogLevelInfo:
		return "INFO"
	case LogLevelDebug:
		return "DEBUG"
	default:
		return "UNKNOWN"
	}
}

// ParseLogLevel parses a string into a LogLevel
func ParseLogLevel(level string) LogLevel {
	level = strings.ToUpper(strings.TrimSpace(level))
	switch level {
	case "OFF":
		return LogLevelOff
	case "FATAL":
		return LogLevelFatal
	case "ERROR":
		return LogLevelError
	case "WARN", "WARNING":
		return LogLevelWarn
	case "INFO":
		return LogLevelInfo
	case "DEBUG":
		return LogLevelDebug
	default:
		return LogLevelInfo // Default to INFO if unknown
	}
}

// Logger wraps the standard log.Logger with level support
type Logger struct {
	service string
	level   LogLevel
	logger  *log.Logger
}

// New creates a new logger with the specified service name and log level
func New(service string, level LogLevel) *Logger {
	var writer io.Writer = os.Stdout
	if level == LogLevelOff {
		writer = io.Discard
	}

	return &Logger{
		service: service,
		level:   level,
		logger:  log.New(writer, fmt.Sprintf("[%s] ", service), log.LstdFlags|log.Lshortfile),
	}
}

// SetLevel sets the logging level
func (l *Logger) SetLevel(level LogLevel) {
	l.level = level
	if level == LogLevelOff {
		l.logger.SetOutput(io.Discard)
	} else {
		l.logger.SetOutput(os.Stdout)
	}
}

// shouldLog returns true if the given level should be logged
func (l *Logger) shouldLog(level LogLevel) bool {
	if l.level == LogLevelOff {
		return false
	}
	return level <= l.level
}

// Debug logs a debug message
func (l *Logger) Debug(format string, v ...interface{}) {
	if l.shouldLog(LogLevelDebug) {
		l.logger.Printf("[DEBUG] "+format, v...)
	}
}

// Info logs an info message
func (l *Logger) Info(format string, v ...interface{}) {
	if l.shouldLog(LogLevelInfo) {
		l.logger.Printf("[INFO] "+format, v...)
	}
}

// Warn logs a warning message
func (l *Logger) Warn(format string, v ...interface{}) {
	if l.shouldLog(LogLevelWarn) {
		l.logger.Printf("[WARN] "+format, v...)
	}
}

// Error logs an error message
func (l *Logger) Error(format string, v ...interface{}) {
	if l.shouldLog(LogLevelError) {
		l.logger.Printf("[ERROR] "+format, v...)
	}
}

// Fatal logs a fatal message and exits
func (l *Logger) Fatal(format string, v ...interface{}) {
	if l.shouldLog(LogLevelFatal) {
		l.logger.Fatalf("[FATAL] "+format, v...)
	}
}

// Printf logs a message at INFO level (for backward compatibility)
func (l *Logger) Printf(format string, v ...interface{}) {
	l.Info(format, v...)
}

// Println logs a message at INFO level (for backward compatibility)
func (l *Logger) Println(v ...interface{}) {
	if l.shouldLog(LogLevelInfo) {
		l.logger.Println(append([]interface{}{"[INFO]"}, v...)...)
	}
}

// Print logs a message at INFO level (for backward compatibility)
func (l *Logger) Print(v ...interface{}) {
	if l.shouldLog(LogLevelInfo) {
		l.logger.Print(append([]interface{}{"[INFO]"}, v...)...)
	}
}

// Fatalf logs a fatal message and exits (for backward compatibility)
func (l *Logger) Fatalf(format string, v ...interface{}) {
	l.Fatal(format, v...)
}
