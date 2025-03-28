package migrate

import (
	"context"
	"fmt"
	"log"
	"log/slog"
	"strings"
	"sync"
)

// Different log levels supported
type LogLevel int

const (
	LevelDebug LogLevel = iota
	LevelInfo
	LevelWarn
	LevelError
	LevelNoLog
)

// Field is a key value pairs.
type Field struct {
	Key   string
	Value any
}

// F is a helper function to create a key value pair field to log.
func F(key string, value any) Field {
	return Field{Key: key, Value: value}
}

// -- nil adapter --

// NilAdapter is a nil logger that doesn't produce any log.
type NilAdapter struct {
	level LogLevel
}

func NewNilLogger() Logger {
	return &NilAdapter{level: LevelInfo}
}

// Level returns the current logging level.
func (a *NilAdapter) Level() LogLevel {
	return a.level
}

// SetLevel set the logging level.
func (a *NilAdapter) SetLevel(lvl LogLevel) {
	a.level = lvl
}

// Error logs an error level message.
func (a *NilAdapter) Error(msg string, fields ...Field) {
}

// Warn logs a warning level message.
func (a *NilAdapter) Warn(msg string, fields ...Field) {
}

// Info logs an info level message.
func (a *NilAdapter) Info(msg string, fields ...Field) {
}

// Debug logs an debug level message.
func (a *NilAdapter) Debug(msg string, fields ...Field) {
}

// -- slog adapter --

// SlogAdapter adapts slog.Logger to Logger
type SlogAdapter struct {
	logger *slog.Logger
	level  LogLevel
	mu     sync.RWMutex
}

// NewSlogLogger returns a Logger using the default slog logger.
func NewSlogLogger(lvl LogLevel) Logger {
	return &SlogAdapter{
		logger: slog.Default(),
		level:  lvl,
	}
}

// NewSlogLoggerWith returns a Logger using the given slog logger.
func NewSlogLoggerWith(logger *slog.Logger, lvl LogLevel) Logger {
	if logger == nil {
		logger = slog.Default()
	}
	return &SlogAdapter{
		logger: logger,
		level:  lvl,
	}
}

// Level returns the current logging level.
func (a *SlogAdapter) Level() LogLevel {
	a.mu.RLock()
	currentLevel := a.level
	a.mu.RUnlock()
	return currentLevel
}

// SetLevel set the logging level.
func (a *SlogAdapter) SetLevel(lvl LogLevel) {
	a.mu.Lock()
	a.level = lvl
	a.mu.Unlock()
}

// Error logs an error level message.
func (a *SlogAdapter) Error(msg string, fields ...Field) {
	a.mu.RLock()
	currentLevel := a.level
	a.mu.RUnlock()
	if currentLevel > LevelError {
		return
	}
	a.log(slog.LevelError, msg, fields...)
}

// Warn logs a warning level message.
func (a *SlogAdapter) Warn(msg string, fields ...Field) {
	a.mu.RLock()
	currentLevel := a.level
	a.mu.RUnlock()
	if currentLevel > LevelWarn {
		return
	}
	a.log(slog.LevelWarn, msg, fields...)
}

// Info logs an info level message.
func (a *SlogAdapter) Info(msg string, fields ...Field) {
	a.mu.RLock()
	currentLevel := a.level
	a.mu.RUnlock()
	if currentLevel > LevelInfo {
		return
	}
	a.log(slog.LevelInfo, msg, fields...)
}

// Debug logs an debug level message.
func (a *SlogAdapter) Debug(msg string, fields ...Field) {
	a.mu.RLock()
	currentLevel := a.level
	a.mu.RUnlock()
	if currentLevel > LevelDebug {
		return
	}
	a.log(slog.LevelDebug, msg, fields...)
}

func (a *SlogAdapter) log(level slog.Level, msg string, fields ...Field) {
	attrs := make([]slog.Attr, 0, len(fields))
	for _, f := range fields {
		attrs = append(attrs, slog.Any(f.Key, f.Value))
	}
	a.logger.LogAttrs(context.Background(), level, msg, attrs...)
}

// -- (std) log adapter --

// LogAdapter adapts the std log to the logger.
type LogAdapter struct {
	logger *log.Logger
	level  LogLevel
	mu     sync.RWMutex
}

// NewLogLogger creates a new std logger using the default logger.
func NewLogLogger(lvl LogLevel) Logger {
	return &LogAdapter{
		logger: log.Default(),
		level:  lvl,
	}
}

// NewLogLoggerWith creates a new std logger using the given logger.
func NewLogLoggerWith(logger *log.Logger, lvl LogLevel) Logger {
	if logger == nil {
		logger = log.Default()
	}
	return &LogAdapter{
		logger: logger,
		level:  lvl,
	}
}

// Level returns the current logging level.
func (a *LogAdapter) Level() LogLevel {
	a.mu.RLock()
	currentLevel := a.level
	a.mu.RUnlock()
	return currentLevel
}

// SetLevel set the logging level.
func (a *LogAdapter) SetLevel(lvl LogLevel) {
	a.mu.Lock()
	a.level = lvl
	a.mu.Unlock()
}

// Error logs an error level message.
func (a *LogAdapter) Error(msg string, fields ...Field) {
	a.mu.RLock()
	currentLevel := a.level
	a.mu.RUnlock()
	if currentLevel > LevelError {
		return
	}
	a.log("ERROR", msg, fields...)
}

// Warn logs a warning level message.
func (a *LogAdapter) Warn(msg string, fields ...Field) {
	a.mu.RLock()
	currentLevel := a.level
	a.mu.RUnlock()
	if currentLevel > LevelWarn {
		return
	}
	a.log("WARN", msg, fields...)
}

// Info logs an info level message.
func (a *LogAdapter) Info(msg string, fields ...Field) {
	a.mu.RLock()
	currentLevel := a.level
	a.mu.RUnlock()
	if currentLevel > LevelInfo {
		return
	}
	a.log("INFO", msg, fields...)
}

// Debug logs an debug level message.
func (a *LogAdapter) Debug(msg string, fields ...Field) {
	a.mu.RLock()
	currentLevel := a.level
	a.mu.RUnlock()
	if currentLevel > LevelDebug {
		return
	}
	a.log("DEBUG", msg, fields...)
}

// // Log logs the message and associated fields.
func (a *LogAdapter) log(levelStr string, msg string, fields ...Field) {
	var buf strings.Builder
	buf.WriteString(fmt.Sprintf("[%s] %s", levelStr, msg))
	if len(fields) > 0 {
		buf.WriteString(" |")
		for _, field := range fields {
			buf.WriteString(fmt.Sprintf(" %s='%v'", field.Key, field.Value))
		}
	}
	a.logger.Println(buf.String())
}
