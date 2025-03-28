package zaplogger

import (
	"sync"

	"github.com/chmike/migrate"

	"go.uber.org/zap"
)

type zapAdapter struct {
	logger *zap.Logger
	level  migrate.LogLevel
	mu     sync.RWMutex
}

// New returns a zap logger using a new default logger.
func New(lvl migrate.LogLevel) migrate.Logger {
	defaultLogger, _ := zap.NewProduction()
	return &zapAdapter{
		logger: defaultLogger,
		level:  lvl,
	}
}

// NewWith returns a zap logger using the given logger and level.
func NewWith(logger *zap.Logger, lvl migrate.LogLevel) migrate.Logger {
	if logger == nil {
		logger, _ = zap.NewProduction()
	}
	return &zapAdapter{
		logger: logger,
		level:  lvl,
	}
}

// Level returns the current logging level.
func (a *zapAdapter) Level() migrate.LogLevel {
	a.mu.RLock()
	currentLevel := a.level
	a.mu.RUnlock()
	return currentLevel
}

// SetLevel set the logging level.
func (a *zapAdapter) SetLevel(lvl migrate.LogLevel) {
	a.mu.Lock()
	a.level = lvl
	a.mu.Unlock()
}

// Error logs an error level message.
func (a *zapAdapter) Error(msg string, fields ...migrate.Field) {
	a.mu.RLock()
	currentLevel := a.level
	a.mu.RUnlock()
	if currentLevel > migrate.LevelError {
		return
	}
	zapFields := make([]zap.Field, 0, len(fields))
	for _, f := range fields {
		zapFields = append(zapFields, zap.Any(f.Key, f.Value))
	}
	a.logger.Error(msg, zapFields...)
}

// Warn logs a warning level message.
func (a *zapAdapter) Warn(msg string, fields ...migrate.Field) {
	a.mu.RLock()
	currentLevel := a.level
	a.mu.RUnlock()
	if currentLevel > migrate.LevelWarn {
		return
	}
	zapFields := make([]zap.Field, 0, len(fields))
	for _, f := range fields {
		zapFields = append(zapFields, zap.Any(f.Key, f.Value))
	}
	a.logger.Warn(msg, zapFields...)
}

// Info logs an info level message.
func (a *zapAdapter) Info(msg string, fields ...migrate.Field) {
	a.mu.RLock()
	currentLevel := a.level
	a.mu.RUnlock()
	if currentLevel > migrate.LevelInfo {
		return
	}
	zapFields := make([]zap.Field, 0, len(fields))
	for _, f := range fields {
		zapFields = append(zapFields, zap.Any(f.Key, f.Value))
	}
	a.logger.Info(msg, zapFields...)
}

// Debug logs an debug level message.
func (a *zapAdapter) Debug(msg string, fields ...migrate.Field) {
	a.mu.RLock()
	currentLevel := a.level
	a.mu.RUnlock()
	if currentLevel > migrate.LevelDebug {
		return
	}
	zapFields := make([]zap.Field, 0, len(fields))
	for _, f := range fields {
		zapFields = append(zapFields, zap.Any(f.Key, f.Value))
	}
	a.logger.Debug(msg, zapFields...)
}
