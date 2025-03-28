package zerologger

import (
	"sync"

	"github.com/chmike/migrate"

	"github.com/rs/zerolog"
)

type zerologAdapter struct {
	logger zerolog.Logger
	level  migrate.LogLevel
	mu     sync.RWMutex
}

// New returns a zerolog adapter.
func New(lvl migrate.LogLevel) migrate.Logger {
	return &zerologAdapter{level: lvl}
}

// NewWith returns a zerolog adapter with the given logger.
func NewWith(logger zerolog.Logger, lvl migrate.LogLevel) migrate.Logger {
	return &zerologAdapter{logger: logger, level: lvl}
}

// Level returns the current logging level.
func (a *zerologAdapter) Level() migrate.LogLevel {
	a.mu.RLock()
	currentLevel := a.level
	a.mu.RUnlock()
	return currentLevel
}

// SetLevel set the logging level.
func (a *zerologAdapter) SetLevel(lvl migrate.LogLevel) {
	a.mu.Lock()
	a.level = lvl
	a.mu.Unlock()
}

// Error logs an error level message.
func (a *zerologAdapter) Error(msg string, fields ...migrate.Field) {
	a.mu.RLock()
	currentLevel := a.level
	a.mu.RUnlock()
	if currentLevel <= migrate.LevelError {
		a.log(a.logger.Error(), msg, fields...)
	}
}

// Warn logs a warning level message.
func (a *zerologAdapter) Warn(msg string, fields ...migrate.Field) {
	a.mu.RLock()
	currentLevel := a.level
	a.mu.RUnlock()
	if currentLevel <= migrate.LevelWarn {
		a.log(a.logger.Warn(), msg, fields...)
	}
}

// Info logs an info level message.
func (a *zerologAdapter) Info(msg string, fields ...migrate.Field) {
	a.mu.RLock()
	currentLevel := a.level
	a.mu.RUnlock()
	if currentLevel <= migrate.LevelInfo {
		a.log(a.logger.Info(), msg, fields...)
	}
}

// Debug logs an debug level message.
func (a *zerologAdapter) Debug(msg string, fields ...migrate.Field) {
	a.mu.RLock()
	currentLevel := a.level
	a.mu.RUnlock()
	if currentLevel <= migrate.LevelDebug {
		a.log(a.logger.Debug(), msg, fields...)
	}
}

// Log logs the message and associated fields.
func (a *zerologAdapter) log(event *zerolog.Event, msg string, fields ...migrate.Field) {
	for _, f := range fields {
		switch v := f.Value.(type) {
		case string:
			event = event.Str(f.Key, v)
		case int:
			event = event.Int(f.Key, v)
		case float64:
			event = event.Float64(f.Key, v)
		case bool:
			event = event.Bool(f.Key, v)
		default:
			event = event.Interface(f.Key, v)
		}
	}
	event.Msg(msg)
}
