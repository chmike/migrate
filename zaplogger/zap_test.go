package zaplogger

import (
	"bytes"
	"encoding/json"
	"testing"

	"github.com/chmike/migrate"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"go.uber.org/zap/zaptest/observer"
)

func TestNewZapLogger(t *testing.T) {
	// Test creating a new logger with default production config
	logger := New(migrate.LevelInfo)

	// Assert the logger is of the correct type
	zapLogger, ok := logger.(*zapAdapter)
	assert.True(t, ok, "Logger should be of type *zapAdapter")

	// Assert the log level is set correctly
	assert.Equal(t, migrate.LevelInfo, zapLogger.Level(), "Log level should be InfoLevel")

	// Assert the zap logger is not nil
	assert.NotNil(t, zapLogger.logger, "Zap logger should not be nil")
}

func TestNewZapLoggerWith(t *testing.T) {
	// Test 1: Providing a valid zap logger
	core, _ := observer.New(zapcore.InfoLevel)
	customLogger := zap.New(core)
	logger := NewWith(customLogger, migrate.LevelWarn)

	zapLogger, ok := logger.(*zapAdapter)
	assert.True(t, ok, "Logger should be of type *zapAdapter")
	assert.Equal(t, migrate.LevelWarn, zapLogger.Level(), "Log level should be WarnLevel")
	assert.Equal(t, customLogger, zapLogger.logger, "Zap logger should be the provided logger")

	// Test 2: Providing a nil zap logger (should create a default one)
	logger = NewWith(nil, migrate.LevelError)

	zapLogger, ok = logger.(*zapAdapter)
	assert.True(t, ok, "Logger should be of type *zapAdapter")
	assert.Equal(t, migrate.LevelError, zapLogger.Level(), "Log level should be ErrorLevel")
	assert.NotNil(t, zapLogger.logger, "Zap logger should not be nil despite providing nil")
}

func TestZapAdapter_Level(t *testing.T) {
	// Create test logger with specific level
	logger := New(migrate.LevelDebug)

	// Assert correct level is returned
	assert.Equal(t, migrate.LevelDebug, logger.Level(), "Level should return DebugLevel")

	// Change level and check again
	logger.SetLevel(migrate.LevelError)
	assert.Equal(t, migrate.LevelError, logger.Level(), "Level should return ErrorLevel after change")
}

func TestZapAdapter_SetLevel(t *testing.T) {
	// Create test logger
	logger := New(migrate.LevelInfo)

	// Set a new level
	logger.SetLevel(migrate.LevelWarn)

	// Verify the level has been updated
	assert.Equal(t, migrate.LevelWarn, logger.Level(), "Log level should be updated to WarnLevel")

	// Test concurrent access (this is a simple test, more sophisticated tests could be done)
	go func() {
		logger.SetLevel(migrate.LevelError)
	}()

	// Read the level in the main goroutine (just to exercise the mutex)
	_ = logger.Level()
}

func TestZapAdapter_Log(t *testing.T) {
	tests := []struct {
		name          string
		loggerLevel   migrate.LogLevel
		messageLevel  migrate.LogLevel
		message       string
		fields        []migrate.Field
		shouldLog     bool
		expectedLevel string
	}{
		{
			name:          "Debug message at debug level",
			loggerLevel:   migrate.LevelDebug,
			messageLevel:  migrate.LevelDebug,
			message:       "debug message",
			fields:        []migrate.Field{{Key: "key1", Value: "value1"}},
			shouldLog:     true,
			expectedLevel: "debug",
		},
		{
			name:          "Info message at debug level",
			loggerLevel:   migrate.LevelDebug,
			messageLevel:  migrate.LevelInfo,
			message:       "info message",
			fields:        []migrate.Field{{Key: "key2", Value: "value2"}},
			shouldLog:     true,
			expectedLevel: "info",
		},
		{
			name:          "Warning message at info level",
			loggerLevel:   migrate.LevelInfo,
			messageLevel:  migrate.LevelWarn,
			message:       "warning message",
			fields:        []migrate.Field{{Key: "key3", Value: 3}},
			shouldLog:     true,
			expectedLevel: "warn",
		},
		{
			name:          "Error message at warning level",
			loggerLevel:   migrate.LevelWarn,
			messageLevel:  migrate.LevelError,
			message:       "error message",
			fields:        []migrate.Field{{Key: "key4", Value: true}},
			shouldLog:     true,
			expectedLevel: "error",
		},
		{
			name:         "Debug message at info level (should not log)",
			loggerLevel:  migrate.LevelInfo,
			messageLevel: migrate.LevelDebug,
			message:      "debug message that should not appear",
			fields:       []migrate.Field{{Key: "key5", Value: 5.5}},
			shouldLog:    false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create an in-memory syncer for testing
			var buf bytes.Buffer

			// Create a custom encoder config that outputs JSON without timestamps/levels for easier testing
			encoderConfig := zapcore.EncoderConfig{
				MessageKey:     "msg",
				LevelKey:       "level",
				NameKey:        "logger",
				EncodeLevel:    zapcore.LowercaseLevelEncoder,
				EncodeTime:     zapcore.ISO8601TimeEncoder,
				EncodeDuration: zapcore.StringDurationEncoder,
			}

			// Create a memory core for the logger
			core := zapcore.NewCore(
				zapcore.NewJSONEncoder(encoderConfig),
				zapcore.AddSync(&buf),
				zapcore.DebugLevel, // Set core to accept all logs
			)

			// Create the zap logger with our test core
			testZapLogger := zap.New(core)

			// Create our adapter with the configured level
			adapter := NewWith(testZapLogger, tt.loggerLevel)

			switch tt.messageLevel {
			case migrate.LevelDebug:
				adapter.Debug(tt.message, tt.fields...)
			case migrate.LevelInfo:
				adapter.Info(tt.message, tt.fields...)
			case migrate.LevelWarn:
				adapter.Warn(tt.message, tt.fields...)
			case migrate.LevelError:
				adapter.Error(tt.message, tt.fields...)
			}

			// Check if a log was made
			if tt.shouldLog {
				// There should be output
				assert.NotEmpty(t, buf.String(), "Expected log output")

				// Decode the JSON to validate its content
				var logMap map[string]interface{}
				err := json.Unmarshal(buf.Bytes(), &logMap)
				require.NoError(t, err, "Should be able to unmarshal log JSON")

				// Check log level
				if level, ok := logMap["level"].(string); ok {
					assert.Equal(t, tt.expectedLevel, level, "Log level should match expected")
				}

				// Check message
				if msg, ok := logMap["msg"].(string); ok {
					assert.Equal(t, tt.message, msg, "Log message should match expected")
				}

				// Check fields
				for _, field := range tt.fields {
					value, exists := logMap[field.Key]
					assert.True(t, exists, "Field key should exist in log")

					// Convert expected value to the same type as the JSON unmarshal
					switch v := field.Value.(type) {
					case int:
						assert.Equal(t, float64(v), value, "Field value should match expected")
					case bool, string:
						assert.Equal(t, v, value, "Field value should match expected")
					case float64:
						assert.Equal(t, v, value, "Field value should match expected")
					default:
						// For complex types, just check that something exists
						assert.NotNil(t, value, "Field value should exist")
					}
				}
			} else {
				// There should be no output
				assert.Empty(t, buf.String(), "Expected no log output")
			}
		})
	}
}

// TestConcurrency makes sure that the mutex properly protects concurrent access
func TestConcurrency(t *testing.T) {
	adapter := New(migrate.LevelInfo).(*zapAdapter)

	// Start multiple goroutines modifying and reading the level
	done := make(chan bool)

	// Several goroutines reading the level
	for i := 0; i < 5; i++ {
		go func() {
			for j := 0; j < 100; j++ {
				_ = adapter.Level()
			}
			done <- true
		}()
	}

	// Several goroutines writing the level
	for i := 0; i < 5; i++ {
		go func() {
			for j := 0; j < 100; j++ {
				adapter.SetLevel(migrate.LogLevel(j % 4))
			}
			done <- true
		}()
	}

	// Wait for all goroutines to finish
	for i := 0; i < 10; i++ {
		<-done
	}

	// If we reached here without deadlock or race conditions, the test passes
}

// TestLogAllLevels ensures all log level switch cases are covered
func TestLogAllLevels(t *testing.T) {
	// Create an observer core to inspect the logs
	core, logs := observer.New(zapcore.DebugLevel)
	testZapLogger := zap.New(core)

	// Create our adapter with debug level to allow all logs
	adapter := NewWith(testZapLogger, migrate.LevelDebug)

	// Test all log levels
	testMsg := "test message"

	// Debug level
	adapter.Debug(testMsg)
	assert.Equal(t, 1, logs.Len(), "Should have 1 log entry")
	assert.Equal(t, zapcore.DebugLevel, logs.All()[0].Level, "Should log at debug level")

	// Info level
	adapter.Info(testMsg)
	assert.Equal(t, 2, logs.Len(), "Should have 2 log entries")
	assert.Equal(t, zapcore.InfoLevel, logs.All()[1].Level, "Should log at info level")

	// Warn level
	adapter.Warn(testMsg)
	assert.Equal(t, 3, logs.Len(), "Should have 3 log entries")
	assert.Equal(t, zapcore.WarnLevel, logs.All()[2].Level, "Should log at warn level")

	// Error level
	adapter.Error(testMsg)
	assert.Equal(t, 4, logs.Len(), "Should have 4 log entries")
	assert.Equal(t, zapcore.ErrorLevel, logs.All()[3].Level, "Should log at error level")

	adapter.SetLevel(migrate.LevelNoLog)
	adapter.Debug("no show message")
	adapter.Info("no show message")
	adapter.Warn("no show message")
	adapter.Error("no show message")
	assert.Equal(t, 4, logs.Len(), "Should have 4 log entries")
}
