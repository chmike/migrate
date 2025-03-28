package zerologger

import (
	"bytes"
	"encoding/json"
	"io"
	"sync"
	"testing"

	"github.com/chmike/migrate"

	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewZerologLogger(t *testing.T) {
	// Test creating a new logger
	logger := New(migrate.LevelInfo)

	// Assert the logger is of the correct type
	zerologAdapter, ok := logger.(*zerologAdapter)
	assert.True(t, ok, "Logger should be of type *zerologAdapter")

	// Assert the log level is set correctly
	assert.Equal(t, migrate.LevelInfo, zerologAdapter.Level(), "Log level should be InfoLevel")
}

func TestNewZerologLoggerWith(t *testing.T) {
	// Create a test zerolog logger
	var buf bytes.Buffer
	testLogger := zerolog.New(&buf)

	// Test providing a custom zerolog logger
	logger := NewWith(testLogger, migrate.LevelWarn)

	zerologAdapter, ok := logger.(*zerologAdapter)
	assert.True(t, ok, "Logger should be of type *zerologAdapter")
	assert.Equal(t, migrate.LevelWarn, zerologAdapter.Level(), "Log level should be WarnLevel")

	// Test the logger is properly set
	zerologAdapter.Warn("test message")
	assert.Contains(t, buf.String(), "test message", "Log message should be written to the buffer")
}

func TestZerologAdapter_Level(t *testing.T) {
	// Create test logger with specific level
	logger := New(migrate.LevelDebug)

	// Assert correct level is returned
	assert.Equal(t, migrate.LevelDebug, logger.Level(), "Level should return DebugLevel")

	// Change level and check again
	logger.SetLevel(migrate.LevelError)
	assert.Equal(t, migrate.LevelError, logger.Level(), "Level should return ErrorLevel after change")
}

func TestZerologAdapter_SetLevel(t *testing.T) {
	// Create test logger
	logger := New(migrate.LevelInfo)

	// Set a new level
	logger.SetLevel(migrate.LevelWarn)

	// Verify the level has been updated
	assert.Equal(t, migrate.LevelWarn, logger.Level(), "Log level should be updated to WarnLevel")

	// Test concurrent access
	var wg sync.WaitGroup
	wg.Add(2)

	go func() {
		defer wg.Done()
		for i := 0; i < 100; i++ {
			logger.SetLevel(migrate.LevelDebug)
		}
	}()

	go func() {
		defer wg.Done()
		for i := 0; i < 100; i++ {
			_ = logger.Level()
		}
	}()

	wg.Wait()
}

func TestZerologAdapter_Log(t *testing.T) {
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
			// Create an in-memory writer for testing
			var buf bytes.Buffer

			// Create a test zerolog logger
			testLogger := zerolog.New(&buf)

			// Create our adapter with the configured level
			adapter := NewWith(testLogger, tt.loggerLevel)
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
				output := buf.String()
				assert.NotEmpty(t, output, "Expected log output")

				// Decode the JSON to validate its content
				var logMap map[string]any
				err := json.Unmarshal(buf.Bytes(), &logMap)
				require.NoError(t, err, "Should be able to unmarshal log JSON")

				// Check log level
				if level, ok := logMap["level"].(string); ok {
					assert.Equal(t, tt.expectedLevel, level, "Log level should match expected")
				}

				// Check message
				if msg, ok := logMap["message"].(string); ok {
					assert.Equal(t, tt.message, msg, "Log message should match expected")
				}

				// Check fields
				for _, field := range tt.fields {
					value, exists := logMap[field.Key]
					assert.True(t, exists, "Field key should exist in log")

					// Check value type-specific matching
					switch v := field.Value.(type) {
					case string:
						assert.Equal(t, v, value, "String field value should match expected")
					case int:
						assert.Equal(t, float64(v), value, "Int field value should match expected")
					case float64:
						assert.Equal(t, v, value, "Float field value should match expected")
					case bool:
						assert.Equal(t, v, value, "Bool field value should match expected")
					default:
						assert.NotNil(t, value, "Complex field value should exist")
					}
				}
			} else {
				// There should be no output
				assert.Empty(t, buf.String(), "Expected no log output")
			}
		})
	}
}

func TestAllFieldTypes(t *testing.T) {
	// Create an in-memory writer for testing
	var buf bytes.Buffer

	// Create a test zerolog logger
	testLogger := zerolog.New(&buf)

	// Create our adapter with debug level to allow all logs
	adapter := NewWith(testLogger, migrate.LevelDebug)

	// Test all field types
	adapter.Info("test all types",
		migrate.Field{Key: "string", Value: "text"},
		migrate.Field{Key: "int", Value: 42},
		migrate.Field{Key: "float", Value: 3.14},
		migrate.Field{Key: "bool", Value: true},
		migrate.Field{Key: "complex", Value: struct{ Name string }{"test"}},
	)

	// Verify output
	output := buf.String()
	assert.NotEmpty(t, output, "Expected log output")

	// Decode the JSON to validate its content
	var logMap map[string]interface{}
	err := json.Unmarshal(buf.Bytes(), &logMap)
	require.NoError(t, err, "Should be able to unmarshal log JSON")

	// Check all fields exist
	assert.Equal(t, "text", logMap["string"], "String field should match")
	assert.Equal(t, float64(42), logMap["int"], "Int field should match")
	assert.Equal(t, 3.14, logMap["float"], "Float field should match")
	assert.Equal(t, true, logMap["bool"], "Bool field should match")
	assert.NotNil(t, logMap["complex"], "Complex field should exist")
}

func TestLogAllLevels(t *testing.T) {
	// Create buffers to capture each level
	buffers := make(map[string]*bytes.Buffer)
	logEvents := map[migrate.LogLevel]string{
		migrate.LevelDebug: "debug",
		migrate.LevelInfo:  "info",
		migrate.LevelWarn:  "warn",
		migrate.LevelError: "error",
	}

	for _, level := range []migrate.LogLevel{migrate.LevelDebug, migrate.LevelInfo, migrate.LevelWarn, migrate.LevelError} {
		buffers[logEvents[level]] = &bytes.Buffer{}

		// Create a test zerolog logger for each level
		testLogger := zerolog.New(buffers[logEvents[level]])

		// Create our adapter with debug level to allow all logs
		adapter := NewWith(testLogger, migrate.LevelDebug)

		// Log at the specific level
		switch level {
		case migrate.LevelError:
			adapter.Error("test message")
		case migrate.LevelWarn:
			adapter.Warn("test message")
		case migrate.LevelInfo:
			adapter.Info("test message")
		case migrate.LevelDebug:
			adapter.Debug("test message")
		}
		//adapter.Log(level, "test message")

		// Verify output exists
		assert.NotEmpty(t, buffers[logEvents[level]].String(), "Expected log output for level: "+logEvents[level])

		// Decode the JSON to validate its level
		var logMap map[string]interface{}
		err := json.Unmarshal(buffers[logEvents[level]].Bytes(), &logMap)
		require.NoError(t, err, "Should be able to unmarshal log JSON")

		// Check log level
		assert.Equal(t, logEvents[level], logMap["level"], "Log level should match expected")
	}
}

func TestConcurrentLogging(t *testing.T) {
	// Create an in-memory writer for testing
	var buf bytes.Buffer
	var mu sync.Mutex

	// Create a writer that synchronizes access to the buffer
	safeWriter := zerolog.ConsoleWriter{Out: syncWriter{Writer: &buf, mu: &mu}}

	// Create a test zerolog logger
	testLogger := zerolog.New(safeWriter)

	// Create our adapter with debug level to allow all logs
	adapter := NewWith(testLogger, migrate.LevelDebug)

	// Run multiple goroutines that log concurrently
	var wg sync.WaitGroup
	numGoroutines := 10
	messagesPerGoroutine := 10
	wg.Add(numGoroutines)

	for i := 0; i < numGoroutines; i++ {
		go func(id int) {
			defer wg.Done()
			for j := 0; j < messagesPerGoroutine; j++ {
				// Log with different levels
				level := migrate.LogLevel(j % 4)
				switch level {
				case migrate.LevelError:
					adapter.Error("concurrent log test", migrate.Field{Key: "goroutine", Value: id}, migrate.Field{Key: "iteration", Value: j})
				case migrate.LevelWarn:
					adapter.Warn("concurrent log test", migrate.Field{Key: "goroutine", Value: id}, migrate.Field{Key: "iteration", Value: j})
				case migrate.LevelInfo:
					adapter.Info("concurrent log test", migrate.Field{Key: "goroutine", Value: id}, migrate.Field{Key: "iteration", Value: j})
				case migrate.LevelDebug:
					adapter.Debug("concurrent log test", migrate.Field{Key: "goroutine", Value: id}, migrate.Field{Key: "iteration", Value: j})
				}
			}
		}(i)
	}

	wg.Wait()

	// Check that logs were written (we can't easily verify the count due to the console writer format)
	assert.NotEmpty(t, buf.String(), "Expected log output from concurrent logging")
}

// syncWriter is a simple wrapper that provides synchronized access to an io.Writer
type syncWriter struct {
	Writer io.Writer
	mu     *sync.Mutex
}

func (w syncWriter) Write(p []byte) (n int, err error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.Writer.Write(p)
}
