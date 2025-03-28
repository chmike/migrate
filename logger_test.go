package migrate

import (
	"bytes"
	"context"
	"log"
	"log/slog"
	"strings"
	"testing"
)

// testCapturingHandler is slog handler to capture logs.
type testCapturingHandler struct {
	logs   []string
	level  slog.Level
	called bool
}

func (h *testCapturingHandler) Enabled(ctx context.Context, level slog.Level) bool {
	return level >= h.level
}

func (h *testCapturingHandler) Handle(ctx context.Context, record slog.Record) error {
	h.called = true
	var sb strings.Builder
	sb.WriteString(record.Level.String())
	sb.WriteString(" ")
	sb.WriteString(record.Message)

	record.Attrs(func(attr slog.Attr) bool {
		sb.WriteString(" ")
		sb.WriteString(attr.Key)
		sb.WriteString("=")
		sb.WriteString(attr.Value.String())
		return true
	})

	h.logs = append(h.logs, sb.String())
	return nil
}

func (h *testCapturingHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	return h
}

func (h *testCapturingHandler) WithGroup(name string) slog.Handler {
	return h
}

func TestNilAdapter(t *testing.T) {
	logger := NewNilLogger()

	if logger.Level() != LevelInfo {
		t.Errorf("expect %v, got %v", LevelInfo, logger.Level())
	}

	logger.SetLevel(LevelDebug)
	if logger.Level() != LevelDebug {
		t.Errorf("expect %v, got %v", LevelDebug, logger.Level())
	}

	logger.Error("Test message", F("key", "value"))
	logger.Warn("Test message", F("key", "value"))
	logger.Info("Test message", F("key", "value"))
	logger.Debug("Test message", F("key", "value"))

	logger.SetLevel(LevelNoLog)
	logger.Error("Test message", F("key", "value"))
	logger.Warn("Test message", F("key", "value"))
	logger.Info("Test message", F("key", "value"))
	logger.Debug("Test message", F("key", "value"))
}

func TestSlogAdapter(t *testing.T) {
	handler := &testCapturingHandler{level: slog.LevelDebug}
	slogLogger := slog.New(handler)
	logger := NewSlogLoggerWith(slogLogger, LevelInfo)

	if logger.Level() != LevelInfo {
		t.Errorf("expect %v, got %v", LevelInfo, logger.Level())
	}

	logger.SetLevel(LevelDebug)
	if logger.Level() != LevelDebug {
		t.Errorf("expect %v, got %v", LevelDebug, logger.Level())
	}

	logger.SetLevel(LevelWarn)

	logger.Info("Info message", F("key1", "value1"))
	if len(handler.logs) != 0 {
		t.Errorf("message logged: %v", handler.logs)
	}

	logger.Warn("Warning message", F("key2", "value2"))
	logger.Error("Error message", F("key3", "value3"))

	if len(handler.logs) != 2 {
		t.Errorf("invalid number logged messages, expect 2, got %d", len(handler.logs))
	}

	if !strings.Contains(handler.logs[0], "WARN") || !strings.Contains(handler.logs[0], "Warning message") || !strings.Contains(handler.logs[0], "key2=value2") {
		t.Errorf("first incorrect log: %s", handler.logs[0])
	}

	if !strings.Contains(handler.logs[1], "ERROR") || !strings.Contains(handler.logs[1], "Error message") || !strings.Contains(handler.logs[1], "key3=value3") {
		t.Errorf("second incorrect log: %s", handler.logs[1])
	}

	logger.SetLevel(LevelDebug)
	logger.Debug("Debug message", F("key2", "value2"))
	if len(handler.logs) != 3 {
		t.Errorf("invalid number logged messages, expect 3, got %d", len(handler.logs))
	}
	if !strings.Contains(handler.logs[2], "DEBUG") || !strings.Contains(handler.logs[2], "Debug message") || !strings.Contains(handler.logs[2], "key2=value2") {
		t.Errorf("third incorrect log: %s", handler.logs[1])
	}

	logger.SetLevel(LevelNoLog)
	logger.Debug("no show message")
	logger.Info("no show message")
	logger.Warn("no show message")
	logger.Error("no show message")
	if len(handler.logs) != 3 {
		t.Errorf("invalid number logged messages, expect 3, got %d", len(handler.logs))
	}
}

func TestLogAdapter(t *testing.T) {
	var buf bytes.Buffer
	stdLogger := log.New(&buf, "", 0) // without prefix to simplify the test
	logger := NewLogLoggerWith(stdLogger, LevelInfo)

	if logger.Level() != LevelInfo {
		t.Errorf("expect %v, got %v", LevelInfo, logger.Level())
	}

	logger.SetLevel(LevelWarn)
	if logger.Level() != LevelWarn {
		t.Errorf("expect %v, got %v", LevelWarn, logger.Level())
	}

	logger.Info("Info message", F("key1", "value1"))
	if buf.Len() != 0 {
		t.Errorf("unexpected logged message: %s", buf.String())
	}

	logger.Warn("Warning message", F("key2", "value2"))
	output := buf.String()
	buf.Reset()

	if !strings.Contains(output, "[WARN]") || !strings.Contains(output, "Warning message") || !strings.Contains(output, "key2='value2'") {
		t.Errorf("incorrect logged message: %s", output)
	}

	logger.Error("Error message", F("key3", 123))
	output = buf.String()

	if !strings.Contains(output, "[ERROR]") || !strings.Contains(output, "Error message") || !strings.Contains(output, "key3='123'") {
		t.Errorf("incorrect logged message: %s", output)
	}

	logger.SetLevel(LevelDebug)

	logger.Debug("Debug message")
	output = buf.String()

	if !strings.Contains(output, "[DEBUG]") || !strings.Contains(output, "Debug message") {
		t.Errorf("incorrect logged message: %s", output)
	}

	logger.Info("Info message")
	output = buf.String()

	if !strings.Contains(output, "[INFO]") || !strings.Contains(output, "Info message") {
		t.Errorf("incorrect logged message: %s", output)
	}

	buf.Reset()
	logger.SetLevel(LevelNoLog)
	logger.Debug("no show message")
	logger.Info("no show message")
	logger.Warn("no show message")
	logger.Error("no show message")
	if buf.Len() != 0 {
		t.Fatalf("expect no logging, got %s", buf.String())
	}

}

func TestDefaultLoggers(t *testing.T) {
	slogLogger := NewSlogLoggerWith(nil, LevelInfo)
	if slogLogger == nil {
		t.Error("expect non-nil logger")
	}

	logLogger := NewLogLoggerWith(nil, LevelInfo)
	if logLogger == nil {
		t.Error("expect non-nil logger")
	}

	defaultSlog := NewSlogLogger(LevelInfo)
	if defaultSlog == nil {
		t.Error("expect non-nil logger")
	}

	logLogger = NewLogLogger(LevelInfo)
	if logLogger == nil {
		t.Error("expect non-nil logger")
	}

	defaultSlog.Info("Test message")
}

func TestConcurrentAccess(t *testing.T) {
	logger := NewNilLogger()

	done := make(chan bool)
	go func() {
		for i := 0; i < 100; i++ {
			logger.SetLevel(LogLevel(i % 4))
		}
		done <- true
	}()

	go func() {
		for i := 0; i < 100; i++ {
			_ = logger.Level()
		}
		done <- true
	}()

	<-done
	<-done
}
