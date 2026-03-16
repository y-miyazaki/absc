package logger_test

import (
	"bytes"
	"strings"
	"testing"

	"log/slog"

	"github.com/y-miyazaki/arc/internal/logger"
)

func TestNewText(t *testing.T) {
	tests := []struct {
		name  string
		level slog.Level
	}{
		{
			name:  "debug level",
			level: slog.LevelDebug,
		},
		{
			name:  "info level",
			level: slog.LevelInfo,
		},
		{
			name:  "warn level",
			level: slog.LevelWarn,
		},
		{
			name:  "error level",
			level: slog.LevelError,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			l := logger.NewText(tt.level)
			if l == nil {
				t.Errorf("NewText() returned nil")
			}
			if l.Logger == nil {
				t.Errorf("NewText() returned logger with nil Logger field")
			}
		})
	}
}

func TestNewJSON(t *testing.T) {
	tests := []struct {
		name  string
		level slog.Level
	}{
		{
			name:  "debug level",
			level: slog.LevelDebug,
		},
		{
			name:  "info level",
			level: slog.LevelInfo,
		},
		{
			name:  "warn level",
			level: slog.LevelWarn,
		},
		{
			name:  "error level",
			level: slog.LevelError,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			l := logger.NewJSON(tt.level)
			if l == nil {
				t.Errorf("NewJSON() returned nil")
			}
			if l.Logger == nil {
				t.Errorf("NewJSON() returned logger with nil Logger field")
			}
		})
	}
}

func TestNewDefault(t *testing.T) {
	l := logger.NewDefault()
	if l == nil {
		t.Errorf("NewDefault() returned nil")
	}
	if l.Logger == nil {
		t.Errorf("NewDefault() returned logger with nil Logger field")
	}
}

func TestLoggerMethods(t *testing.T) {
	// Create a buffer to capture log output
	var buf bytes.Buffer

	// Create a logger that writes to our buffer
	opts := &slog.HandlerOptions{
		Level: slog.LevelDebug,
	}
	handler := slog.NewTextHandler(&buf, opts)
	testLogger := &logger.Logger{
		Logger: slog.New(handler),
	}

	tests := []struct {
		name     string
		logFunc  func()
		expected string
	}{
		{
			name: "Info log",
			logFunc: func() {
				testLogger.Info("test info message", "key", "value")
			},
			expected: "INFO",
		},
		{
			name: "Error log",
			logFunc: func() {
				testLogger.Error("test error message", "error", "test error")
			},
			expected: "ERROR",
		},
		{
			name: "Debug log",
			logFunc: func() {
				testLogger.Debug("test debug message", "debug", "true")
			},
			expected: "DEBUG",
		},
		{
			name: "Warn log",
			logFunc: func() {
				testLogger.Warn("test warn message", "warning", "test warning")
			},
			expected: "WARN",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			buf.Reset()
			tt.logFunc()
			output := buf.String()
			if !strings.Contains(output, tt.expected) {
				t.Errorf("Expected log output to contain %q, got %q", tt.expected, output)
			}
		})
	}
}

func TestSetOutput(t *testing.T) {
	l := logger.NewDefault()

	// Create a buffer to capture output
	var buf bytes.Buffer
	l.SetOutput(&buf)

	// Log a message
	l.Info("test message", "key", "value")

	// Check that output was written to the buffer
	output := buf.String()
	if output == "" {
		t.Error("Expected output to be written to buffer, but it was empty")
	}
	if !strings.Contains(output, "test message") {
		t.Errorf("Expected output to contain 'test message', got: %s", output)
	}
}
