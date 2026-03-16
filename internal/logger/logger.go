// Package logger provides structured logging utilities.
package logger

import (
	"io"
	"log/slog"
	"os"
)

// Logger wraps slog.Logger with custom configuration
type Logger struct {
	*slog.Logger
	writer io.Writer
}

// NewText creates a new logger with text output format
func NewText(level slog.Level) *Logger {
	opts := &slog.HandlerOptions{
		Level: level,
	}
	handler := slog.NewTextHandler(os.Stderr, opts)
	return &Logger{
		Logger: slog.New(handler),
		writer: os.Stderr,
	}
}

// NewJSON creates a new logger with JSON output format
func NewJSON(level slog.Level) *Logger {
	opts := &slog.HandlerOptions{
		Level: level,
	}
	handler := slog.NewJSONHandler(os.Stderr, opts)
	return &Logger{
		Logger: slog.New(handler),
		writer: os.Stderr,
	}
}

// NewDefault creates a logger with default settings (INFO level, text format)
// Previously the default was JSON; switch to text to produce human-readable logs.
func NewDefault() *Logger {
	return NewText(slog.LevelInfo)
}

// SetOutput sets the output destination for the logger
func (l *Logger) SetOutput(w io.Writer) {
	l.writer = w
	// Recreate the handler with the new writer
	// For simplicity, we'll recreate as text handler
	opts := &slog.HandlerOptions{
		Level: slog.LevelInfo, // Default level
	}
	handler := slog.NewTextHandler(l.writer, opts)
	l.Logger = slog.New(handler)
}

// GetWriter returns the current writer
func (l *Logger) GetWriter() io.Writer {
	return l.writer
}

// Info logs at INFO level
func (l *Logger) Info(msg string, args ...any) {
	l.Logger.Info(msg, args...)
}

// Error logs at ERROR level
func (l *Logger) Error(msg string, args ...any) {
	l.Logger.Error(msg, args...)
}

// Debug logs at DEBUG level
func (l *Logger) Debug(msg string, args ...any) {
	l.Logger.Debug(msg, args...)
}

// Warn logs at WARN level
func (l *Logger) Warn(msg string, args ...any) {
	l.Logger.Warn(msg, args...)
}
