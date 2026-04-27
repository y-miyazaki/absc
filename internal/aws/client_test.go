package aws

import (
	"context"
	"errors"
	"strings"
	"testing"
)

func TestNewConfig_SetsRegion(t *testing.T) {
	t.Setenv("AWS_EC2_METADATA_DISABLED", "true")

	region := "ap-northeast-1"
	cfg, err := NewConfig(context.Background(), region, "")
	if err != nil {
		t.Fatalf("NewConfig() error = %v", err)
	}
	if cfg.Region != region {
		t.Fatalf("cfg.Region = %q, want %q", cfg.Region, region)
	}
}

func TestNewConfig_InvalidProfileReturnsWrappedError(t *testing.T) {
	t.Setenv("AWS_EC2_METADATA_DISABLED", "true")

	_, err := NewConfig(context.Background(), "", "this-profile-should-not-exist-123456")
	if err == nil {
		t.Fatal("NewConfig() error = nil, want error")
	}
	if !strings.Contains(err.Error(), "failed to load aws config") {
		t.Fatalf("error message = %q, want prefix %q", err.Error(), "failed to load aws config")
	}
	if errors.Unwrap(err) == nil {
		t.Fatal("expected wrapped error, got nil unwrap")
	}
}
