package aws

import (
	"context"
	"errors"
	"strings"
	"testing"
)

func TestNewConfig(t *testing.T) {
	t.Setenv("AWS_EC2_METADATA_DISABLED", "true")

	tests := []struct {
		name        string
		region      string
		profile     string
		wantRegion  string
		wantErrText string
		wantErr     bool
	}{
		{
			name:       "sets region",
			region:     "ap-northeast-1",
			wantRegion: "ap-northeast-1",
		},
		{
			name:        "invalid profile returns wrapped error",
			profile:     "this-profile-should-not-exist-123456",
			wantErr:     true,
			wantErrText: "failed to load aws config",
		},
	}

	for i := range tests {
		tt := tests[i]
		t.Run(tt.name, func(t *testing.T) {
			cfg, err := NewConfig(context.Background(), tt.region, tt.profile)
			if tt.wantErr {
				if err == nil {
					t.Fatal("NewConfig() error = nil, want error")
				}
				if !strings.Contains(err.Error(), tt.wantErrText) {
					t.Fatalf("NewConfig() error = %q, want containing %q", err.Error(), tt.wantErrText)
				}
				if errors.Unwrap(err) == nil {
					t.Fatal("NewConfig() unwrap = nil, want wrapped error")
				}
				return
			}
			if err != nil {
				t.Fatalf("NewConfig() error = %v", err)
			}
			if cfg.Region != tt.wantRegion {
				t.Fatalf("cfg.Region = %q, want %q", cfg.Region, tt.wantRegion)
			}
		})
	}
}
