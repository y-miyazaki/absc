// Package aws provides shared AWS SDK configuration helpers for the CLI.
package aws

import (
	"context"
	"fmt"

	awssdk "github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
)

// NewConfig loads the default AWS SDK configuration with optional overrides.
func NewConfig(ctx context.Context, region, profile string) (awssdk.Config, error) {
	var opts []func(*config.LoadOptions) error
	if region != "" {
		opts = append(opts, config.WithRegion(region))
	}
	if profile != "" {
		opts = append(opts, config.WithSharedConfigProfile(profile))
	}
	cfg, err := config.LoadDefaultConfig(ctx, opts...)
	if err != nil {
		return awssdk.Config{}, fmt.Errorf("failed to load aws config: %w", err)
	}
	return cfg, nil
}
