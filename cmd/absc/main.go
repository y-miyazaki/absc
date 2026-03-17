// Package main provides the absc CLI entry point.
package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/urfave/cli/v2"

	awscfg "github.com/y-miyazaki/absc/internal/aws"
	"github.com/y-miyazaki/absc/internal/aws/resources"
	"github.com/y-miyazaki/absc/internal/exporter"
	"github.com/y-miyazaki/go-common/pkg/logger"
	"github.com/y-miyazaki/go-common/pkg/utils/aws/validation"
)

// Command defaults and flag names are centralized for lint compliance.
const (
	// Account and ARN parsing defaults keep CLI behavior explicit.
	accountIDUnknown      = "unknown"
	accountIndex          = 4
	arnSplitParts         = 6
	defaultLookbackHours  = 24
	defaultMaxConcurrency = 5
	defaultMaxResults     = 50
	defaultOutputDir      = "./output"
	defaultRegion         = "ap-northeast-1"
	// defaultTimeout bounds the full CLI execution to avoid hanging runs.
	defaultTimeout         = 10 * time.Minute
	defaultTimezone        = "UTC"
	outputDirPermission    = 0o750
	regionsFlagName        = "regions"
	regionFlagName         = "region"
	regionShortAlias       = "r"
	regionValueMinParts    = accountIndex + 1
	outputDirFlagName      = "output-dir"
	profileFlagName        = "profile"
	lookbackHoursFlagName  = "lookback-hours"
	maxConcurrencyFlagName = "max-concurrency"
	maxResultsFlagName     = "max-results"
	timeoutFlagName        = "timeout"
	timezoneFlagName       = "timezone"
)

var (
	// errInvalidMaxResults is returned when the caller requests zero records.
	errInvalidMaxResults = errors.New("max-results must be >= 1")

	// Injected functions keep the command path testable without AWS access.
	checkAWSCredentials = validation.CheckAWSCredentials
	collectSchedules    = resources.Collect
	mkdirAll            = os.MkdirAll
	newAWSConfig        = awscfg.NewConfig
	nowFunc             = time.Now
	writeHTML           = exporter.WriteHTML
	writeJSON           = exporter.WriteJSON
)

// parseRegions normalizes a comma-separated region list and removes duplicates.
func parseRegions(v string) []string {
	parts := strings.Split(v, ",")
	regions := make([]string, 0, len(parts))
	seen := make(map[string]struct{}, len(parts))
	for _, p := range parts {
		t := strings.TrimSpace(p)
		if t == "" {
			continue
		}
		if _, ok := seen[t]; ok {
			continue
		}
		seen[t] = struct{}{}
		regions = append(regions, t)
	}
	return regions
}

// regionArg resolves the region flag with support for the deprecated alias.
func regionArg(c *cli.Context) string {
	if c.IsSet(regionsFlagName) {
		if v := strings.TrimSpace(c.String(regionsFlagName)); v != "" {
			return v
		}
	}
	if c.IsSet(regionFlagName) {
		if v := strings.TrimSpace(c.String(regionFlagName)); v != "" {
			return v
		}
	}
	if v := strings.TrimSpace(c.String(regionFlagName)); v != "" {
		return v
	}
	return defaultRegion
}

// commandContext returns the CLI-bound context and falls back to background.
func commandContext(c *cli.Context) context.Context {
	if c != nil && c.Context != nil {
		return c.Context
	}
	return context.Background()
}

// accountIDFromARN extracts the AWS account ID from a caller identity ARN.
func accountIDFromARN(arn string) string {
	parts := strings.SplitN(arn, ":", arnSplitParts)
	if len(parts) < regionValueMinParts || parts[accountIndex] == "" {
		return accountIDUnknown
	}
	return parts[accountIndex]
}

// newApp builds the CLI application and wires the main action.
func newApp(l interface {
	Error(msg string, args ...any)
	Info(msg string, args ...any)
}) *cli.App {
	return &cli.App{
		Name:  "absc",
		Usage: "Collect cron schedules from AWS and render timeline HTML",
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:    profileFlagName,
				Usage:   "AWS profile to use",
				EnvVars: []string{"AWS_PROFILE", "AWS_DEFAULT_PROFILE"},
			},
			&cli.StringFlag{
				Name:    regionFlagName,
				Aliases: []string{regionShortAlias},
				Usage:   "AWS region(s) to use (comma-separated list accepted)",
				EnvVars: []string{"AWS_DEFAULT_REGION"},
				Value:   defaultRegion,
			},
			&cli.StringFlag{
				Name:   regionsFlagName,
				Usage:  "Deprecated alias of --region (comma-separated list accepted)",
				Hidden: true,
			},
			&cli.StringFlag{Name: timezoneFlagName, Usage: "IANA timezone", Value: defaultTimezone},
			&cli.StringFlag{Name: outputDirFlagName, Aliases: []string{"D"}, Usage: "Output base directory", Value: defaultOutputDir},
			&cli.IntFlag{Name: lookbackHoursFlagName, Usage: "Execution history lookback hours", Value: defaultLookbackHours},
			&cli.IntFlag{Name: maxConcurrencyFlagName, Usage: "Max concurrent resource collectors", Value: defaultMaxConcurrency},
			&cli.IntFlag{Name: maxResultsFlagName, Usage: "Max executions/jobs per target", Value: defaultMaxResults},
			&cli.DurationFlag{Name: timeoutFlagName, Usage: "Overall command timeout", Value: defaultTimeout},
		},
		Action: func(c *cli.Context) error {
			return runCommand(c, l)
		},
	}
}

// runCommand executes the absc collection flow for a prepared CLI context.
func runCommand(c *cli.Context, l interface {
	Error(msg string, args ...any)
	Info(msg string, args ...any)
}) error {
	ctx := commandContext(c)
	// Apply an overall command timeout that propagates to all downstream AWS calls.
	if timeout := c.Duration(timeoutFlagName); timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, timeout)
		defer cancel()
	}

	if c.Int(maxResultsFlagName) < 1 {
		return errInvalidMaxResults
	}

	// Resolve CLI options before touching AWS services.
	loc, err := time.LoadLocation(c.String(timezoneFlagName))
	if err != nil {
		return fmt.Errorf("failed to load timezone: %w", err)
	}

	regions := parseRegions(regionArg(c))
	if len(regions) == 0 {
		regions = []string{defaultRegion}
	}

	// Load config and verify credentials up front for clearer failures.
	cfg, err := newAWSConfig(ctx, regions[0], c.String(profileFlagName))
	if err != nil {
		return fmt.Errorf("failed to initialize aws config: %w", err)
	}

	l.Info("checking AWS credentials")
	identityARN, err := checkAWSCredentials(ctx, &cfg)
	if err != nil {
		return fmt.Errorf("aws credentials check failed: %w", err)
	}

	l.Info("AWS identity", "identity", identityARN)
	accountID := accountIDFromARN(identityARN)

	// Collect schedules first, then persist both JSON and HTML outputs.
	now := nowFunc().In(loc)
	since := now.Add(-time.Duration(c.Int(lookbackHoursFlagName)) * time.Hour)
	schedules, errs := collectSchedules(ctx, &cfg, resources.CollectOptions{
		MaxConcurrency: c.Int(maxConcurrencyFlagName),
		MaxResults:     c.Int(maxResultsFlagName),
		Regions:        regions,
		Since:          since,
	})

	result := exporter.BuildOutput(accountID, now, loc, schedules, errs)
	outDir := filepath.Join(c.String(outputDirFlagName), accountID, "cron")
	if mkErr := mkdirAll(outDir, outputDirPermission); mkErr != nil {
		return fmt.Errorf("failed to create output directory: %w", mkErr)
	}

	jsonPath := filepath.Join(outDir, "schedules.json")
	if wErr := writeJSON(jsonPath, &result); wErr != nil {
		return fmt.Errorf("failed to write json: %w", wErr)
	}

	htmlPath := filepath.Join(outDir, "index.html")
	if hErr := writeHTML(htmlPath, &result); hErr != nil {
		return fmt.Errorf("failed to write html: %w", hErr)
	}

	l.Info("generated outputs", "json_path", jsonPath, "html_path", htmlPath)
	return nil
}

// main runs the CLI application and exits on fatal errors.
func main() {
	l := logger.NewSlogLogger(&logger.SlogConfig{Level: logger.LevelInfo, Format: "text"})
	app := newApp(l)

	if err := app.Run(os.Args); err != nil {
		l.Error("failed to run app", "error", err)
		os.Exit(1)
	}
}
