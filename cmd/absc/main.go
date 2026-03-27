// Package main provides the absc CLI entry point.
//
//revive:disable:comments-density reason: CLI orchestration keeps comments focused on behavior boundaries.
package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"time"

	awssdk "github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/account"
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
	dayDuration           = 24 * time.Hour
	daysAgoFlagName       = "days-ago"
	defaultDaysAgo        = 1
	defaultErrorsHTMLFile = "errors.html"
	defaultIssuesCSVFile  = "slot_run_issues.csv"
	defaultMaxConcurrency = 5
	defaultMaxResults     = 144
	defaultOutputDir      = "./output"
	defaultOutputSubDir   = "schedules"
	defaultRegion         = "ap-northeast-1"
	// defaultTimeout bounds the full CLI execution to avoid hanging runs.
	defaultTimeout             = 10 * time.Minute
	defaultTimezone            = "UTC"
	defaultWindowDays          = 1
	includeNonSlotRunsFlagName = "include-non-slot-runs"
	maxConcurrencyFlagName     = "max-concurrency"
	maxResultsFlagName         = "max-results"
	outputDirFlagName          = "output-dir"
	outputDirPermission        = 0o750
	profileFlagName            = "profile"
	regionFlagName             = "region"
	regionsFlagName            = "regions"
	regionShortAlias           = "r"
	regionValueMinParts        = accountIndex + 1
	timeoutFlagName            = "timeout"
	timezoneFlagName           = "timezone"
)

// Build-time version information injected by GoReleaser via ldflags.
var (
	version = "v1.0.12"
)

var (
	// errInvalidDaysAgo is returned when a negative day offset is requested.
	errInvalidDaysAgo = errors.New("days-ago must be >= 0")

	// errInvalidMaxResults is returned when the caller requests zero records.
	errInvalidMaxResults = errors.New("max-results must be >= 1")

	// Injected functions keep the command path testable without AWS access.
	buildOutput         = exporter.BuildOutputWithOptions
	checkAWSCredentials = validation.CheckAWSCredentials
	collectSchedules    = resources.Collect
	getAccountName      = fetchAccountName
	mkdirAll            = os.MkdirAll
	newAccountClient    = func(cfg *awssdk.Config) accountInformationAPI { return account.NewFromConfig(*cfg) }
	newAWSConfig        = awscfg.NewConfig
	nowFunc             = time.Now
	writeErrorsHTML     = exporter.WriteErrorsHTML
	writeHTML           = exporter.WriteHTML
	writeJSON           = exporter.WriteJSON
	writeSlotIssuesCSV  = exporter.WriteSlotRunIssuesCSV
)

// accountInformationAPI wraps the AWS Account API used to resolve account names.
type accountInformationAPI interface {
	GetAccountInformation(
		_ context.Context,
		_ *account.GetAccountInformationInput,
		_ ...func(*account.Options),
	) (*account.GetAccountInformationOutput, error)
}

// main runs the CLI application and exits on fatal errors.
func main() {
	l := logger.NewSlogLogger(&logger.SlogConfig{Level: slog.LevelInfo, Format: "text"})
	app := newApp(l)

	if err := app.Run(os.Args); err != nil {
		l.Error("failed to run app", "error", err)
		os.Exit(1)
	}
}

// accountIDFromARN extracts the AWS account ID from a caller identity ARN.
func accountIDFromARN(arn string) string {
	parts := strings.SplitN(arn, ":", arnSplitParts)
	if len(parts) < regionValueMinParts || parts[accountIndex] == "" {
		return accountIDUnknown
	}
	return parts[accountIndex]
}

// commandContext returns the CLI-bound context and falls back to background.
func commandContext(c *cli.Context) context.Context {
	if c != nil && c.Context != nil {
		return c.Context
	}
	return context.Background()
}

// fetchAccountName retrieves the human-readable account name for the given account ID.
func fetchAccountName(ctx context.Context, cfg *awssdk.Config, accountID string) (string, error) {
	client := newAccountClient(cfg)
	result, err := client.GetAccountInformation(ctx, &account.GetAccountInformationInput{
		AccountId: awssdk.String(accountID),
	})
	if err != nil {
		return "", fmt.Errorf("get account information: %w", err)
	}

	return strings.TrimSpace(awssdk.ToString(result.AccountName)), nil
}

// newApp builds the CLI application and wires the main action.
func newApp(l *logger.SlogLogger) *cli.App {
	return &cli.App{
		Name:    "absc",
		Usage:   "Collect cron schedules from AWS and render timeline HTML",
		Version: version,
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
			&cli.IntFlag{Name: daysAgoFlagName, Usage: "Calendar day offset (0=today, 1=yesterday)", Value: defaultDaysAgo},
			&cli.IntFlag{Name: maxConcurrencyFlagName, Usage: "Max concurrent resource collectors", Value: defaultMaxConcurrency},
			&cli.IntFlag{Name: maxResultsFlagName, Usage: "Max executions/jobs per target", Value: defaultMaxResults},
			&cli.BoolFlag{Name: includeNonSlotRunsFlagName, Usage: "Include runs that do not overlap scheduled slots in output", Value: false},
			&cli.DurationFlag{Name: timeoutFlagName, Usage: "Overall command timeout", Value: defaultTimeout},
		},
		Action: func(c *cli.Context) error {
			return runCommand(c, l)
		},
	}
}

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

// runCommand executes the absc collection flow for a prepared CLI context.
func runCommand(c *cli.Context, l *logger.SlogLogger) error {
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
	if c.Int(daysAgoFlagName) < 0 {
		return errInvalidDaysAgo
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
	accountName, accountNameErr := getAccountName(ctx, &cfg, accountID)
	if accountNameErr != nil {
		l.Warn("failed to fetch AWS account information", "account_id", accountID, "error", accountNameErr)
	}

	// Collect schedules first, then persist both JSON and HTML outputs.
	now := nowFunc().In(loc)
	since := timelineWindowStart(now, c.Int(daysAgoFlagName), loc)
	schedules, errs := collectSchedules(ctx, &cfg, resources.CollectOptions{
		MaxConcurrency: c.Int(maxConcurrencyFlagName),
		MaxResults:     c.Int(maxResultsFlagName),
		Regions:        regions,
		Since:          since,
		Until:          since.Add(defaultWindowDays * dayDuration),
	})

	result := buildOutput(accountID, now, since, loc, schedules, errs, exporter.BuildOutputOptions{
		IncludeNonSlotRuns: c.Bool(includeNonSlotRunsFlagName),
	})
	result.AccountName = accountName
	outDir := filepath.Join(c.String(outputDirFlagName), accountID, defaultOutputSubDir)
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

	errorsPath := filepath.Join(outDir, defaultErrorsHTMLFile)
	if eErr := writeErrorsHTML(errorsPath, &result); eErr != nil {
		return fmt.Errorf("failed to write errors html: %w", eErr)
	}

	csvPath := filepath.Join(outDir, defaultIssuesCSVFile)
	if cErr := writeSlotIssuesCSV(csvPath, &result); cErr != nil {
		return fmt.Errorf("failed to write slot run issues csv: %w", cErr)
	}

	l.Info("generated outputs", "json_path", jsonPath, "html_path", htmlPath, "errors_html_path", errorsPath, "issues_csv_path", csvPath)
	return nil
}

// timelineWindowStart returns the calendar-day start offset by whole days.
func timelineWindowStart(now time.Time, daysAgo int, loc *time.Location) time.Time {
	nowInLoc := now.In(loc)
	todayStart := time.Date(nowInLoc.Year(), nowInLoc.Month(), nowInLoc.Day(), 0, 0, 0, 0, loc)
	return todayStart.AddDate(0, 0, -daysAgo)
}
