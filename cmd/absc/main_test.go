package main

import (
	"context"
	"errors"
	"flag"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	awssdk "github.com/aws/aws-sdk-go-v2/aws"
	"github.com/urfave/cli/v2"

	"github.com/y-miyazaki/absc/internal/aws/resources"
	"github.com/y-miyazaki/absc/internal/exporter"
)

type noopLogger struct{}

func (noopLogger) Error(string, ...any) {}

func (noopLogger) Info(string, ...any) {}

func newTestContext(t *testing.T, values map[string]string) *cli.Context {
	t.Helper()

	app := newApp(noopLogger{})
	set := flag.NewFlagSet("test", flag.ContinueOnError)
	for _, f := range app.Flags {
		if err := f.Apply(set); err != nil {
			t.Fatalf("failed to apply flag: %v", err)
		}
	}
	for name, value := range values {
		if err := set.Set(name, value); err != nil {
			t.Fatalf("failed to set flag %s: %v", name, err)
		}
	}

	return cli.NewContext(app, set, nil)
}

func restoreCommandDeps() func() {
	originalCheck := checkAWSCredentials
	originalCollect := collectSchedules
	originalMkdirAll := mkdirAll
	originalNewAWSConfig := newAWSConfig
	originalNowFunc := nowFunc
	originalBuildOutput := buildOutput
	originalWriteErrorsHTML := writeErrorsHTML
	originalWriteSlotIssuesCSV := writeSlotIssuesCSV
	originalWriteHTML := writeHTML
	originalWriteJSON := writeJSON

	return func() {
		checkAWSCredentials = originalCheck
		collectSchedules = originalCollect
		mkdirAll = originalMkdirAll
		newAWSConfig = originalNewAWSConfig
		nowFunc = originalNowFunc
		buildOutput = originalBuildOutput
		writeErrorsHTML = originalWriteErrorsHTML
		writeSlotIssuesCSV = originalWriteSlotIssuesCSV
		writeHTML = originalWriteHTML
		writeJSON = originalWriteJSON
	}
}

func TestParseRegions(t *testing.T) {
	tests := []struct {
		name  string
		input string
		want  []string
	}{
		{
			name:  "deduplicates and trims",
			input: " ap-northeast-1,us-east-1,ap-northeast-1 ,, us-east-1 ",
			want:  []string{"ap-northeast-1", "us-east-1"},
		},
		{
			name:  "empty input",
			input: " , , ",
			want:  nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := parseRegions(tt.input)
			if len(got) != len(tt.want) {
				t.Fatalf("parseRegions() length = %d, want %d", len(got), len(tt.want))
			}
			for idx := range got {
				if got[idx] != tt.want[idx] {
					t.Fatalf("parseRegions()[%d] = %q, want %q", idx, got[idx], tt.want[idx])
				}
			}
		})
	}
}

func TestRegionArg(t *testing.T) {
	tests := []struct {
		name   string
		values map[string]string
		want   string
	}{
		{
			name:   "prefers deprecated regions flag when set",
			values: map[string]string{regionsFlagName: "us-east-1", regionFlagName: "ap-northeast-1"},
			want:   "us-east-1",
		},
		{
			name:   "uses region flag",
			values: map[string]string{regionFlagName: "eu-west-1"},
			want:   "eu-west-1",
		},
		{
			name:   "falls back to default",
			values: map[string]string{regionFlagName: ""},
			want:   defaultRegion,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := newTestContext(t, tt.values)
			if got := regionArg(ctx); got != tt.want {
				t.Fatalf("regionArg() = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestAccountIDFromARN(t *testing.T) {
	tests := []struct {
		name string
		arn  string
		want string
	}{
		{
			name: "valid ARN",
			arn:  "arn:aws:iam::123456789012:role/Admin",
			want: "123456789012",
		},
		{
			name: "empty account in ARN",
			arn:  "arn:aws:iam:::role/Admin",
			want: "unknown",
		},
		{
			name: "invalid ARN format",
			arn:  "invalid",
			want: "unknown",
		},
		{
			name: "empty string",
			arn:  "",
			want: "unknown",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := accountIDFromARN(tt.arn); got != tt.want {
				t.Fatalf("accountIDFromARN() = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestRunCommand_MaxResultsValidation(t *testing.T) {
	ctx := newTestContext(t, map[string]string{maxResultsFlagName: "0"})
	err := runCommand(ctx, noopLogger{})
	if !errors.Is(err, errInvalidMaxResults) {
		t.Fatalf("runCommand() error = %v, want %v", err, errInvalidMaxResults)
	}
}

func TestRunCommand_InvalidTimezone(t *testing.T) {
	ctx := newTestContext(t, map[string]string{timezoneFlagName: "Mars/Phobos"})
	err := runCommand(ctx, noopLogger{})
	if err == nil || !strings.Contains(err.Error(), "failed to load timezone") {
		t.Fatalf("runCommand() error = %v, want timezone error", err)
	}
}

func TestRunCommand_AWSConfigError(t *testing.T) {
	defer restoreCommandDeps()()

	newAWSConfig = func(context.Context, string, string) (awssdk.Config, error) {
		return awssdk.Config{}, errors.New("config error")
	}

	ctx := newTestContext(t, map[string]string{})
	err := runCommand(ctx, noopLogger{})
	if err == nil || !strings.Contains(err.Error(), "failed to initialize aws config") {
		t.Fatalf("runCommand() error = %v, want config error", err)
	}
}

func TestRunCommand_Success(t *testing.T) {
	defer restoreCommandDeps()()

	var csvPath string
	var errorsPath string
	var jsonPath string
	var htmlPath string
	var mkdirPath string
	fixedNow := time.Date(2026, time.March, 17, 10, 0, 0, 0, time.UTC)

	newAWSConfig = func(context.Context, string, string) (awssdk.Config, error) {
		return awssdk.Config{}, nil
	}
	checkAWSCredentials = func(context.Context, *awssdk.Config) (string, error) {
		return "arn:aws:iam::123456789012:role/Admin", nil
	}
	collectSchedules = func(context.Context, *awssdk.Config, resources.CollectOptions) ([]resources.Schedule, []resources.ErrorRecord) {
		return nil, nil
	}
	mkdirAll = func(path string, _ os.FileMode) error {
		mkdirPath = path
		return nil
	}
	writeJSON = func(path string, _ *exporter.Output) error {
		jsonPath = path
		return nil
	}
	writeHTML = func(path string, _ *exporter.Output) error {
		htmlPath = path
		return nil
	}
	writeErrorsHTML = func(path string, _ *exporter.Output) error {
		errorsPath = path
		return nil
	}
	buildOutput = func(accountID string, now, since time.Time, loc *time.Location, schedules []resources.Schedule, errs []resources.ErrorRecord, options exporter.BuildOutputOptions) exporter.Output {
		return exporter.BuildOutput(accountID, now, since, loc, schedules, errs)
	}
	writeSlotIssuesCSV = func(path string, _ *exporter.Output) error {
		csvPath = path
		return nil
	}
	nowFunc = func() time.Time {
		return fixedNow
	}

	baseDir := t.TempDir()
	ctx := newTestContext(t, map[string]string{
		outputDirFlagName: baseDir,
		regionFlagName:    "ap-northeast-1,us-east-1",
		timezoneFlagName:  "UTC",
	})

	if err := runCommand(ctx, noopLogger{}); err != nil {
		t.Fatalf("runCommand() unexpected error: %v", err)
	}

	wantDir := filepath.Join(baseDir, "123456789012", "schedules")
	if mkdirPath != wantDir {
		t.Fatalf("mkdirAll path = %q, want %q", mkdirPath, wantDir)
	}
	if jsonPath != filepath.Join(wantDir, "schedules.json") {
		t.Fatalf("writeJSON path = %q", jsonPath)
	}
	if htmlPath != filepath.Join(wantDir, "index.html") {
		t.Fatalf("writeHTML path = %q", htmlPath)
	}
	if errorsPath != filepath.Join(wantDir, defaultErrorsHTMLFile) {
		t.Fatalf("writeErrorsHTML path = %q", errorsPath)
	}
	if csvPath != filepath.Join(wantDir, defaultIssuesCSVFile) {
		t.Fatalf("writeSlotIssuesCSV path = %q", csvPath)
	}
}
