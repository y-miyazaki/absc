//revive:disable:comments-density reason: table-driven tests are self-explanatory via subtest names.
package main

import (
	"context"
	"errors"
	"log/slog"
	"os"
	"strings"
	"testing"
	"time"

	awssdk "github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/account"
	"github.com/urfave/cli/v3"

	"github.com/y-miyazaki/absc/cmd/absc/mocks"
	"github.com/y-miyazaki/absc/internal/aws/resources"
	"github.com/y-miyazaki/absc/internal/exporter"
	"github.com/y-miyazaki/go-common/pkg/logger"
	"go.uber.org/mock/gomock"
)

var (
	errTestAccountAPI          = errors.New("boom")
	errTestAccountLookupFailed = errors.New("account lookup failed")
	errTestConfigError         = errors.New("config error")
	errTestNoCredentials       = errors.New("no credentials")
)

func testLogger() *logger.SlogLogger {
	return logger.NewSlogLogger(&logger.SlogConfig{Level: slog.LevelError, Format: "text"})
}

// newMockCommand creates a parsed CLI command for testing by simulating CLI arguments.
func newMockCommand(t *testing.T, values map[string]string) *cli.Command {
	t.Helper()

	args := make([]string, 0, len(values))
	for key, value := range values {
		if key == accountNameFlagName || key == includeNonSlotRunsFlagName {
			if value == "true" {
				args = append(args, "--"+key)
			}
			continue
		}
		if value != "" {
			args = append(args, "--"+key+"="+value)
		}
	}

	cmd := &cli.Command{
		Name: "test",
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:    profileFlagName,
				Usage:   "AWS profile to use",
				Sources: cli.EnvVars("AWS_PROFILE", "AWS_DEFAULT_PROFILE"),
			},
			&cli.StringFlag{
				Name:    regionFlagName,
				Aliases: []string{regionShortAlias},
				Usage:   "AWS region(s) to use (comma-separated list accepted)",
				Sources: cli.EnvVars("AWS_DEFAULT_REGION"),
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
			&cli.BoolFlag{Name: accountNameFlagName, Usage: "Resolve account display name via account:GetAccountInformation", Value: false},
			&cli.DurationFlag{Name: timeoutFlagName, Usage: "Overall command timeout", Value: defaultTimeout},
		},
	}

	var parsedCmd *cli.Command
	testCmd := &cli.Command{
		Name:  "test",
		Flags: cmd.Flags,
		Action: func(_ context.Context, c *cli.Command) error {
			parsedCmd = c
			return nil
		},
	}

	if err := testCmd.Run(context.Background(), append([]string{"test"}, args...)); err != nil {
		t.Fatalf("testCmd.Run() error = %v", err)
	}
	if parsedCmd == nil {
		t.Fatal("parsedCmd = nil, want parsed command")
	}
	return parsedCmd
}

func restoreCommandDeps() func() {
	originalCheck := checkAWSCredentials
	originalCollect := collectSchedules
	originalGetAccountName := getAccountName
	originalMkdirAll := mkdirAll
	originalNewAccountClient := newAccountClient
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
		getAccountName = originalGetAccountName
		mkdirAll = originalMkdirAll
		newAccountClient = originalNewAccountClient
		newAWSConfig = originalNewAWSConfig
		nowFunc = originalNowFunc
		buildOutput = originalBuildOutput
		writeErrorsHTML = originalWriteErrorsHTML
		writeSlotIssuesCSV = originalWriteSlotIssuesCSV
		writeHTML = originalWriteHTML
		writeJSON = originalWriteJSON
	}
}

func TestNewApp(t *testing.T) {
	t.Parallel()

	app := newApp(testLogger())
	if got, want := app.Name, "absc"; got != want {
		t.Fatalf("app.Name = %q, want %q", got, want)
	}
	if got, want := app.Version, version; got != want {
		t.Fatalf("app.Version = %q, want %q", got, want)
	}
	if app.Action == nil {
		t.Fatal("app.Action = nil, want non-nil")
	}
}

func TestParseRegions(t *testing.T) {
	t.Parallel()

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
		{name: "empty input", input: " , , ", want: nil},
	}

	for i := range tests {
		tt := tests[i]
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
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
	t.Parallel()

	tests := []struct {
		name   string
		values map[string]string
		want   string
	}{
		{
			name: "prefers deprecated regions flag when set",
			values: map[string]string{
				regionsFlagName: "us-east-1",
				regionFlagName:  "ap-northeast-1",
			},
			want: "us-east-1",
		},
		{
			name:   "uses region flag",
			values: map[string]string{regionFlagName: "eu-west-1"},
			want:   "eu-west-1",
		},
		{name: "falls back to default", values: map[string]string{}, want: defaultRegion},
	}

	for i := range tests {
		tt := tests[i]
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			cmd := newMockCommand(t, tt.values)
			if got := regionArg(cmd); got != tt.want {
				t.Fatalf("regionArg() = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestTimelineWindowStart(t *testing.T) {
	t.Parallel()

	loc := time.FixedZone("JST", 9*3600)
	now := time.Date(2026, 3, 19, 15, 30, 0, 0, loc)

	tests := []struct {
		wantDate time.Time
		name     string
		daysAgo  int
	}{
		{name: "today", daysAgo: 0, wantDate: time.Date(2026, 3, 19, 0, 0, 0, 0, loc)},
		{name: "yesterday", daysAgo: 1, wantDate: time.Date(2026, 3, 18, 0, 0, 0, 0, loc)},
	}

	for i := range tests {
		tt := tests[i]
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got := timelineWindowStart(now, tt.daysAgo, loc)
			if !got.Equal(tt.wantDate) {
				t.Fatalf("timelineWindowStart() = %v, want %v", got, tt.wantDate)
			}
		})
	}
}

func TestAccountIDFromARN(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		arn  string
		want string
	}{
		{name: "valid ARN", arn: "arn:aws:iam::123456789012:role/Admin", want: "123456789012"},
		{name: "empty account in ARN", arn: "arn:aws:iam:::role/Admin", want: "unknown"},
		{name: "invalid ARN format", arn: "invalid", want: "unknown"},
		{name: "empty string", arn: "", want: "unknown"},
	}

	for i := range tests {
		tt := tests[i]
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			if got := accountIDFromARN(tt.arn); got != tt.want {
				t.Fatalf("accountIDFromARN() = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestFetchAccountName(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		setupMock  func(*testing.T, *gomock.Controller) AccountInformationAPI
		want       string
		wantErrMsg string
	}{
		{
			name: "success",
			setupMock: func(t *testing.T, ctrl *gomock.Controller) AccountInformationAPI {
				mockClient := mocks.NewMockAccountInformationAPI(ctrl)
				mockClient.EXPECT().
					GetAccountInformation(gomock.Any(), gomock.Any(), gomock.Any()).
					DoAndReturn(func(_ context.Context, in *account.GetAccountInformationInput, _ ...func(*account.Options)) (*account.GetAccountInformationOutput, error) {
						if got := awssdk.ToString(in.AccountId); got != "123456789012" {
							t.Fatalf("account id = %q, want %q", got, "123456789012")
						}
						return &account.GetAccountInformationOutput{AccountName: awssdk.String("sandbox")}, nil
					})
				return mockClient
			},
			want: "sandbox",
		},
		{
			name: "api error",
			setupMock: func(_ *testing.T, ctrl *gomock.Controller) AccountInformationAPI {
				mockClient := mocks.NewMockAccountInformationAPI(ctrl)
				mockClient.EXPECT().
					GetAccountInformation(gomock.Any(), gomock.Any(), gomock.Any()).
					Return(nil, errTestAccountAPI)
				return mockClient
			},
			wantErrMsg: "get account information",
		},
	}

	for i := range tests {
		tt := tests[i]
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			defer restoreCommandDeps()()

			ctrl := gomock.NewController(t)
			newAccountClient = func(_ *awssdk.Config) AccountInformationAPI {
				return tt.setupMock(t, ctrl)
			}

			got, err := fetchAccountName(context.Background(), &awssdk.Config{}, "123456789012")
			if tt.wantErrMsg != "" {
				if err == nil || !strings.Contains(err.Error(), tt.wantErrMsg) {
					t.Fatalf("fetchAccountName() error = %v, want wrapped error containing %q", err, tt.wantErrMsg)
				}
				return
			}
			if err != nil {
				t.Fatalf("fetchAccountName() error = %v", err)
			}
			if got != tt.want {
				t.Fatalf("fetchAccountName() = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestRunCommand_Validation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		wantErr error
		values  map[string]string
		name    string
	}{
		{
			name:    "max results below minimum",
			values:  map[string]string{maxResultsFlagName: "0"},
			wantErr: errInvalidMaxResults,
		},
		{
			name:    "negative days ago",
			values:  map[string]string{daysAgoFlagName: "-1"},
			wantErr: errInvalidDaysAgo,
		},
	}

	for i := range tests {
		tt := tests[i]
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			cmd := newMockCommand(t, tt.values)
			err := runCommand(context.Background(), cmd, testLogger())
			if !errors.Is(err, tt.wantErr) {
				t.Fatalf("runCommand() error = %v, want %v", err, tt.wantErr)
			}
		})
	}
}

func TestRunCommand_InvalidTimezone(t *testing.T) {
	t.Parallel()

	cmd := newMockCommand(t, map[string]string{timezoneFlagName: "Not/A/Timezone"})
	err := runCommand(context.Background(), cmd, testLogger())
	if err == nil || !strings.Contains(err.Error(), "failed to load timezone") {
		t.Fatalf("runCommand() error = %v, want timezone error", err)
	}
}

func TestRunCommand_AWSConfigError(t *testing.T) {
	defer restoreCommandDeps()()

	newAWSConfig = func(context.Context, string, string) (awssdk.Config, error) {
		return awssdk.Config{}, errTestConfigError
	}

	cmd := newMockCommand(t, map[string]string{})
	err := runCommand(context.Background(), cmd, testLogger())
	if err == nil || !strings.Contains(err.Error(), "failed to initialize aws config") {
		t.Fatalf("runCommand() error = %v, want config error", err)
	}
}

func TestRunCommand_CredentialsError(t *testing.T) {
	defer restoreCommandDeps()()

	newAWSConfig = func(context.Context, string, string) (awssdk.Config, error) {
		return awssdk.Config{Region: defaultRegion}, nil
	}
	checkAWSCredentials = func(context.Context, *awssdk.Config) (string, error) {
		return "", errTestNoCredentials
	}

	cmd := newMockCommand(t, map[string]string{})
	err := runCommand(context.Background(), cmd, testLogger())
	if err == nil || !strings.Contains(err.Error(), "aws credentials check failed") {
		t.Fatalf("runCommand() error = %v, want credentials error", err)
	}
}

func TestRunCommand_Success(t *testing.T) {
	defer restoreCommandDeps()()

	outDir := t.TempDir()
	fixedNow := time.Date(2026, 3, 19, 10, 0, 0, 0, time.UTC)
	nowFunc = func() time.Time { return fixedNow }
	newAWSConfig = func(context.Context, string, string) (awssdk.Config, error) {
		return awssdk.Config{Region: defaultRegion}, nil
	}
	checkAWSCredentials = func(context.Context, *awssdk.Config) (string, error) {
		return "arn:aws:iam::123456789012:root", nil
	}
	collectSchedules = func(context.Context, *awssdk.Config, resources.CollectOptions) ([]resources.Schedule, []resources.ErrorRecord) {
		return nil, nil
	}
	buildOutput = func(accountID string, _, _ time.Time, _ *time.Location, _ []resources.Schedule, _ []resources.ErrorRecord, _ exporter.BuildOutputOptions) exporter.Output {
		return exporter.Output{AccountID: accountID}
	}
	mkdirAll = func(string, os.FileMode) error { return nil }
	writeJSON = func(string, *exporter.Output) error { return nil }
	writeHTML = func(string, *exporter.Output) error { return nil }
	writeErrorsHTML = func(string, *exporter.Output) error { return nil }
	writeSlotIssuesCSV = func(string, *exporter.Output) error { return nil }

	cmd := newMockCommand(t, map[string]string{outputDirFlagName: outDir})
	if err := runCommand(context.Background(), cmd, testLogger()); err != nil {
		t.Fatalf("runCommand() error = %v", err)
	}
}

func TestRunCommand_AccountNameLookupFailureContinues(t *testing.T) {
	defer restoreCommandDeps()()

	outDir := t.TempDir()
	fixedNow := time.Date(2026, 3, 19, 10, 0, 0, 0, time.UTC)
	nowFunc = func() time.Time { return fixedNow }
	newAWSConfig = func(context.Context, string, string) (awssdk.Config, error) {
		return awssdk.Config{Region: defaultRegion}, nil
	}
	checkAWSCredentials = func(context.Context, *awssdk.Config) (string, error) {
		return "arn:aws:iam::123456789012:root", nil
	}
	getAccountName = func(context.Context, *awssdk.Config, string) (string, error) {
		return "", errTestAccountLookupFailed
	}
	collectSchedules = func(context.Context, *awssdk.Config, resources.CollectOptions) ([]resources.Schedule, []resources.ErrorRecord) {
		return nil, nil
	}
	buildOutput = func(accountID string, _, _ time.Time, _ *time.Location, _ []resources.Schedule, _ []resources.ErrorRecord, _ exporter.BuildOutputOptions) exporter.Output {
		return exporter.Output{AccountID: accountID}
	}
	mkdirAll = func(string, os.FileMode) error { return nil }
	writeJSON = func(string, *exporter.Output) error { return nil }
	writeHTML = func(string, *exporter.Output) error { return nil }
	writeErrorsHTML = func(string, *exporter.Output) error { return nil }
	writeSlotIssuesCSV = func(string, *exporter.Output) error { return nil }

	cmd := newMockCommand(t, map[string]string{
		outputDirFlagName:   outDir,
		accountNameFlagName: "true",
	})
	if err := runCommand(context.Background(), cmd, testLogger()); err != nil {
		t.Fatalf("runCommand() error = %v", err)
	}
}
