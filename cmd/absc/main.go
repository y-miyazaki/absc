package main

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sts"
	"github.com/urfave/cli/v2"

	awscfg "github.com/y-miyazaki/arc/internal/aws"
	"github.com/y-miyazaki/arc/internal/aws/resources"
	"github.com/y-miyazaki/arc/internal/exporter"
	"github.com/y-miyazaki/arc/internal/logger"
)

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

func regionArg(c *cli.Context) string {
	if c.IsSet("regions") {
		if v := strings.TrimSpace(c.String("regions")); v != "" {
			return v
		}
	}
	if c.IsSet("region") {
		if v := strings.TrimSpace(c.String("region")); v != "" {
			return v
		}
	}
	if v := strings.TrimSpace(c.String("region")); v != "" {
		return v
	}
	return "ap-northeast-1"
}

func main() {
	l := logger.NewDefault()

	app := &cli.App{
		Name:  "absc",
		Usage: "Collect cron schedules from AWS and render timeline HTML",
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:    "profile",
				Usage:   "AWS profile to use",
				EnvVars: []string{"AWS_PROFILE", "AWS_DEFAULT_PROFILE"},
			},
			&cli.StringFlag{
				Name:    "region",
				Aliases: []string{"r"},
				Usage:   "AWS region(s) to use (comma-separated list accepted)",
				EnvVars: []string{"AWS_DEFAULT_REGION"},
				Value:   "ap-northeast-1",
			},
			&cli.StringFlag{
				Name:   "regions",
				Usage:  "Deprecated alias of --region (comma-separated list accepted)",
				Hidden: true,
			},
			&cli.StringFlag{Name: "timezone", Usage: "IANA timezone", Value: "UTC"},
			&cli.StringFlag{Name: "output-dir", Aliases: []string{"D"}, Usage: "Output base directory", Value: "./output"},
			&cli.IntFlag{Name: "lookback-hours", Usage: "Execution history lookback hours", Value: 24},
			&cli.IntFlag{Name: "max-concurrency", Usage: "Max concurrent resource collectors", Value: 5},
			&cli.IntFlag{Name: "max-results", Usage: "Max executions/jobs per target", Value: 50},
		},
		Action: func(c *cli.Context) error {
			if c.Int("max-results") < 1 {
				return fmt.Errorf("max-results must be >= 1")
			}

			loc, err := time.LoadLocation(c.String("timezone"))
			if err != nil {
				return fmt.Errorf("failed to load timezone: %w", err)
			}

			regions := parseRegions(regionArg(c))
			if len(regions) == 0 {
				regions = []string{"ap-northeast-1"}
			}
			cfg, err := awscfg.NewConfig(context.Background(), regions[0], c.String("profile"))
			if err != nil {
				return fmt.Errorf("failed to initialize aws config: %w", err)
			}

			identity, err := sts.NewFromConfig(cfg).GetCallerIdentity(context.Background(), &sts.GetCallerIdentityInput{})
			if err != nil {
				return fmt.Errorf("failed to get caller identity: %w", err)
			}
			accountID := aws.ToString(identity.Account)
			if accountID == "" {
				accountID = "unknown"
			}

			now := time.Now().In(loc)
			since := now.Add(-time.Duration(c.Int("lookback-hours")) * time.Hour)
			schedules, errs := resources.Collect(context.Background(), cfg, resources.CollectOptions{
				MaxConcurrency: c.Int("max-concurrency"),
				MaxResults:     c.Int("max-results"),
				Regions:        regions,
				Since:          since,
			})

			result := exporter.BuildOutput(accountID, now, loc, schedules, errs)

			outDir := filepath.Join(c.String("output-dir"), accountID, "cron")
			if mkErr := os.MkdirAll(outDir, 0o750); mkErr != nil {
				return fmt.Errorf("failed to create output directory: %w", mkErr)
			}

			jsonPath := filepath.Join(outDir, "schedules.json")
			if wErr := exporter.WriteJSON(jsonPath, result); wErr != nil {
				return fmt.Errorf("failed to write json: %w", wErr)
			}

			htmlPath := filepath.Join(outDir, "index.html")
			if hErr := exporter.WriteHTML(htmlPath, result); hErr != nil {
				return fmt.Errorf("failed to write html: %w", hErr)
			}

			l.Info("generated outputs", "json_path", jsonPath, "html_path", htmlPath)
			return nil
		},
	}

	if err := app.Run(os.Args); err != nil {
		l.Error("failed to run app", "error", err)
		os.Exit(1)
	}
}
