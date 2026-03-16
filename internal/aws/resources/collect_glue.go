package resources

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/glue"
)

// collectGlueRuns returns Glue job run history for the given job name or ARN.
func collectGlueRuns(ctx context.Context, svc *glue.Client, jobNameOrARN string, since time.Time, maxResults int) ([]Run, error) {
	jobName := jobNameOrARN
	if strings.Contains(jobNameOrARN, ":") {
		jobName = resourceNameFromARN(jobNameOrARN)
	}
	if jobName == "" {
		return []Run{}, nil
	}

	maxRes := int32(maxResults)
	out, err := svc.GetJobRuns(ctx, &glue.GetJobRunsInput{
		JobName:    aws.String(jobName),
		MaxResults: &maxRes,
	})
	if err != nil {
		return nil, fmt.Errorf("get glue job runs for %s: %w", jobName, err)
	}

	runs := make([]Run, 0, len(out.JobRuns))
	for _, r := range out.JobRuns {
		if r.StartedOn == nil {
			continue
		}
		if r.StartedOn.Before(since) {
			continue
		}
		run := Run{
			RunID:         aws.ToString(r.Id),
			Status:        string(r.JobRunState),
			StartAt:       formatRFC3339UTC(*r.StartedOn),
			SourceService: "glue",
		}
		if r.CompletedOn != nil {
			run.EndAt = formatRFC3339UTC(*r.CompletedOn)
			dur := int64(r.CompletedOn.Sub(*r.StartedOn).Seconds())
			run.DurationSec = &dur
		}
		runs = append(runs, run)
	}
	return runs, nil
}
