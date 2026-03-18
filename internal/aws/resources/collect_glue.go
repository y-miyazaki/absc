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
	// Normalize ARN inputs so the Glue API always receives a job name.
	jobName := jobNameOrARN
	if strings.Contains(jobNameOrARN, ":") {
		jobName = resourceNameFromARN(jobNameOrARN)
	}
	if jobName == "" {
		return make([]Run, 0), nil
	}

	// Limit the API request size to the Glue SDK's int32 field.
	maxRes := safeInt32(maxResults)
	if maxRes == 0 {
		maxRes = safeInt32(defaultMaxResults)
	}

	runs := make([]Run, 0, maxResults)
	var nextToken *string
	for len(runs) < maxResults {
		// Glue returns newest job runs first, so filtering can remain linear.
		out, err := svc.GetJobRuns(ctx, &glue.GetJobRunsInput{
			JobName:    aws.String(jobName),
			MaxResults: &maxRes,
			NextToken:  nextToken,
		})
		if err != nil {
			return nil, fmt.Errorf("get glue job runs for %s: %w", jobName, err)
		}

		// Filter out historical runs outside the caller's lookback window.
		for jobRunIndex := range out.JobRuns {
			// Preserve Glue-specific fields while normalizing timestamps to UTC strings.
			r := out.JobRuns[jobRunIndex]
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
			if len(runs) >= maxResults {
				break
			}
		}

		if out.NextToken == nil {
			break
		}
		nextToken = out.NextToken
	}
	return runs, nil
}
