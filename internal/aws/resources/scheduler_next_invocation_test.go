//revive:disable:comments-density reason: table-driven tests are self-explanatory via subtest names.
package resources

import (
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/scheduler"
	"github.com/aws/aws-sdk-go-v2/service/scheduler/types"
)

func TestComputeSchedulerNextInvocation(t *testing.T) {
	t.Parallel()

	created := time.Date(2026, 3, 16, 9, 0, 0, 0, time.UTC)

	tests := []struct {
		name   string
		detail *scheduler.GetScheduleOutput
		now    time.Time
		want   string
	}{
		{
			name: "disabled",
			detail: &scheduler.GetScheduleOutput{
				State:              types.ScheduleStateDisabled,
				ScheduleExpression: aws.String("rate(5 minutes)"),
			},
			now:  time.Date(2026, 3, 16, 10, 0, 0, 0, time.UTC),
			want: "",
		},
		{
			name: "at",
			detail: &scheduler.GetScheduleOutput{
				State:                      types.ScheduleStateEnabled,
				ScheduleExpression:         aws.String("at(2026-03-17T12:30:00)"),
				ScheduleExpressionTimezone: aws.String("UTC"),
			},
			now:  time.Date(2026, 3, 16, 10, 0, 0, 0, time.UTC),
			want: "2026-03-17T12:30:00Z",
		},
		{
			name: "cron",
			detail: &scheduler.GetScheduleOutput{
				State:                      types.ScheduleStateEnabled,
				ScheduleExpression:         aws.String("cron(0 * * * ? *)"),
				ScheduleExpressionTimezone: aws.String("UTC"),
			},
			now:  time.Date(2026, 3, 16, 10, 15, 0, 0, time.UTC),
			want: "2026-03-16T11:00:00Z",
		},
		{
			name: "rate",
			detail: &scheduler.GetScheduleOutput{
				State:              types.ScheduleStateEnabled,
				ScheduleExpression: aws.String("rate(15 minutes)"),
				CreationDate:       &created,
			},
			now:  time.Date(2026, 3, 16, 10, 7, 0, 0, time.UTC),
			want: "2026-03-16T10:15:00Z",
		},
		{
			name: "cron wraparound minute range",
			detail: &scheduler.GetScheduleOutput{
				State:                      types.ScheduleStateEnabled,
				ScheduleExpression:         aws.String("cron(50-10/10 * * * ? *)"),
				ScheduleExpressionTimezone: aws.String("UTC"),
			},
			now:  time.Date(2026, 3, 16, 10, 15, 0, 0, time.UTC),
			want: "2026-03-16T10:50:00Z",
		},
		{
			name: "cron wraparound weekday range",
			detail: &scheduler.GetScheduleOutput{
				State:                      types.ScheduleStateEnabled,
				ScheduleExpression:         aws.String("cron(0 9 ? * FRI-MON *)"),
				ScheduleExpressionTimezone: aws.String("UTC"),
			},
			now:  time.Date(2026, 3, 19, 10, 15, 0, 0, time.UTC),
			want: "2026-03-20T09:00:00Z",
		},
	}

	for i := range tests {
		tt := tests[i]
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got := computeSchedulerNextInvocation(tt.detail, tt.now)
			if got != tt.want {
				t.Fatalf("computeSchedulerNextInvocation() = %q, want %q", got, tt.want)
			}
		})
	}
}
