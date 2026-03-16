package resources

import (
"testing"
"time"

"github.com/aws/aws-sdk-go-v2/aws"
"github.com/aws/aws-sdk-go-v2/service/scheduler"
"github.com/aws/aws-sdk-go-v2/service/scheduler/types"
)

func TestComputeSchedulerNextInvocation_Disabled(t *testing.T) {
detail := &scheduler.GetScheduleOutput{
State:              types.ScheduleStateDisabled,
ScheduleExpression: aws.String("rate(5 minutes)"),
}

got := computeSchedulerNextInvocation(detail, time.Date(2026, 3, 16, 10, 0, 0, 0, time.UTC))
if got != "" {
t.Fatalf("expected empty next invocation for disabled schedule, got %q", got)
}
}

func TestComputeSchedulerNextInvocation_At(t *testing.T) {
detail := &scheduler.GetScheduleOutput{
State:                      types.ScheduleStateEnabled,
ScheduleExpression:         aws.String("at(2026-03-17T12:30:00)"),
ScheduleExpressionTimezone: aws.String("UTC"),
}

now := time.Date(2026, 3, 16, 10, 0, 0, 0, time.UTC)
got := computeSchedulerNextInvocation(detail, now)
want := "2026-03-17T12:30:00Z"
if got != want {
t.Fatalf("unexpected next invocation: want %q, got %q", want, got)
}
}

func TestComputeSchedulerNextInvocation_Cron(t *testing.T) {
detail := &scheduler.GetScheduleOutput{
State:                      types.ScheduleStateEnabled,
ScheduleExpression:         aws.String("cron(0 * * * ? *)"),
ScheduleExpressionTimezone: aws.String("UTC"),
}

now := time.Date(2026, 3, 16, 10, 15, 0, 0, time.UTC)
got := computeSchedulerNextInvocation(detail, now)
want := "2026-03-16T11:00:00Z"
if got != want {
t.Fatalf("unexpected next invocation: want %q, got %q", want, got)
}
}

func TestComputeSchedulerNextInvocation_Rate(t *testing.T) {
created := time.Date(2026, 3, 16, 9, 0, 0, 0, time.UTC)
detail := &scheduler.GetScheduleOutput{
State:              types.ScheduleStateEnabled,
ScheduleExpression: aws.String("rate(15 minutes)"),
CreationDate:       &created,
}

now := time.Date(2026, 3, 16, 10, 7, 0, 0, time.UTC)
got := computeSchedulerNextInvocation(detail, now)
want := "2026-03-16T10:15:00Z"
if got != want {
t.Fatalf("unexpected next invocation: want %q, got %q", want, got)
}
}
