//revive:disable:comments-density reason: table-driven tests are self-explanatory via subtest names.
package resources

import (
	"context"
	"testing"

	awssdk "github.com/aws/aws-sdk-go-v2/aws"
)

func TestCollect_EmptyRegions(t *testing.T) {
	t.Parallel()

	schedules, errs := Collect(context.Background(), &awssdk.Config{}, CollectOptions{})
	if len(schedules) != 0 {
		t.Fatalf("len(schedules) = %d, want 0", len(schedules))
	}
	if len(errs) != 0 {
		t.Fatalf("len(errs) = %d, want 0", len(errs))
	}
}

func TestCollect_DefaultConcurrency(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	schedules, errs := Collect(ctx, &awssdk.Config{Region: "ap-northeast-1"}, CollectOptions{
		Regions: []string{"ap-northeast-1"},
	})
	if schedules == nil {
		t.Fatal("schedules = nil, want non-nil slice")
	}
	_ = errs
}

func TestCollect_MultipleRegions(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	schedules, errs := Collect(ctx, &awssdk.Config{Region: "ap-northeast-1"}, CollectOptions{
		Regions:        []string{"ap-northeast-1", "us-east-1"},
		MaxConcurrency: 1,
	})
	if schedules == nil {
		t.Fatal("schedules = nil, want non-nil slice")
	}
	_ = errs
}
