//revive:disable:comments-density reason: table-driven tests are self-explanatory via subtest names.
package resources

import (
	"context"
	"testing"

	awssdk "github.com/aws/aws-sdk-go-v2/aws"
)

func TestCollect(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		opts           CollectOptions
		wantSchedules  int
		wantErrRecords int
		checkNonNil    bool
	}{
		{
			name:           "empty regions",
			opts:           CollectOptions{},
			wantSchedules:  0,
			wantErrRecords: 0,
		},
		{
			name: "default concurrency",
			opts: CollectOptions{
				Regions: []string{"ap-northeast-1"},
			},
			checkNonNil: true,
		},
		{
			name: "multiple regions",
			opts: CollectOptions{
				Regions:        []string{"ap-northeast-1", "us-east-1"},
				MaxConcurrency: 1,
			},
			checkNonNil: true,
		},
	}

	for i := range tests {
		tt := tests[i]
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ctx, cancel := context.WithCancel(context.Background())
			cancel()

			schedules, errs := Collect(ctx, &awssdk.Config{Region: "ap-northeast-1"}, tt.opts)
			if tt.checkNonNil {
				if schedules == nil {
					t.Fatal("Collect() schedules = nil, want non-nil slice")
				}
				if errs == nil {
					t.Fatal("Collect() errs = nil, want non-nil slice")
				}
			} else {
				if len(schedules) != tt.wantSchedules {
					t.Fatalf("len(schedules) = %d, want %d", len(schedules), tt.wantSchedules)
				}
				if len(errs) != tt.wantErrRecords {
					t.Fatalf("len(errs) = %d, want %d", len(errs), tt.wantErrRecords)
				}
			}
		})
	}
}
