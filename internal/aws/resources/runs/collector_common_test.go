//revive:disable:comments-density reason: table-driven tests are self-explanatory via subtest names.
package runs

import (
	"context"
	"errors"
	"testing"

	resourcescore "github.com/y-miyazaki/absc/internal/aws/resources/core"
)

const (
	// Shared time component constants for test fixtures.
	testYear   = 2026
	testMonth  = 3
	testDay    = 18
	testDay19  = 19
	testDay24  = 24
	testHour   = 17
	testHour1  = 1
	testHour2  = 2
	testMinute = 0
	testSecond = 49

	// Pagination fixture limits.
	testPageLimit1   = 1
	testPageLimit10  = 10
	testPageLimit15  = 15
	testPageLimit25  = 25
	testPageLimit50  = 50
	testPageLimit99  = 99
	testPageLimit100 = 100
	testPageLimit200 = 200
	testPageLimit300 = 300
	testPageLimit400 = 400

	// Common AWS test fixture strings.
	testRegionUSEast1 = "us-east-1"
	testSampleJob     = "sample-job"
	testMyFunction    = "my-function"

	// Fatalf format strings reused across table-driven tests.
	testFmtLenRuns = "len(runs) = %d, want %d"
	testFmtStatus  = "status = %q, want %q"
)

var (
	errTestUpstreamFailed = errors.New("upstream failed")
	errTestShouldNotRun   = errors.New("should not run")
)

type stubRunCollector struct {
	service string
}

//nolint:gocritic // stub must match runCollector interface signature.
func (stubRunCollector) Collect(ctx context.Context, schedule *resourcescore.Schedule, targetARN, runJobName string, hints TargetHints, opts resourcescore.CollectOptions) ([]resourcescore.Run, error) {
	_ = ctx
	_ = schedule
	_ = targetARN
	_ = runJobName
	_ = hints
	_ = opts
	return nil, nil
}

func (s stubRunCollector) Service() string {
	return s.service
}

func TestGetCachedRuns(t *testing.T) {
	t.Parallel()

	t.Run("returns cached success without recalling collectFn", func(t *testing.T) {
		t.Parallel()

		runsCache := make(map[string][]resourcescore.Run)
		errCache := make(map[string]error)
		calls := 0
		collectFn := func() ([]resourcescore.Run, error) {
			calls++
			return []resourcescore.Run{{RunID: "run-1"}}, nil
		}

		first, err := getCachedRuns(runsCache, errCache, "k1", "job sample", collectFn)
		if err != nil {
			t.Fatalf("first getCachedRuns() error = %v", err)
		}
		second, err := getCachedRuns(runsCache, errCache, "k1", "job sample", collectFn)
		if err != nil {
			t.Fatalf("second getCachedRuns() error = %v", err)
		}
		if calls != 1 {
			t.Fatalf("collectFn calls = %d, want 1", calls)
		}
		if got, want := first[0].RunID, "run-1"; got != want {
			t.Fatalf("first RunID = %q, want %q", got, want)
		}
		if got, want := second[0].RunID, "run-1"; got != want {
			t.Fatalf("second RunID = %q, want %q", got, want)
		}
	})

	t.Run("caches original error and wraps only the miss", func(t *testing.T) {
		t.Parallel()

		runsCache := make(map[string][]resourcescore.Run)
		errCache := make(map[string]error)
		calls := 0
		collectFn := func() ([]resourcescore.Run, error) {
			calls++
			return nil, errTestUpstreamFailed
		}

		_, firstErr := getCachedRuns(runsCache, errCache, "k-err", "job sample", collectFn)
		if firstErr == nil {
			t.Fatal("first getCachedRuns() error = nil, want wrapped error")
		}
		if !errors.Is(firstErr, errTestUpstreamFailed) {
			t.Fatalf("first error %v does not wrap original", firstErr)
		}
		if firstErr.Error() == errTestUpstreamFailed.Error() {
			t.Fatal("first error was not wrapped with description")
		}

		_, secondErr := getCachedRuns(runsCache, errCache, "k-err", "job sample", collectFn)
		if !errors.Is(secondErr, errTestUpstreamFailed) {
			t.Fatalf("second error = %v, want original cached error", secondErr)
		}
		if calls != 1 {
			t.Fatalf("collectFn calls = %d, want 1", calls)
		}
	})
}

func TestGetCachedRunsForCollector(t *testing.T) {
	t.Parallel()

	caches := newRunCollectorCaches()
	collector := stubRunCollector{service: "glue"}
	want := []resourcescore.Run{{RunID: "cached-run"}}
	calls := 0

	got, err := getCachedRunsForCollector(caches, collector, "target-a", "Glue job=sample", func() ([]resourcescore.Run, error) {
		calls++
		return want, nil
	})
	if err != nil {
		t.Fatalf("getCachedRunsForCollector() error = %v", err)
	}
	if calls != 1 {
		t.Fatalf("collectFn calls = %d, want 1", calls)
	}
	if got, wantID := got[0].RunID, "cached-run"; got != wantID {
		t.Fatalf("RunID = %q, want %q", got, wantID)
	}

	again, err := getCachedRunsForCollector(caches, collector, "target-a", "Glue job=sample", func() ([]resourcescore.Run, error) {
		calls++
		return nil, errTestShouldNotRun
	})
	if err != nil {
		t.Fatalf("cached getCachedRunsForCollector() error = %v", err)
	}
	if calls != 1 {
		t.Fatalf("collectFn calls after cache hit = %d, want 1", calls)
	}
	if got, wantID := again[0].RunID, "cached-run"; got != wantID {
		t.Fatalf("cached RunID = %q, want %q", got, wantID)
	}
}

func TestEnsureServiceRunCaches(t *testing.T) {
	t.Parallel()

	caches := newRunCollectorCaches()
	firstRuns, firstErrs := ensureServiceRunCaches(caches, "batch")
	secondRuns, _ := ensureServiceRunCaches(caches, "batch")
	otherRuns, _ := ensureServiceRunCaches(caches, "glue")

	if firstRuns == nil || firstErrs == nil {
		t.Fatal("ensureServiceRunCaches() returned nil maps")
	}
	firstRuns["k"] = []resourcescore.Run{{RunID: "x"}}
	if _, ok := secondRuns["k"]; !ok {
		t.Fatal("same service did not reuse run cache map")
	}
	if _, ok := otherRuns["k"]; ok {
		t.Fatal("different service reused the same run cache map")
	}
}
