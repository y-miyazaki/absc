//revive:disable:comments-density reason: table-driven tests are self-explanatory via subtest names.
package resources

import (
	"reflect"
	"testing"
)

func TestSchedulerCollector_Name(t *testing.T) {
	t.Parallel()

	collector := &SchedulerCollector{}
	if got, want := collector.Name(), "eventbridge_scheduler"; got != want {
		t.Fatalf("Name() = %q, want %q", got, want)
	}
}

func TestExtractAccountIDFromRoleARN(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		roleARN string
		want    string
	}{
		{name: "valid role arn", roleARN: "arn:aws:iam::123456789012:role/sample", want: "123456789012"},
		{name: "empty", roleARN: "", want: ""},
		{name: "invalid", roleARN: "arn:aws:iam", want: ""},
	}

	for i := range tests {
		tt := tests[i]
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			if got := extractAccountIDFromRoleARN(tt.roleARN); got != tt.want {
				t.Fatalf("extractAccountIDFromRoleARN() = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestGetStringFromJSON(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		raw  string
		keys []string
		want string
	}{
		{name: "empty raw", raw: "  ", keys: []string{"key"}, want: ""},
		{name: "invalid json", raw: "{", keys: []string{"key"}, want: ""},
		{name: "first matching key", raw: `{"JobName":"sample","Other":"x"}`, keys: []string{"Missing", "JobName"}, want: "sample"},
		{name: "non-string value", raw: `{"count":1}`, keys: []string{"count"}, want: ""},
	}

	for i := range tests {
		tt := tests[i]
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			if got := getStringFromJSON(tt.raw, tt.keys...); got != tt.want {
				t.Fatalf("getStringFromJSON() = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestGetStringSliceFromJSON(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		raw  string
		keys []string
		want []string
	}{
		{name: "empty raw", raw: "", keys: []string{"InstanceIds"}, want: nil},
		{name: "invalid json", raw: "{", keys: []string{"InstanceIds"}, want: nil},
		{name: "string slice", raw: `{"InstanceIds":["i-aaa","i-bbb"]}`, keys: []string{"InstanceIds"}, want: []string{"i-aaa", "i-bbb"}},
		{name: "skips empty strings", raw: `{"InstanceIds":["i-aaa","  "]}`, keys: []string{"InstanceIds"}, want: []string{"i-aaa"}},
		{name: "non-array value", raw: `{"InstanceIds":"i-aaa"}`, keys: []string{"InstanceIds"}, want: nil},
	}

	for i := range tests {
		tt := tests[i]
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got := getStringSliceFromJSON(tt.raw, tt.keys...)
			if !reflect.DeepEqual(got, tt.want) {
				t.Fatalf("getStringSliceFromJSON() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestResolveSchedulerRunTarget_BatchFallback(t *testing.T) {
	t.Parallel()

	got := resolveSchedulerRunTarget(
		"arn:aws:batch:ap-northeast-1:123456789012:job-queue/sample",
		"",
	)
	if !got.hasBatchParameters {
		t.Fatal("hasBatchParameters = false, want true")
	}
}
