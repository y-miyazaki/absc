package helpers

import (
	"reflect"
	"testing"
	"time"
)

func TestBuildDailySlots(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		expr       string
		slotMinute int
		wantOnes   []int
		wantCount  int
	}{
		{name: "cron expression", expr: "cron(0 1 * * ? *)", slotMinute: 10, wantOnes: []int{6}, wantCount: 1},
		{name: "rate minutes", expr: "rate(30 minutes)", slotMinute: 10, wantOnes: []int{0, 3, 6, 9}, wantCount: 48},
		{name: "rate hours", expr: "rate(2 hours)", slotMinute: 10, wantOnes: []int{0, 12, 24}, wantCount: 12},
		{name: "rate default unit", expr: "rate(1 day)", slotMinute: 10, wantOnes: []int{0}, wantCount: 1},
		{name: "invalid expression", expr: "hello", slotMinute: 10, wantOnes: nil, wantCount: 0},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			slots := BuildDailySlots(tt.expr, tt.slotMinute)
			count := 0
			for _, v := range slots {
				if v == 1 {
					count++
				}
			}
			if count != tt.wantCount {
				t.Fatalf("BuildDailySlots(%q) ones count = %d, want %d", tt.expr, count, tt.wantCount)
			}
			for _, idx := range tt.wantOnes {
				if slots[idx] != 1 {
					t.Fatalf("BuildDailySlots(%q) slots[%d] = %d, want 1", tt.expr, idx, slots[idx])
				}
			}
		})
	}
}

func TestMatchAWSCronExpression(t *testing.T) {
	t.Parallel()

	monday := time.Date(2026, time.April, 20, 9, 0, 0, 0, time.UTC)
	tuesday := monday.Add(24 * time.Hour)

	tests := []struct {
		name      string
		fields    []string
		candidate time.Time
		want      bool
	}{
		{name: "invalid field count", fields: []string{"0"}, candidate: monday, want: false},
		{name: "day of week only", fields: []string{"0", "9", "?", "*", "MON", "2026"}, candidate: monday, want: true},
		{name: "day of week mismatch", fields: []string{"0", "9", "?", "*", "MON", "2026"}, candidate: tuesday, want: false},
		{name: "day of month only", fields: []string{"0", "9", "20", "*", "?", "2026"}, candidate: monday, want: true},
		{name: "dom and dow both required", fields: []string{"0", "9", "20", "*", "MON", "2026"}, candidate: monday, want: true},
		{name: "dom and dow mismatch", fields: []string{"0", "9", "20", "*", "MON", "2026"}, candidate: tuesday, want: false},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got := MatchAWSCronExpression(tt.fields, tt.candidate)
			if got != tt.want {
				t.Fatalf("MatchAWSCronExpression(%v, %v) = %v, want %v", tt.fields, tt.candidate, got, tt.want)
			}
		})
	}
}

func TestMatchCronField(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		field    string
		value    int
		minValue int
		maxValue int
		aliases  map[string]int
		want     bool
	}{
		{name: "empty false", field: "", value: 1, minValue: 0, maxValue: 59, want: false},
		{name: "wildcard true", field: "*", value: 10, minValue: 0, maxValue: 59, want: true},
		{name: "question true", field: "?", value: 10, minValue: 0, maxValue: 59, want: true},
		{name: "list match", field: "1,5,10", value: 5, minValue: 0, maxValue: 59, want: true},
		{name: "alias match", field: "MON", value: 2, minValue: 1, maxValue: 7, aliases: awsCronDayAliases, want: true},
		{name: "no match", field: "1,2,3", value: 4, minValue: 0, maxValue: 59, want: false},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got := MatchCronField(tt.field, tt.value, tt.minValue, tt.maxValue, tt.aliases)
			if got != tt.want {
				t.Fatalf("MatchCronField(%q, %d) = %v, want %v", tt.field, tt.value, got, tt.want)
			}
		})
	}
}

func TestMatchCronPart(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		part     string
		value    int
		minValue int
		maxValue int
		want     bool
	}{
		{name: "step in range", part: "10-20/5", value: 15, minValue: 0, maxValue: 59, want: true},
		{name: "step wrap around", part: "50-10/10", value: 0, minValue: 0, maxValue: 59, want: true},
		{name: "simple range", part: "10-12", value: 11, minValue: 0, maxValue: 59, want: true},
		{name: "simple wrap range", part: "22-2", value: 1, minValue: 0, maxValue: 23, want: true},
		{name: "single value", part: "7", value: 7, minValue: 0, maxValue: 59, want: true},
		{name: "invalid step", part: "*/x", value: 0, minValue: 0, maxValue: 59, want: false},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got := MatchCronPart(tt.part, tt.value, tt.minValue, tt.maxValue, nil)
			if got != tt.want {
				t.Fatalf("MatchCronPart(%q, %d) = %v, want %v", tt.part, tt.value, got, tt.want)
			}
		})
	}
}

func TestParseCronAtom(t *testing.T) {
	t.Parallel()

	if _, ok := ParseCronAtom(" ", nil); ok {
		t.Fatalf("ParseCronAtom(empty) expected ok=false")
	}

	if got, ok := ParseCronAtom("MON", awsCronDayAliases); !ok || got != 2 {
		t.Fatalf("ParseCronAtom(MON) = (%d, %v), want (2, true)", got, ok)
	}

	if _, ok := ParseCronAtom("x", nil); ok {
		t.Fatalf("ParseCronAtom(x) expected ok=false")
	}
}

func TestParseCronField(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		field    string
		minValue int
		maxValue int
		want     []int
	}{
		{name: "wildcard", field: "*", minValue: 1, maxValue: 3, want: []int{1, 2, 3}},
		{name: "list", field: "1,3,2", minValue: 1, maxValue: 3, want: []int{1, 2, 3}},
		{name: "range", field: "10-12", minValue: 0, maxValue: 59, want: []int{10, 11, 12}},
		{name: "wrap range", field: "22-2", minValue: 0, maxValue: 23, want: []int{0, 1, 2, 22, 23}},
		{name: "step", field: "0-10/5", minValue: 0, maxValue: 59, want: []int{0, 5, 10}},
		{name: "wrap step", field: "22-2/2", minValue: 0, maxValue: 23, want: []int{0, 2, 22}},
		{name: "invalid fallback", field: "a", minValue: 1, maxValue: 3, want: []int{1, 2, 3}},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got := ParseCronField(tt.field, tt.minValue, tt.maxValue)
			if !reflect.DeepEqual(got, tt.want) {
				t.Fatalf("ParseCronField(%q) = %v, want %v", tt.field, got, tt.want)
			}
		})
	}
}
