package resources

import "testing"

func TestBuildSlots_CronWraparoundHourRange(t *testing.T) {
	t.Parallel()

	slots := buildSlots("cron(0 21-2 * * ? *)")

	for _, idx := range []int{0, 6, 12, 126, 132, 138} {
		if slots[idx] != 1 {
			t.Fatalf("slots[%d] = %d, want 1", idx, slots[idx])
		}
	}
	if slots[18] != 0 {
		t.Fatalf("slots[18] = %d, want 0", slots[18])
	}
}

func TestBuildSlots_CronWraparoundMinuteStepRange(t *testing.T) {
	t.Parallel()

	slots := buildSlots("cron(50-10/10 0 * * ? *)")

	for _, idx := range []int{0, 1, 5} {
		if slots[idx] != 1 {
			t.Fatalf("slots[%d] = %d, want 1", idx, slots[idx])
		}
	}
	if slots[2] != 0 {
		t.Fatalf("slots[2] = %d, want 0", slots[2])
	}
}
