// Package core contains shared value types used across resources subpackages.
package core

import "time"

// SlotsPerDay is the fixed number of 10-minute slots in one rendered day.
const SlotsPerDay = 144

// CollectOptions controls the shared schedule collection window and limits.
type CollectOptions struct {
	ReferenceTime  time.Time
	Since          time.Time
	Until          time.Time
	Regions        []string
	MaxConcurrency int
	MaxResults     int
}

// ErrorRecord is a soft error emitted while collecting schedules or runs.
type ErrorRecord struct {
	Service string
	Region  string
	Message string
}

// Run represents one collected execution record.
type Run struct {
	RunID         string
	Status        string
	StartAt       string
	EndAt         string
	DurationSec   *int64
	SourceService string
}

// Schedule represents one collected schedule and its associated runs.
type Schedule struct {
	Description                string
	Region                     string
	TargetID                   string
	TargetName                 string
	ScheduleGroupName          string
	ScheduleName               string
	ScheduleExpression         string
	ScheduleExpressionTimezone string
	NextInvocationAt           string
	Service                    string
	TargetKind                 string
	TargetAction               string
	ID                         string
	TargetService              string
	TargetARN                  string
	TriggerType                string // "cron" or "event"
	TriggerLabel               string // cron expression or event rule name
	Slots                      []int
	Runs                       []Run
	Enabled                    bool
	RunsCapped                 bool
}
