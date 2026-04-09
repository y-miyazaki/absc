// Package core contains shared value types used across resources subpackages.
package core

import "time"

const SlotsPerDay = 144

type CollectOptions struct {
	ReferenceTime  time.Time
	Since          time.Time
	Until          time.Time
	Regions        []string
	MaxConcurrency int
	MaxResults     int
}

type ErrorRecord struct {
	Service string
	Region  string
	Message string
}

type Run struct {
	RunID         string
	Status        string
	StartAt       string
	EndAt         string
	DurationSec   *int64
	SourceService string
}

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
