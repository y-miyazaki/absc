package resources

type Schedule struct {
	ID                         string
	Service                    string
	ScheduleName               string
	ScheduleExpression         string
	ScheduleExpressionTimezone string
	Enabled                    bool
	Region                     string
	TargetARN                  string
	TargetKind                 string
	TargetService              string
	TargetName                 string
	NextInvocationAt           string
	Slots                      []int
	Runs                       []Run
	RunsCapped                 bool
}

type Run struct {
	RunID         string
	Status        string
	StartAt       string
	EndAt         string
	DurationSec   *int64
	SourceService string
}

type ErrorRecord struct {
	Service string
	Region  string
	Message string
}

const slotsPerDay = 144
