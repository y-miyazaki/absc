package resources

type Schedule struct {
	Region                     string
	TargetName                 string
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
	Slots                      []int
	Runs                       []Run
	Enabled                    bool
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
