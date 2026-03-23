// Package runs resolves execution history for schedule targets.
//
//revive:disable:comments-density reason: pagination helpers are intentionally compact and self-descriptive.
package runs

import "github.com/y-miyazaki/absc/internal/helpers"

const (
	batchListJobsPageSizeMax int32 = 1000
	// Keep CloudWatch page size conservative to reduce transient memory spikes per page.
	cloudWatchLogsFilterEventsPageSizeMax  int32 = 1000
	ecsListTasksPageSizeMax                int32 = 100
	glueGetJobRunsPageSizeMax              int32 = 200
	stepFunctionsListExecutionsPageSizeMax int32 = 1000
)

func pageSizeForLimit(limit int, serviceMax int32) int32 {
	if serviceMax < 1 {
		return 1
	}
	if limit < 1 {
		return serviceMax
	}
	pageSize := helpers.SafeInt32(limit)
	if pageSize < 1 {
		return 1
	}
	if pageSize > serviceMax {
		return serviceMax
	}
	return pageSize
}

func remainingPageSize(limit, collected int, serviceMax int32) int32 {
	if serviceMax < 1 {
		return 1
	}
	if limit < 1 {
		return serviceMax
	}
	remaining := limit - collected
	if remaining < 1 {
		return 1
	}
	return pageSizeForLimit(remaining, serviceMax)
}
