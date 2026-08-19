//revive:disable:comments-density reason: table-driven tests are self-explanatory via subtest names.
package runs

import "testing"

func TestCollectorServiceNames(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		collector runCollector
		want      string
	}{
		{name: "batch", collector: &batchCollector{}, want: "batch"},
		{name: "ec2", collector: &ec2Collector{}, want: "ec2"},
		{name: "ecs", collector: &ecsCollector{}, want: "ecs"},
		{name: "glue", collector: &glueCollector{}, want: "glue"},
		{name: "lambda", collector: &lambdaCollector{}, want: "lambda"},
		{name: "rds", collector: &rdsCollector{}, want: "rds"},
		{name: "redshift", collector: &redshiftCollector{}, want: "redshift"},
		{name: "step functions", collector: &stepFunctionsCollector{}, want: "stepfunctions"},
	}

	for i := range tests {
		tt := tests[i]
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			if got := tt.collector.Service(); got != tt.want {
				t.Fatalf("Service() = %q, want %q", got, tt.want)
			}
		})
	}
}
