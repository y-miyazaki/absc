package helpers

import "testing"

func TestResourceNameFromARN(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		arn  string
		want string
	}{
		{name: "empty", arn: "", want: ""},
		{name: "slash form", arn: "arn:aws:lambda:ap-northeast-1:123:function:my-func/qualifier", want: "qualifier"},
		{name: "colon form", arn: "arn:aws:events:ap-northeast-1:123:rule/sample", want: "sample"},
		{name: "plain text", arn: "resource-name", want: "resource-name"},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got := ResourceNameFromARN(tt.arn)
			if got != tt.want {
				t.Fatalf("ResourceNameFromARN(%q) = %q, want %q", tt.arn, got, tt.want)
			}
		})
	}
}
