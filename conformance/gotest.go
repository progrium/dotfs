package conformance

import (
	"context"
	"strings"
	"testing"
)

// RunTest runs the suite as Go subtests, one per conformance test, so an
// implementation can check itself with go test. Warnings are logged; only
// failures fail.
func RunTest(t *testing.T, target Target, opts Options) *Report {
	t.Helper()
	rep, err := Run(context.Background(), target, opts)
	if err != nil {
		t.Fatalf("conformance: %v", err)
	}
	for _, res := range rep.Results {
		t.Run(res.ID, func(t *testing.T) {
			msg := strings.Join(res.Messages, "\n")
			switch res.Status {
			case Fail:
				t.Errorf("%s\nspec: %s\n%s", res.Name, res.Section, msg)
			case Skip:
				t.Skip(msg)
			default:
				if msg != "" {
					t.Log(msg)
				}
			}
		})
	}
	return rep
}
