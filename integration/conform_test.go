package integration

import (
	"encoding/json"
	"os/exec"
	"testing"
)

// conformReport is the part of dotfs conform -json the tests look at.
type conformReport struct {
	OK         bool            `json:"ok"`
	Extensions map[string]bool `json:"extensions"`
	Summary    map[string]struct {
		Pass, Fail, Warn, Skip int
	} `json:"summary"`
	Profiles []struct {
		Name string `json:"name"`
		Met  bool   `json:"met"`
	} `json:"profiles"`
	Results []struct {
		ID       string   `json:"id"`
		Status   string   `json:"status"`
		Messages []string `json:"messages"`
	} `json:"results"`
}

// conform runs dotfs conform against srv's TCP listener on root aname.
func conform(t *testing.T, srv *Server, aname string, extra ...string) conformReport {
	t.Helper()
	args := append([]string{"conform", "-json", "-aname", aname}, extra...)
	args = append(args, srv.TCP)
	out, err := exec.Command(bin, args...).Output()
	var rep conformReport
	if jerr := json.Unmarshal(out, &rep); jerr != nil {
		t.Fatalf("dotfs conform: %v, %v\n%s", err, jerr, out)
	}
	return rep
}

func checkConform(t *testing.T, rep conformReport) {
	t.Helper()
	for _, ext := range []string{"index", "events", "path-operations", "staged-commit"} {
		if !rep.Extensions[ext] {
			t.Errorf("extension %s not offered", ext)
		}
	}
	for name, s := range rep.Summary {
		if s.Fail > 0 {
			t.Errorf("%s: %d failed", name, s.Fail)
		}
	}
	for _, r := range rep.Results {
		if r.Status == "fail" {
			t.Errorf("FAIL %s: %v", r.ID, r.Messages)
		}
	}
	for _, p := range rep.Profiles {
		if !p.Met {
			t.Errorf("profile %s not met", p.Name)
		}
	}
	if !rep.OK {
		t.Errorf("conform reported not ok")
	}
}

func TestConformAgainstServe(t *testing.T) {
	srv := NewServer(t)
	checkConform(t, conform(t, srv, "conform", "-other-user", "other"))
}
