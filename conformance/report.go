package conformance

import (
	"encoding/json"
	"fmt"
	"io"
	"strings"
	"time"
)

// Counts tallies results by status.
type Counts struct {
	Pass int `json:"pass"`
	Fail int `json:"fail"`
	Warn int `json:"warn"`
	Skip int `json:"skip"`
}

func (c *Counts) add(s Status) {
	switch s {
	case Pass:
		c.Pass++
	case Fail:
		c.Fail++
	case Warn:
		c.Warn++
	case Skip:
		c.Skip++
	}
}

// Profile is a named set of extensions from the spec's Profiles section.
type Profile struct {
	Name       string `json:"name"`
	Extensions []Ext  `json:"extensions"`
	// Optional extensions are checked when present.
	Optional []Ext `json:"optional,omitempty"`
}

// Profiles are the spec's three profiles.
var Profiles = []Profile{
	{Name: "read-only cache", Extensions: []Ext{ExtIndex}},
	{Name: "watchable", Extensions: []Ext{ExtEvents, ExtCtl}, Optional: []Ext{ExtIndex}},
	{Name: "sync", Extensions: []Ext{ExtIndex, ExtEvents, ExtCtl, ExtStage}},
}

// ProfileResult says whether the server meets a profile.
type ProfileResult struct {
	Name string `json:"name"`
	Met  bool   `json:"met"`
	// Why explains a profile that is not met.
	Why string `json:"why,omitempty"`
}

// Report is the outcome of a run.
type Report struct {
	Addr       string          `json:"addr"`
	Aname      string          `json:"aname"`
	Started    time.Time       `json:"started"`
	Duration   time.Duration   `json:"duration_ns"`
	Extensions map[Ext]bool    `json:"extensions"`
	Summary    map[Ext]*Counts `json:"summary"`
	Total      Counts          `json:"total"`
	Profiles   []ProfileResult `json:"profiles"`
	OK         bool            `json:"ok"`
	Results    []Result        `json:"results"`
}

func (r *Report) finish() {
	r.Summary = map[Ext]*Counts{}
	for _, e := range Exts {
		r.Summary[e] = &Counts{}
	}
	for _, res := range r.Results {
		r.Summary[res.Ext].add(res.Status)
		r.Total.add(res.Status)
	}
	r.OK = r.Total.Fail == 0
	for _, p := range Profiles {
		pr := ProfileResult{Name: p.Name, Met: true}
		var why []string
		for _, e := range p.Extensions {
			if !r.Extensions[e] {
				pr.Met = false
				why = append(why, "no "+e.Title())
			} else if c := r.Summary[e]; c.Fail > 0 {
				pr.Met = false
				why = append(why, fmt.Sprintf("%d %s failures", c.Fail, e.Title()))
			}
		}
		for _, e := range p.Optional {
			if c := r.Summary[e]; r.Extensions[e] && c.Fail > 0 {
				pr.Met = false
				why = append(why, fmt.Sprintf("%d %s failures", c.Fail, e.Title()))
			}
		}
		if c := r.Summary[ExtDiscovery]; c.Fail > 0 {
			pr.Met = false
			why = append(why, fmt.Sprintf("%d discovery failures", c.Fail))
		}
		pr.Why = strings.Join(why, ", ")
		r.Profiles = append(r.Profiles, pr)
	}
}

// WriteJSON writes the report as indented JSON.
func (r *Report) WriteJSON(w io.Writer) error {
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	return enc.Encode(r)
}

// WriteText writes a per-extension report for people. With verbose set,
// notes on passing and skipped tests are shown too.
func (r *Report) WriteText(w io.Writer, verbose bool) {
	fmt.Fprintf(w, "dotfs conformance: %s", r.Addr)
	if r.Aname != "" {
		fmt.Fprintf(w, " (attach %q)", r.Aname)
	}
	fmt.Fprintf(w, "\n\n")
	var offered []string
	for _, e := range Exts[1:] {
		mark := "no"
		if r.Extensions[e] {
			mark = "yes"
		}
		offered = append(offered, fmt.Sprintf("%s %s", e.Title(), mark))
	}
	fmt.Fprintf(w, "extensions: %s\n", strings.Join(offered, ", "))
	for _, e := range Exts {
		c := r.Summary[e]
		if c == nil || c.Pass+c.Fail+c.Warn+c.Skip == 0 {
			continue
		}
		fmt.Fprintf(w, "\n%s: %s\n", e.Title(), c)
		for _, res := range r.Results {
			if res.Ext != e {
				continue
			}
			fmt.Fprintf(w, "  %-4s %-36s %s\n", strings.ToUpper(string(res.Status)), res.ID, res.Name)
			if res.Status == Fail || res.Status == Warn || verbose {
				if res.Status == Fail || res.Status == Warn {
					fmt.Fprintf(w, "         spec: %s\n", res.Section)
				}
				for _, m := range res.Messages {
					for _, line := range strings.Split(m, "\n") {
						fmt.Fprintf(w, "         %s\n", line)
					}
				}
			}
		}
	}
	fmt.Fprintf(w, "\nprofiles:\n")
	for _, p := range r.Profiles {
		if p.Met {
			fmt.Fprintf(w, "  %-16s met\n", p.Name)
		} else {
			fmt.Fprintf(w, "  %-16s not met (%s)\n", p.Name, p.Why)
		}
	}
	verdict := "PASS"
	if !r.OK {
		verdict = "FAIL"
	}
	fmt.Fprintf(w, "\n%s: %s in %v\n", verdict, &r.Total, r.Duration.Round(time.Millisecond))
}

func (c *Counts) String() string {
	s := fmt.Sprintf("%d passed, %d failed", c.Pass, c.Fail)
	if c.Warn > 0 {
		s += fmt.Sprintf(", %d warnings", c.Warn)
	}
	if c.Skip > 0 {
		s += fmt.Sprintf(", %d skipped", c.Skip)
	}
	return s
}
