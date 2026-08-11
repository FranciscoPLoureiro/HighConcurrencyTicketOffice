package health

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"
)

func okProbe(name string) Probe {
	return Probe{Name: name, Ping: func(context.Context) error { return nil }}
}

func failingProbe(name string, err error) Probe {
	return Probe{Name: name, Ping: func(context.Context) error { return err }}
}

func TestAProcessThatReachesEverythingIsHealthy(t *testing.T) {
	report := New(okProbe("postgres"), okProbe("redis")).Check(context.Background())

	if !report.Healthy() {
		t.Errorf("Healthy() = false, want true; report = %+v", report)
	}
	if len(report.Checks) != 2 {
		t.Fatalf("got %d checks, want 2", len(report.Checks))
	}
	for name, result := range report.Checks {
		if result.Status != StatusOK {
			t.Errorf("check %q = %s, want %s", name, result.Status, StatusOK)
		}
		if result.Error != "" {
			t.Errorf("check %q carries error %q on success", name, result.Error)
		}
	}
}

// The endpoint exists so an orchestrator can stop routing traffic to a process
// that cannot serve it. One unreachable dependency has to be enough.
func TestOneUnreachableDependencyMakesTheProcessUnavailable(t *testing.T) {
	report := New(
		okProbe("redis"),
		failingProbe("postgres", errors.New("connection refused")),
	).Check(context.Background())

	if report.Healthy() {
		t.Error("Healthy() = true with postgres down, want false")
	}
	if report.Status != StatusUnavailable {
		t.Errorf("overall status = %s, want %s", report.Status, StatusUnavailable)
	}

	// The healthy dependency must still be reported as healthy: knowing
	// which half is broken is the entire point of per-dependency reporting.
	if got := report.Checks["redis"].Status; got != StatusOK {
		t.Errorf("redis = %s, want %s", got, StatusOK)
	}
	if got := report.Checks["postgres"].Status; got != StatusUnavailable {
		t.Errorf("postgres = %s, want %s", got, StatusUnavailable)
	}
	if got := report.Checks["postgres"].Error; !strings.Contains(got, "connection refused") {
		t.Errorf("postgres error = %q, want it to carry the underlying reason", got)
	}
}

// A dependency that is up but wedged never answers and never fails. Without a
// deadline the health endpoint inherits the hang, the load balancer times out,
// and the platform reports "no response" instead of "postgres is unavailable".
func TestAHangingDependencyIsReportedRatherThanInherited(t *testing.T) {
	hanging := Probe{
		Name: "postgres",
		Ping: func(ctx context.Context) error {
			<-ctx.Done()
			return ctx.Err()
		},
	}

	report := New(hanging).WithTimeout(20 * time.Millisecond).Check(context.Background())

	if report.Healthy() {
		t.Error("Healthy() = true for a probe that never answered")
	}
	if got := report.Checks["postgres"].Error; !strings.Contains(got, "deadline exceeded") {
		t.Errorf("postgres error = %q, want a deadline error", got)
	}
}

// Total latency should be bounded by the slowest dependency, not by their sum.
//
// The test is deterministic rather than timing-based: both probes must be in
// flight before either is allowed to return. Sequential execution blocks on the
// second receive and the test fails by timing out.
func TestProbesRunConcurrently(t *testing.T) {
	started := make(chan struct{}, 2)
	release := make(chan struct{})

	blocking := func(name string) Probe {
		return Probe{Name: name, Ping: func(context.Context) error {
			started <- struct{}{}
			<-release
			return nil
		}}
	}

	checker := New(blocking("postgres"), blocking("redis"))
	done := make(chan Report, 1)
	go func() { done <- checker.Check(context.Background()) }()

	<-started
	<-started
	close(release)

	if report := <-done; !report.Healthy() {
		t.Errorf("Healthy() = false, want true; report = %+v", report)
	}
}

func TestLatencyIsRecordedPerDependency(t *testing.T) {
	checker := New(okProbe("postgres"))

	// Drive the clock so the assertion does not depend on how fast the
	// machine running the test happens to be.
	start := time.Now()
	calls := 0
	checker.now = func() time.Time {
		calls++
		if calls == 1 {
			return start
		}
		return start.Add(150 * time.Millisecond)
	}

	report := checker.Check(context.Background())

	if got := report.Checks["postgres"].LatencyMS; got != 150 {
		t.Errorf("LatencyMS = %d, want 150", got)
	}
}

func TestAProcessWithNoDependenciesIsHealthy(t *testing.T) {
	report := New().Check(context.Background())

	if !report.Healthy() {
		t.Errorf("Healthy() = false for a checker with no probes, want true")
	}
	if len(report.Checks) != 0 {
		t.Errorf("got %d checks, want none", len(report.Checks))
	}
}
