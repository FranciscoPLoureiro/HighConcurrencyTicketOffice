// Package health reports whether the process can reach the dependencies it
// needs in order to serve traffic.
//
// The package deliberately knows nothing about Postgres or Redis. Callers hand
// it named probes, which keeps the aggregation logic testable without any
// infrastructure and lets later phases add dependencies without touching this
// file.
package health

import (
	"context"
	"sync"
	"time"
)

// Status is the state of one dependency, or of the process as a whole.
type Status string

const (
	StatusOK          Status = "ok"
	StatusUnavailable Status = "unavailable"
)

// Probe is a single named dependency check.
type Probe struct {
	// Name identifies the dependency in the response body, e.g. "postgres".
	Name string
	// Ping must return nil when the dependency is usable. It is expected to
	// honour the context deadline.
	Ping func(context.Context) error
}

// Result is the outcome of one probe.
type Result struct {
	Status Status `json:"status"`
	// Error carries the failure reason. It is intended for an operator
	// reading logs, and is omitted entirely when the probe succeeded.
	Error string `json:"error,omitempty"`
	// LatencyMS records how long the dependency took to answer. A
	// dependency that is reachable but slow is worth seeing before it
	// becomes a dependency that is unreachable.
	LatencyMS int64 `json:"latency_ms"`
}

// Report is the aggregate health of the process.
type Report struct {
	Status Status            `json:"status"`
	Checks map[string]Result `json:"checks"`
}

// Healthy reports whether every dependency answered.
func (r Report) Healthy() bool { return r.Status == StatusOK }

// Checker runs a fixed set of probes.
type Checker struct {
	probes  []Probe
	timeout time.Duration
	// now is injectable so tests can measure latency deterministically.
	now func() time.Time
}

// DefaultTimeout bounds each individual probe.
//
// The value matters more than it looks. An unreachable Postgres typically fails
// fast, but a Postgres that is up and wedged does not fail at all — it simply
// never answers. Without a deadline the health endpoint inherits that hang, the
// load balancer times out, and the platform reports "no response" instead of
// the far more useful "postgres is unavailable".
const DefaultTimeout = 2 * time.Second

// New builds a Checker over the given probes.
func New(probes ...Probe) *Checker {
	return &Checker{
		probes:  probes,
		timeout: DefaultTimeout,
		now:     time.Now,
	}
}

// WithTimeout overrides the per-probe deadline.
func (c *Checker) WithTimeout(d time.Duration) *Checker {
	c.timeout = d
	return c
}

// Check runs every probe and aggregates the results.
//
// Probes run concurrently so that total latency is bounded by the slowest
// dependency rather than by their sum. With two dependencies that is a small
// win; it stops being small as soon as a third is added.
func (c *Checker) Check(ctx context.Context) Report {
	results := make([]Result, len(c.probes))

	var wg sync.WaitGroup
	for i, probe := range c.probes {
		wg.Add(1)
		go func() {
			defer wg.Done()
			results[i] = c.run(ctx, probe)
		}()
	}
	wg.Wait()

	report := Report{
		Status: StatusOK,
		Checks: make(map[string]Result, len(c.probes)),
	}
	for i, probe := range c.probes {
		report.Checks[probe.Name] = results[i]
		if results[i].Status != StatusOK {
			// One unreachable dependency is enough. The API cannot sell
			// a ticket without Postgres or Redis, so reporting anything
			// other than unavailable would invite traffic it must reject.
			report.Status = StatusUnavailable
		}
	}

	return report
}

func (c *Checker) run(ctx context.Context, probe Probe) Result {
	ctx, cancel := context.WithTimeout(ctx, c.timeout)
	defer cancel()

	start := c.now()
	err := probe.Ping(ctx)
	latency := c.now().Sub(start)

	result := Result{
		Status:    StatusOK,
		LatencyMS: latency.Milliseconds(),
	}
	if err != nil {
		result.Status = StatusUnavailable
		result.Error = err.Error()
	}
	return result
}
