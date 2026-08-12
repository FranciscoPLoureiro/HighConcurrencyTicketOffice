// Package metrics is what the system says about itself.
//
// It owns one Prometheus registry and every collector registered on it, so that
// the packages doing the work depend on a two-method interface rather than on
// Prometheus. That is the same arrangement the rate limiter and the idempotency
// store already use, and for the same reason: a handler test should not need a
// metrics registry any more than it needs a Redis.
//
// Everything here is deliberately cheap. A counter increment is an atomic add,
// and the histograms below are the only thing on the purchase path that
// allocates — which is why their buckets are chosen rather than inherited.
package metrics

import (
	"net/http"
	"strconv"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// requestBuckets are the latency buckets for HTTP requests.
//
// Not the library defaults, which are tuned for requests measured in whole
// seconds and have nothing at all between 100 ms and 250 ms. The load test
// fails CI if the p99 of a purchase exceeds 200 ms, and a quantile is
// interpolated *within* whichever bucket it falls in — so with the defaults the
// number that decides the build would be a straight-line guess across a gap
// wider than the threshold itself.
//
// 0.2 is therefore a boundary rather than a value that happens to be covered.
// The rest are spaced to give real resolution either side of it, and the long
// tail is coarse because once a request takes two seconds the interesting fact
// is that it did, not whether it took 2.1 or 2.4.
var requestBuckets = []float64{
	0.005, 0.01, 0.025, 0.05, 0.075, 0.1, 0.15, 0.2, 0.3, 0.5, 0.75, 1, 2, 5,
}

// fulfilmentBuckets cover work measured in seconds rather than milliseconds:
// the simulated render alone is two seconds, so the HTTP buckets would put
// every fulfilment in the same overflow bucket and report nothing.
var fulfilmentBuckets = []float64{0.5, 1, 1.5, 2, 2.5, 3, 5, 10, 30, 60}

// queueLatencyBuckets measure how long a ticket waited between being sold and
// being picked up, which is the number that says whether the workers are
// keeping up. It spans milliseconds to minutes because a healthy system and a
// backlogged one differ by that much.
var queueLatencyBuckets = []float64{0.01, 0.05, 0.1, 0.5, 1, 5, 15, 60, 300}

// Metrics holds the registry and every collector on it.
type Metrics struct {
	registry *prometheus.Registry

	requestDuration *prometheus.HistogramVec
	requestsTotal   *prometheus.CounterVec

	ticketsSold     prometheus.Counter
	ticketsRejected *prometheus.CounterVec
	compensations   *prometheus.CounterVec

	fulfilmentDuration prometheus.Histogram
	fulfilmentsTotal   *prometheus.CounterVec
	queueLatency       prometheus.Histogram

	queueDepth *prometheus.GaugeVec
}

// New builds a registry and registers everything on it.
//
// A registry of its own rather than the package-global default, which comes
// with whatever any dependency happened to register on it. Two of the standard
// collectors are added back explicitly, because "how much memory is this
// process using and how many goroutines are alive" is the first question asked
// when latency moves and nothing in the business metrics explains it.
func New() *Metrics {
	registry := prometheus.NewRegistry()
	registry.MustRegister(
		collectors.NewGoCollector(),
		collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}),
	)

	m := &Metrics{
		registry: registry,

		requestDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "http_request_duration_seconds",
			Help:    "How long a request took, by route and outcome.",
			Buckets: requestBuckets,
		}, []string{"route", "method", "status"}),

		requestsTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "http_requests_total",
			Help: "Requests served, by route and outcome.",
		}, []string{"route", "method", "status"}),

		// The headline business number, and the one the load test asserts is
		// exactly one hundred.
		ticketsSold: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "tickets_sold_total",
			Help: "Tickets granted by the atomic script and durably recorded.",
		}),

		// Labelled by reason, because "we refused four thousand nine hundred
		// requests" is not a fact anybody can act on and "we refused four
		// thousand nine hundred because the campaign was empty" is.
		ticketsRejected: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "tickets_rejected_total",
			Help: "Purchases refused, by reason.",
		}, []string{"reason"}),

		compensations: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "compensations_total",
			Help: "Tickets handed back after a sale could not be completed, by outcome.",
		}, []string{"outcome"}),

		fulfilmentDuration: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:    "fulfilment_duration_seconds",
			Help:    "How long the worker took to finish one ticket.",
			Buckets: fulfilmentBuckets,
		}),

		fulfilmentsTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "fulfilments_total",
			Help: "Fulfilment attempts, by outcome.",
		}, []string{"outcome"}),

		queueLatency: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:    "ticket_queue_latency_seconds",
			Help:    "How long a ticket waited between being sold and being picked up.",
			Buckets: queueLatencyBuckets,
		}),

		queueDepth: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "rabbitmq_queue_messages",
			Help: "Messages waiting in a queue.",
		}, []string{"queue"}),
	}

	registry.MustRegister(
		m.requestDuration, m.requestsTotal,
		m.ticketsSold, m.ticketsRejected, m.compensations,
		m.fulfilmentDuration, m.fulfilmentsTotal, m.queueLatency,
		m.queueDepth,
	)

	return m
}

// Handler serves the registry over HTTP.
func (m *Metrics) Handler() http.Handler {
	return promhttp.HandlerFor(m.registry, promhttp.HandlerOpts{
		// A failing collector should be visible in the scrape rather than
		// silently reducing the number of series, which looks like traffic
		// disappearing.
		ErrorHandling: promhttp.HTTPErrorOnError,
	})
}

// ObserveRequest records one served request.
//
// The route is the pattern rather than the path. Labelling by path would put
// every purchase id in its own time series, and a metric with unbounded
// cardinality takes the monitoring system down some time after the thing it was
// meant to be watching.
func (m *Metrics) ObserveRequest(route, method string, status int, elapsed time.Duration) {
	code := strconv.Itoa(status)
	m.requestDuration.WithLabelValues(route, method, code).Observe(elapsed.Seconds())
	m.requestsTotal.WithLabelValues(route, method, code).Inc()
}

// TicketSold records a completed sale.
func (m *Metrics) TicketSold() { m.ticketsSold.Inc() }

// TicketRejected records a refusal and why.
func (m *Metrics) TicketRejected(reason string) {
	m.ticketsRejected.WithLabelValues(reason).Inc()
}

// Compensated records a ticket handed back, and whether handing it back worked.
//
// The failed outcome is the one worth alerting on: it means a ticket is out of
// circulation and nothing but the next reconciliation will notice.
func (m *Metrics) Compensated(outcome string) {
	m.compensations.WithLabelValues(outcome).Inc()
}

// ObserveFulfilment records one fulfilment attempt.
func (m *Metrics) ObserveFulfilment(outcome string, elapsed time.Duration) {
	m.fulfilmentsTotal.WithLabelValues(outcome).Inc()
	m.fulfilmentDuration.Observe(elapsed.Seconds())
}

// ObserveQueueLatency records how long a ticket waited to be picked up.
//
// Clock skew between the API and the worker can make this negative, and a
// negative observation would drag the histogram's sum below zero and make every
// average computed from it nonsense. Clamped rather than dropped, because a
// message that appears to arrive before it was sent is still a message.
func (m *Metrics) ObserveQueueLatency(waited time.Duration) {
	m.queueLatency.Observe(max(waited.Seconds(), 0))
}

// SetQueueDepth records how many messages a queue is holding.
func (m *Metrics) SetQueueDepth(queue string, depth int) {
	m.queueDepth.WithLabelValues(queue).Set(float64(depth))
}
