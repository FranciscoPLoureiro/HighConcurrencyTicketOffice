# High Concurrency Ticket Office

[![CI](https://github.com/FranciscoPLoureiro/HighConcurrencyTicketOffice/actions/workflows/ci.yml/badge.svg)](https://github.com/FranciscoPLoureiro/HighConcurrencyTicketOffice/actions/workflows/ci.yml)

A student festival releases exactly **100 tickets at 80% off**, and the campaign
opens at midnight. Around **5,000 people** press *Buy* inside the same few
seconds. This is the backend that sells those tickets without ever selling 101
and without letting one person take two — both of which it now does — and,
eventually, without losing a ticket when a process dies halfway through a
purchase.

That last one is the interesting problem, and it is now closed. The stock lives
in Redis and the fulfilment work lives in RabbitMQ: separate systems that fail
independently, so "decrement the stock, then publish the job" has a gap in the
middle where a ticket can vanish. A reservation makes the gap visible and a
sweeper closes it, which the test suite proves by killing sales mid-flight and
watching the tickets come back.

## The argument, in one table

500 virtual students against a 100 ticket campaign, through the real HTTP API,
on the same machine and the same command for every column:

| | Phase 1 — SELECT then UPDATE | Phase 2 — one Lua script | Phase 3 — 202 and a worker |
|---|---|---|---|
| Tickets sold | **500** | **100** | **100** |
| Oversold | **400** | **0** | **0** |
| `available` in PostgreSQL afterwards | **−400** | 0 | 0 |
| p95 latency | 2.65 s | 1.01 s | **0.58 s** |
| Throughput | 176 req/s | 372 req/s | **564 req/s** |

Phase 3 adds a synchronous `INSERT` and a confirmed publish to the request path
and comes out *faster*, which deserves an explanation rather than a victory lap.
That explanation, the full table and the sequence diagram of the race are in
[the decisions](docs/DECISIONS.md#why-a-select-followed-by-an-update-does-not-hold).

## Status

Built in phases, each one a branch of small commits merged through a pull
request.

| Phase | Scope | State |
|---|---|---|
| 0 | CI/CD, containers, health checks | ✅ done |
| 1 | Naive MVP that demonstrates the race | ✅ done |
| 2 | Atomic purchase in Redis, stock reconciliation | ✅ done |
| 3 | Async fulfilment with RabbitMQ, idempotency | ✅ done |
| 4 | Prometheus, Grafana, calibrated load testing | ✅ done |
| 5 | The lost ticket, compensation saga, failure modes | ✅ done |

Every phase is merged. The remaining work is in the roadmap at the bottom, and
the known limitations section is honest about what this still does not do.

One of those limitations turned into its own project: phase 4 could describe a
p99 that moved with Prometheus running and not say why. The cause was measured
with [wallclock](https://github.com/FranciscoPLoureiro/wallclock), an eBPF
wall-clock profiler written for the question, and the answer was not the
obvious one —
[where those milliseconds went](docs/MEASUREMENT.md#where-those-milliseconds-actually-went).

## Quick start

Requires Docker and GNU make. Everything else the project builds or runs with —
the Go toolchain, the linter, the load generator — is pulled as an image, so
there is nothing to install and nothing to keep in step with CI.

```bash
make up
curl localhost:8080/health
```

```json
{
  "status": "ok",
  "checks": {
    "postgres": { "status": "ok", "latency_ms": 1 },
    "redis":    { "status": "ok", "latency_ms": 0 },
    "rabbitmq": { "status": "ok", "latency_ms": 2 }
  }
}
```

`/health` returns **503** when any dependency is unreachable, so an orchestrator
can act on the status line without parsing the body. The body still names the
failing dependency, because that is the difference between an alert someone can
act on and one they cannot.

`make up` starts the API, the worker, their three dependencies, and Prometheus
and Grafana — the dashboard is on <http://localhost:3000> and needs no login.

The **api** serves requests and decides who gets a ticket; the **worker**
generates the tickets it sold. The worker publishes no port and answers no
health check on purpose — whether it is working is visible
in the depth of `ticket_processing_queue`, which is a better signal than a
running process, because a worker wedged on a dependency passes any probe it
could serve about itself. Add more of them independently:

```bash
docker compose up -d --scale worker=3
```

RabbitMQ's management UI is on <http://localhost:15672> (`tickets` / `tickets`),
which is the quickest way to watch the queue drain, or to look at whatever
ended up in the dead letter queue.

| Command | Does |
|---|---|
| `make up` | Build and start everything, waiting until healthy |
| `make test` | Tests with the race detector |
| `make lint` | golangci-lint |
| `make verify` | Everything CI runs, in the same order |
| `make integration-test` | Tests against real containers via Testcontainers |
| `make cover` | Coverage across the whole suite, integration included |
| `make load-test-calibrate` | Measure the generator before believing it |
| `make load-test-ramp` | The CI profile, with thresholds that fail |
| `make verify-campaign` | Check the invariants in PostgreSQL and Redis |
| `make load-test-campaign-internal` | The midnight burst, inside the network |
| `make dashboard` | Render the Grafana dashboard to `docs/` |
| `make reset` | Truncate, flush, purge and reconcile |

Run `make` on its own for the full list. The two `load-test` targets that drive
the API from the host expect a `k6` binary there; the `-internal` one, which is
the only one whose numbers this README quotes, runs k6 from an image.

## Architecture

Solid lines are built. Dashed ones are phase 5.

```
Client ──Idempotency-Key──> [Rate limiter] ──429 + Retry-After──>
                                  │          sliding window, per user and per IP
                                  v
                            [API — Go] ──idempotency claim──> previous response
                                  │        (atomic, in Redis)
                                  v
                    [Redis: one atomic Lua script]
             checks the buyer, checks stock, decrements, records
                    │                              │
                success                     no stock / already bought
                    v                              v
              [PostgreSQL]                    409 Conflict
              writes the row as *pending* — the seat is taken from here on
                    │
                    v
              [RabbitMQ]  publisher confirms + mandatory
                    │     ticket_processing_queue
                    v
              [Worker]  generates the ticket, settles the row, *then* acks
                    │
        fails ──> [retry 5s] ──> [retry 30s] ──> [dead letter queue]
                                                       ┊
                                                       ┊┄┄> phase 5.3:
                                                            compensate, INCR

  [PostgreSQL] ── source of truth; rebuilds the Redis stock *and* the set of
                  people who already bought, under a distributed lock, on
                  every start

  API returns 202 + correlation id; the client polls /status.

  [Prometheus] <-- scrapes the API and every worker; [Grafana] draws it

  [Sweeper]  in the API, every 15s, under the reconciliation lock
      |  reservation open and no row  --> give the ticket back
      +--reservation open and a row --> close it
      |  row pending and no message  --> publish it again
      v
  [Saga]  in the worker, draining the dead letter queue
      cancel the row, INCR the stock, drop the holder, notify
```

A sale makes three writes across three systems, and no two of them are atomic
with each other. The Lua script settles the contention, the PostgreSQL row makes
the sale durable, and the publish hands the rest away. What happens when one of
them fails is the whole design, and it turns on a single question each time:
*does this failure prove the write did not happen?* If it does, the sale is
undone. If it does not — a commit that timed out, a confirm that never arrived —
nothing is undone, because undoing a write that actually succeeded is how a
hundred tickets becomes a hundred and one.

The gap that remains is between the Lua script and the row: a process that dies
in those microseconds leaves a ticket decremented and unrecorded. Phase 5.1
closes it with a reservation that expires.

## Known limitations

The five worth knowing before reading any of the above as a production claim.
The [full list](docs/LIMITATIONS.md) is longer, and kept honest as the project
grows.

- Authentication is out of scope by design. Requests carry an `X-User-ID`
  header, standing in for a JWT already validated by a gateway. This is a
  deliberate simplification of the brief, not an oversight — the interesting
  problem here is contention, not identity.
- **A ticket stranded by a crash is recovered within a sweep, not instantly.**
  The reservation makes it visible and the sweeper returns it, but only once it
  has been open longer than `RESERVATION_AGE` — a minute by default, because
  releasing one whose sale is still in flight would sell the seat twice. So the
  worst case is a ticket off the shelf for about seventy-five seconds. During a
  campaign decided in the first few seconds, that is a ticket which effectively
  did not sell, and the honest fix is not a shorter timer but fewer crashes.
- **A write whose outcome is unknown is never undone, and that costs a ticket.**
  A `COMMIT` that times out, or a publish the broker never confirmed, may have
  succeeded. Compensating there would return stock the system has already given
  away, so nothing is compensated and the ticket stays out of circulation until
  the next reconciliation. Underselling by one is recoverable; overselling by
  one is not.
- **The sweeper is the one component that can cause an oversell.** Everything
  else here errs towards keeping a ticket off the shelf; this is the only thing
  whose job is to put one back. It is guarded by asking the source of truth
  before every release, by treating an unanswered query as "do not touch", and
  by a startup check that refuses a `RESERVATION_AGE` which does not outlast the
  request budget *and* one more database budget — but it is the piece to read
  first if a ticket is ever sold twice.
- **No circuit breaker.** The brief lists one as a bonus: if PostgreSQL is
  unavailable, the API should answer `503` immediately rather than accumulating
  timeouts. Today every call has its own budget, so a database outage produces
  slow failures rather than hung ones — bounded, but not fast. It is in the
  roadmap rather than done, and the honest reason is that the timeouts already
  bound the damage and a breaker would be the next improvement rather than a
  missing guarantee.

## The rest

The detail that used to live on this page, kept where it can be read on purpose
rather than scrolled past:

- **[docs/DECISIONS.md](docs/DECISIONS.md)** — twenty decisions in the format
  context, the options actually considered, the decision, and what it costs.
  Why a SELECT then an UPDATE does not hold, why one Lua script instead of
  `WATCH`/`MULTI`/`EXEC`, and the lost ticket with the three ways out of it.
- **[docs/MEASUREMENT.md](docs/MEASUREMENT.md)** — the two declared
  environments, the calibration that comes before believing any number, the run
  that gates the build, and where those milliseconds actually went.
- **[docs/API.md](docs/API.md)** — the endpoints, every status and
  `error.code` the API can answer with, and the idempotency contract.
- **[docs/LIMITATIONS.md](docs/LIMITATIONS.md)** — the full list, and why there
  is no public URL.

## Roadmap

What would come next, in the order it would earn its place:

- **A circuit breaker on PostgreSQL.** The brief's bonus, and the one genuinely
  missing piece of resilience: today a database outage produces slow failures
  rather than immediate ones.
- **A consumer for the compensation queue** — a notification service telling
  people their ticket failed, which is the only thing currently published into
  the void.
- **Real authentication.** `X-User-ID` stands in for a validated JWT, which is a
  documented simplification and would be the first thing to fix for real users.
- **Infrastructure as code**, if this ever needed to run somewhere permanently.
- **Multi-region**, which changes everything above and none of the concurrency
  problem this project exists to solve — the stock invariant is a single-writer
  problem wherever it runs.

## Licence

MIT. See [LICENSE](LICENSE).
