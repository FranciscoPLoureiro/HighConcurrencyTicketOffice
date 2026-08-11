# High Concurrency Ticket Office

[![CI](https://github.com/FranciscoPLoureiro/HighConcurrencyTicketOffice/actions/workflows/ci.yml/badge.svg)](https://github.com/FranciscoPLoureiro/HighConcurrencyTicketOffice/actions/workflows/ci.yml)

A student festival releases exactly **100 tickets at 80% off**, and the campaign
opens at midnight. Around **5,000 people** press *Buy* inside the same few
seconds. This is the backend that sells those tickets without ever selling 101,
without letting one person take two, and without losing a ticket if a process
dies halfway through a purchase.

The last of those is the interesting one. The stock lives in Redis and the
fulfilment work lives in RabbitMQ; they are separate systems that fail
independently, so "decrement the stock, then publish the job" has a gap in the
middle where a ticket can vanish. Most of this repository is about that gap.

## Status

Built in phases, each one a branch of small commits merged through a pull
request.

| Phase | Scope | State |
|---|---|---|
| 0 | CI/CD, containers, health checks | ✅ done |
| 1 | Naive MVP that demonstrates the race | 🔜 next |
| 2 | Atomic purchase in Redis, stock reconciliation | ⬜ |
| 3 | Async fulfilment with RabbitMQ, idempotency | ⬜ |
| 4 | Prometheus, Grafana, calibrated load testing | ⬜ |
| 5 | The lost ticket, compensation saga, failure modes | ⬜ |

Sections below marked *(phase N)* describe work that has not landed yet and are
listed so the shape of the system is visible from the start.

## Quick start

Requires Docker. Nothing else.

```bash
make up
curl localhost:8080/health
```

```json
{
  "status": "ok",
  "checks": {
    "postgres": { "status": "ok", "latency_ms": 1 },
    "redis":    { "status": "ok", "latency_ms": 0 }
  }
}
```

`/health` returns **503** when any dependency is unreachable, so an orchestrator
can act on the status line without parsing the body. The body still names the
failing dependency, because that is the difference between an alert someone can
act on and one they cannot.

| Command | Does |
|---|---|
| `make up` | Build and start everything, waiting until healthy |
| `make test` | Tests with the race detector |
| `make lint` | golangci-lint |
| `make verify` | Everything CI runs, in the same order |
| `make load-test` | k6 against a running stack |
| `make reset` | Destroy all state and come back up clean |

Run `make` on its own for the full list.

### The Go toolchain runs in a container

`make test` and friends execute the Go toolchain inside `golang:1.26` rather
than on the host, so local builds and CI compile in the same environment. It is
also a hard requirement on the machine this was developed on, where Windows
Smart App Control blocks the unsigned Go toolchain binaries outright. Pass
`GO_LOCAL=1` to use a host toolchain instead:

```bash
make test GO_LOCAL=1
```

## Architecture

Target shape. Phases 2, 3 and 5 fill it in.

```
Client ──Idempotency-Key──> [Rate limiter] ──429──>
                                  │
                                  v
                            [API — Go] ──idempotency cache──> previous response
                                  │
                                  v
                    [Redis: one atomic Lua script]
        checks the buyer, checks stock, decrements, reserves with a TTL
                    │                              │
                success                     no stock / already bought
                    v                              v
              [RabbitMQ]                      409 Conflict
                    │      ^
                    v      │ reconciliation of expired reservations
               [Worker] ───┘
                    │  generates the ticket, confirms in Postgres, acks
                    │
                    └──failure──> [DLQ] ──> [compensation queue] ──> INCR in Redis
                    │
                    v
              [PostgreSQL]  ← source of truth; reconciles Redis at startup
```

## Architectural decisions

The format is deliberate: context, the options that were actually considered,
the decision, and what it costs.

### Why Go

**Context.** The load is roughly 5,000 concurrent requests arriving in a few
seconds, each one mostly waiting on network I/O to Redis, Postgres and RabbitMQ.
Nearly all of the wall clock is spent blocked, not computing.

**Options.** Go, Java with Spring Boot, or C# on .NET. All three can do this.

**Decision.** Go, for two properties that matter under exactly this shape of
load.

A goroutine starts with a **2 KiB stack** that grows by copying as needed, so
5,000 in-flight requests cost single-digit megabytes of stack. Modelling the
same concurrency with OS threads costs a default 8 MiB of virtual address space
each, and the kernel scheduler starts to matter well before that number.

More importantly, blocking is cheap in the case this system actually hits. When
a goroutine waits on a socket, the runtime parks it in the **netpoller** — epoll
underneath — and the OS thread immediately runs another goroutine. No thread is
consumed by waiting. This is not universal: a genuinely blocking syscall does
block its thread, and the runtime responds by handing that processor to another
thread. Since essentially all the waiting here is network I/O, the system stays
on the cheap path.

The garbage collector is a concurrent mark-and-sweep with sub-millisecond stop
the world pauses. For a project judged on p99 latency, a collector that does not
introduce tail spikes is worth more than raw throughput.

**Consequences.** Cheap concurrency makes it easy to write code that leaks
goroutines instead of threads, which is quieter and harder to notice. Every
outbound call therefore carries a context with a deadline, and the race detector
runs in CI on every push.

### Why the standard library instead of a web framework

**Context.** The API has a handful of routes and needs middleware for identity,
rate limiting and idempotency.

**Options.** `chi`, `gin`, or `net/http`.

**Decision.** `net/http`. Since Go 1.22 the standard `ServeMux` matches on
method and path pattern (`GET /api/v1/tickets/{id}/status`), which is the only
feature a router was previously needed for. Middleware is a function that takes
and returns an `http.Handler`.

**Consequences.** A dependency avoided on the request path, and no framework
conventions between the reader and what the code does. The cost is writing a few
middleware helpers by hand that a framework would have supplied.

## Known limitations

Kept honest as the project grows.

- Authentication is out of scope by design. Requests carry an `X-User-ID`
  header, standing in for a JWT already validated by a gateway. This is a
  deliberate simplification of the brief, not an oversight — the interesting
  problem here is contention, not identity.
- Payment is out of scope. A purchase reserves a ticket; no money moves.
- Everything below is not built yet and the sections above say so where it
  matters. The current API serves `/health` and nothing else.

## Roadmap

Beyond the phases above: real authentication, infrastructure as code rather than
a Makefile, and multi-region — none of which change the concurrency problem this
project exists to solve.

## Licence

MIT. See [LICENSE](LICENSE).
