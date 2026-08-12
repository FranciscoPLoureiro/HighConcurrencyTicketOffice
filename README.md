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

## The API

A purchase needs two headers: who is buying, and a key that makes the request
safe to send twice.

```bash
curl -X POST localhost:8080/api/v1/tickets/purchase \
  -H 'X-User-ID: student-1' \
  -H "Idempotency-Key: $(uuidgen)"
```

```json
{
  "purchase_id": "5a3f1c88-2e4b-4f7a-9d21-0c6b8e1a4f30",
  "campaign_id": "queima-2026",
  "user_id": "student-1",
  "status": "pending",
  "correlation_id": "c0ffee00-1111-4222-8333-444455556666",
  "status_url": "/api/v1/tickets/5a3f1c88-2e4b-4f7a-9d21-0c6b8e1a4f30/status",
  "created_at": "2026-08-12T00:00:00.123456Z"
}
```

**`202`, not `200`.** The seat is decided and durably recorded; the document is
not, and will not be for another two seconds. Answering `200` would promise a
ticket that does not exist yet. The caller polls `status_url` — or, if it has
one, watches for the `correlation_id` in the logs.

```bash
curl localhost:8080/api/v1/tickets/{id}/status -H 'X-User-ID: student-1'
```

```json
{ "purchase_id": "5a3f…", "status": "confirmed",
  "created_at": "…", "updated_at": "…" }
```

A purchase moves `pending → confirmed` when the worker finishes it,
`pending → failed` if it exhausts its retries, and `→ cancelled` only if
something deliberately gives the ticket back. Everything except `cancelled`
holds a seat.

Every outcome has its own status and its own machine-readable code. Several
share a status, so the code is the contract and the prose beside it is not —
a client should switch on `error.code`, never on the message.

| Status | `error.code` | Means |
|---|---|---|
| `202` | — | The seat is yours; the ticket is being generated |
| `409` | `stock_exhausted` | The campaign is empty |
| `409` | `already_purchased` | You already hold one |
| `409` | `idempotency_key_in_use` | An earlier request with this key is still running |
| `409` | `idempotency_key_replayed` | This key already bought a ticket, and the response is no longer cached |
| `429` | `rate_limited` | Too many attempts; see `Retry-After` |
| `400` | `missing_idempotency_key` | No `Idempotency-Key` header |
| `400` | `invalid_idempotency_key` | The key is not a UUID |
| `401` | `missing_identity` | No `X-User-ID` header |
| `404` | `campaign_not_found` | No such campaign, or Redis holds no stock for it |
| `404` | `purchase_not_found` | No such purchase — or it belongs to somebody else |

```json
{ "error": { "code": "already_purchased",
             "message": "this account already holds a ticket for this campaign" } }
```

`404` covers two cases worth naming. If Redis has no stock counter for the
campaign, the answer is *not* `stock_exhausted`: a missing key means
reconciliation has not run, and telling a caller "sold out" in that state is a
lie that looks like a normal, final refusal — they stop trying and nobody
investigates. It is a `404` and a loud log line instead. And a purchase that
exists but belongs to somebody else is also a `404`, not a `403`, because
"this exists but is not yours" confirms that a guessed identifier is real.

Repeating a request with the same `Idempotency-Key` returns the original
response byte for byte, with `Idempotent-Replay: true` on it. That is the whole
point of the header, and [why it has to come from the client](#why-the-client-generates-the-idempotency-key)
is its own section below.

### Measure the generator before believing the numbers

On Docker Desktop for Windows, driving the API through its published port
refused **57% of connections** at 500 virtual users — before a single request
reached the application. The API logged nothing, because nothing arrived.

Worse, the first version of the load test scored that run as a pass: k6 reports
status `0` for a request that never got a response, and the check was
`status < 500`. Both the measurement and the thing measuring it were wrong.

`make load-test-campaign-internal` runs the generator on the same Docker network
as the API, which removes the host's port forwarding from the path: 0% failures,
same hardware. Any number quoted in this README comes from that path.

### The test environment, declared

A load figure without the machine it came from is decoration. This is the
machine, and these are the limits every service runs under — set in
`docker-compose.yml` so that the same numbers apply on every run rather than
depending on what else the laptop was doing.

| | |
|---|---|
| CPU | AMD Ryzen 7 5700U — 8 cores, 16 threads |
| Memory | 16 GB, of which Docker's VM gets 8 GB |
| OS | Windows 11 Home, Docker Desktop 29.7.2 on the WSL2 backend |
| Generator | k6 v2.2.0, in a container on the same Docker network |

| Service | CPU limit | Memory limit |
|---|---|---|
| api | 2.0 | 512 MB |
| worker | 1.0 | 256 MB |
| postgres | 2.0 | 1 GB |
| redis | 1.0 | 256 MB |
| rabbitmq | 1.0 | 512 MB |

**Calibration first.** `make load-test-calibrate` ramps k6 against `/health` to
a thousand virtual users and reports what came back: **1,044 req/s** with a p95
of 1.1 s and no dropped requests.

That number is worth reading carefully, because it does not mean what the brief
assumes it will. `/health` checks PostgreSQL, Redis *and* RabbitMQ on every
call, which makes it more expensive than a sold-out refusal — so 1,044 req/s is
a ceiling on *that endpoint*, not on the generator. The campaign run below
sustained **3,366 req/s** through the same generator on the same machine, which
settles the question the calibration was asked: k6 was nowhere near its limit,
so the campaign figures are measurements of the API.

**The operating point, and why it is 50.** The load test holds a plateau and
asserts a p99 under 200 ms on the purchase path. Measured repeatedly:

| Peak VUs | p99, a sale | p99, a refusal | Verdict |
|---|---|---|---|
| 50 | **126 ms**, **106 ms** | **75 ms**, **77 ms** | passes, repeatably |
| 100 | 153 ms, 291 ms, 333 ms | 96 ms, 118 ms, 120 ms | passed once in three |
| 150 | 451 ms | 167 ms | fails |
| 300 | 363 ms | 168 ms | fails |

100 is where this gets interesting, and where it would have been tempting to
stop. The first run there passed at 153 ms, and had the measurement been taken
once it would be in this README as the operating point. Repeating it produced
291 ms and 333 ms — the difference being that Prometheus and Grafana were by
then running on the same laptop, scraping every five seconds.

**The variance is the finding.** A figure that passes on a quiet machine and
fails on a busy one is not a capacity number, it is a coin toss, and a CI gate
built on one teaches everybody to re-run the build until it goes green. 50 is
where the result repeats, so 50 is what is declared and what CI runs.

Two honest consequences. The first is that adding observability cost roughly
half the headroom on this machine — a real trade, paid in capacity for the
ability to see anything at all. The second is that a p99 over exactly one
hundred samples is close to "the slowest sale of the hundred", which is a weak
statistic however it comes out; the campaign is a hundred tickets, so there is
no larger sample to be had, and the number should be read with that in mind
rather than as a percentile in the usual sense.

### The load test turns the per-address rate limit off, on purpose

Every virtual user comes from one container and therefore one address, which is
exactly the traffic shape the per-address limit exists to stop. Left on, the
first campaign-scale run produced **250,917 rate-limited responses against 3,900
genuine sold-out refusals** — the run measured the limiter and almost nothing
else.

The limiter was working correctly. It was simply pointed at the generator. So
`make load-test-ramp` sets `RATE_LIMIT_IP=0` for the duration and leaves the
per-account limit on, which is the one doing the real work here anyway — the
README has said since phase 2 that the address limit is deliberately loose,
because the audience is a university behind a handful of NAT addresses.

This is worth stating plainly because it is the kind of change that looks like
tuning until a threshold goes green. The test for whether it is: the invariant
thresholds are untouched, and the run still sells exactly one hundred tickets.

### The run that gates the build

`make reset && make load-test-ramp`, at the declared operating point, unedited:

```
  █ THRESHOLDS
    checks
    ✓ 'rate==1' rate=100.00%
    http_req_failed
    ✓ 'rate<0.01' rate=0.00%
    http_reqs
    ✓ 'count>1000' count=132926
    purchase_duration{outcome:refused}
    ✓ 'p(99)<100' p(99)=40.54ms
    purchase_duration{outcome:sold}
    ✓ 'p(99)<200' p(99)=77.29ms
    refusals
    ✓ 'count>1000' count=132826
    server_errors
    ✓ 'count==0' count=0
    tickets_sold
    ✓ 'count==100' count=100
    undocumented_answers
    ✓ 'count==0' count=0

  █ TOTAL RESULTS
    checks_succeeded...: 100.00% 398778 out of 398778
    purchase_duration..: avg=12.83ms med=12.5ms  p(90)=16.89ms p(95)=20.06ms
      { outcome:sold }.: avg=22.25ms med=21.4ms  p(90)=35.14ms p(95)=43.95ms
    tickets_sold.......: 100
    rejected_stock_exhausted: 132826
    server_errors......: 0
    http_reqs..........: 132926  3323.398549/s
```

**Then the invariants, read from the source of truth rather than from k6:**

```
campaign invariants, read from the source of truth
  ok   live tickets                           100
  ok   people holding more than one ticket    0
  ok   available column                       0
  ok   redis stock                            0
  ok   redis buyers                           100
all invariants hold
```

That second step is not decoration, and it is not something k6 could do.
`tickets_sold` counts the answers the generator received; the invariant is what
the two datastores hold afterwards, and those are different claims. A system
that answered `202` a hundred and one times and then lost one to a failed write
would satisfy k6 and be broken.

The fairness rule in particular is **invisible to the generator**. Every virtual
user has its own identity, so a run in which one person was sold every ticket
would look flawless from k6 and be the worst outcome the system has. It can only
be checked by asking the database, which is what `make verify-campaign` does and
what CI runs immediately after the load test.

It has been checked the only way a check can be: by making it fail. Dropping
`purchases_one_live_ticket_per_user_idx` and inserting a second live ticket for
an existing buyer produces

```
  FAIL live tickets                           101 (want 100)
  FAIL people holding more than one ticket    1 (want 0)
```

and exit code 1. Worth noting what happened on the first attempt at that: the
database refused the insert outright, because the partial unique index added in
phase 2 is exactly the backstop that stops this reaching the table. The index
had to be dropped before the invariant could be broken at all.

### What the dashboard shows

`make up` brings up Prometheus and Grafana with the datasource and dashboard
provisioned from files — no clicking, and nothing saved in anybody's browser.
Grafana is on <http://localhost:3000> and opens straight onto this:

![The Grafana dashboard during a campaign](docs/grafana-dashboard.png)

Exported with `make dashboard`, which renders it headlessly rather than
screenshotting it, so the image above can be regenerated by anybody with one
command instead of being a picture of a particular afternoon.

Reading it left to right: the purchase p99 sitting at 141 ms under the 200 ms
line, throughput peaking at 1.1k req/s, exactly 100 tickets sold, no
compensations, every refusal accounted for as `stock_exhausted`, no 5xx at all,
the processing queue filling to 93 and draining, and the gap between how long a
ticket waited and how long fulfilling it took.

That last panel is the one worth pausing on. Fulfilment p95 is 2.48 s — the
simulated render, near enough exactly — while the queue wait p95 reaches 4.8
minutes. The work is not slow; the backlog is deep, because one worker at two
seconds a ticket drains a hundred of them in three and a half minutes. A
dashboard showing only the duration would report a perfectly healthy worker
throughout. `--scale worker=3` divides it.

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

### Why a SELECT followed by an UPDATE does not hold

**Context.** The obvious way to sell a ticket is to read the stock, decide, and
write. Phase 1 does exactly that, on purpose, so the failure can be measured
before it is fixed.

```sql
SELECT available FROM tickets WHERE campaign_id = $1;  -- 100
-- application decides: there is stock
UPDATE tickets SET available = available - 1 WHERE campaign_id = $1;
INSERT INTO purchases ...;
```

Each statement is individually atomic. The **sequence** is not. Postgres runs
at READ COMMITTED by default, which guarantees a statement never sees another
transaction's uncommitted work — and guarantees nothing whatsoever about a value
still being true by the time the application acts on it. The gap between reading
and writing is where every other request reads the same value.

```mermaid
sequenceDiagram
    participant A as Request A
    participant B as Request B
    participant DB as PostgreSQL
    Note over DB: available = 1
    A->>DB: SELECT available
    DB-->>A: 1
    B->>DB: SELECT available
    DB-->>B: 1
    Note over A,B: both concluded they have the last ticket
    A->>DB: UPDATE available = 0
    A->>DB: INSERT purchase
    B->>DB: UPDATE available = -1
    B->>DB: INSERT purchase
    Note over DB: 2 tickets sold, 1 existed
```

**Measured, not asserted.** With 500 virtual students against a 100 ticket
campaign, through the real HTTP API, on the same machine and the same command
for both columns — `make reset` first, generator inside the Docker network:

| | Phase 1 — SELECT then UPDATE | Phase 2 — one Lua script | Phase 3 — 202 and a worker |
|---|---|---|---|
| Tickets sold | **500** | **100** | **100** |
| Oversold | **400** | **0** | **0** |
| Users holding more than one | 0 | 0 | 0 |
| `available` in PostgreSQL afterwards | **−400** | 0 | 0 |
| Refused `stock_exhausted` | 0 | 400 | 400 |
| Requests that failed to get a response | 0 | 0 | 0 |
| p95 latency | 2.65 s | 1.01 s | **0.58 s** |
| p90 latency | 2.64 s | 0.77 s | **0.48 s** |
| Throughput | 176 req/s | 372 req/s | **564 req/s** |
| Wall clock for all 500 | 2.8 s | 1.3 s | **0.9 s** |

Phase 3 adds a synchronous `INSERT` and a confirmed publish to the request path
and comes out *faster*, which deserves an explanation rather than a victory lap.
Two things moved. The connection pool is now sized deliberately — sixteen
connections with four kept warm, against a default that depends on the machine
and dials on demand, so the hundred winners no longer queue behind a handshake
apiece at exactly the wrong moment. And the response no longer waits on anything
slow, because there is nothing slow left on that path: the two-second render
moved to the worker, which is the entire point of the phase.

What the table does *not* show is time to a finished ticket. That is now a
separate question with a separate answer — a single worker at `FULFILMENT_DELAY=2s`
takes about three and a half minutes to drain a hundred, and `--scale worker=3`
divides it. Answering "how long until my PDF exists?" with a number from this
table would be the kind of quiet dishonesty the 202 exists to avoid.

The phase 3 run, unedited:

```
     ✓ no server error
     ✓ answer is one of the documented outcomes
     ✓ the request was well formed

     checks_total.......: 1500    1691.563277/s
     checks_succeeded...: 100.00% 1500 out of 1500
     checks_failed......: 0.00%   0 out of 1500

     ✓ http_req_failed ....... rate<0.01  rate=0.00%

     rejected_stock_exhausted: 400
     tickets_sold............: 100
     http_req_duration.......: avg=259.31ms med=208.2ms p(90)=478.61ms p(95)=576.05ms
     iterations..............: 500     563.854426/s
```

and the state it left behind, in both systems:

```
redis     stock=0  buyers=100
postgres  100 live tickets, 0 users holding more than one, available=0
          (11 confirmed, 89 pending at the moment of the check — all 100
           confirmed once the worker caught up, with every queue empty and
           nothing in the dead letter queue)
```

Phase 1 sold not 101, not 140, but every ticket asked for: every request read
the same availability, concluded it had the last one, and got it. The
integration test reproduces it in miniature — 300 requests, 20 tickets, 300
sold — and **asserts** the failure, so that if it ever stops reproducing the
comparison above is known to be measuring nothing rather than quietly passing.

The latency column is the part worth reading twice, because it is the opposite
of what "we added a second datastore" suggests. Phase 1 was not slow because of
Postgres; it was slow because five hundred connections queued on one row's
locks. Phase 2 answers four hundred of those requests from Redis without
touching Postgres at all, so only the hundred winners ever open a transaction.
Correctness and latency moved in the same direction, which is the case for
Redis here — not that SQL could not have been made correct.

**Options.** `SELECT ... FOR UPDATE` to lock the row; a single conditional
`UPDATE ... WHERE available > 0` that is atomic by itself; or move the decision
out of Postgres entirely.

**Decision.** The decision moved to Redis — but the honest answer to "why not
just fix the SQL?" is that fixing the SQL *would work*. For 100 tickets a single
conditional `UPDATE ... WHERE available > 0` is correct and Postgres would not
break a sweat. Pretending otherwise would be inventing a problem to justify a
solution.

Redis earns its place for different reasons: it keeps the write burst off the
primary database, it keeps latency flat instead of degrading with lock
contention, and it makes the stock check and the one-per-user check a **single**
atomic operation over two different keys — which row locking on one table does
not give you.

The measurements above bear the second of those out more strongly than expected.
Adding a datastore made the p95 better, not worse, because four hundred of the
five hundred requests are now answered without opening a transaction at all.

**Consequences.** The invariant now lives outside the source of truth, so the two
can disagree. Everything interesting in this phase and phase 5 follows from that:
rebuilding Redis from Postgres at startup, and what happens when a process dies
between the two writes.

### Why one Lua script instead of WATCH/MULTI/EXEC

**Context.** Selling a ticket means establishing two facts and making two writes
that follow from them: this person does not already hold a ticket, there is
stock left, and therefore the counter drops by one and the person is recorded as
a holder. Two keys, two conditions, one decision. Any gap between them is a
race — either two requests both read `stock = 1`, or one person's two tabs both
pass the duplicate check.

**Options.**

*`DECR` and check the result.* Genuinely atomic, and the first thing that comes
to mind. It fails on both counts here: the stock goes negative before you can
correct it, so other clients observe a number that was never true, and it has
nothing to say about who the buyer is. The one-per-user check would still need
its own round trip, and that round trip is a race.

*WATCH/MULTI/EXEC — optimistic locking.* `WATCH` both keys, read them, decide,
queue the writes, `EXEC`. If either key changed in the meantime the transaction
aborts and the client retries. Native to Redis, no new language.

*A Lua script.* Redis executes it to completion before looking at another
command, so the whole decision is one indivisible step by construction.

**Decision.** Lua — [`purchase.lua`](internal/cache/scripts/purchase.lua).

The case against WATCH is that its cost scales with contention, and contention
is the entire problem. Five thousand clients watching the same key means nearly
every `EXEC` aborts; each retry is another round trip that re-reads and
re-decides, so the number of attempts per success climbs precisely when the
system is busiest. Optimistic locking is a good trade when conflicts are rare.
Here a conflict is the expected case.

There is a second, less obvious cost. `WATCH` is per-connection state, so a
client library has to pin a pooled connection for the length of a multi-round-
trip conversation. Five thousand concurrent purchases would hold five thousand
conversations open against a connection pool sized for far fewer.

The script is one round trip, no retries, and no pinned connection. It is sent
with `EVALSHA` and only shipped in full when the server answers `NOSCRIPT`, so
the body crosses the wire once per server lifetime rather than once per
purchase.

**Consequences.** Three, and the first is the one that matters.

Redis is single-threaded, which is exactly what makes the script atomic and
exactly what makes a slow script a global outage: while it runs, nothing else in
the server does. Every script here is O(1) except reconciliation, which is
O(buyers) and therefore chunks its writes rather than unpacking an unbounded
argument list in one call. "Keep the script short" is not style advice in Redis;
it is the price of the guarantee.

Lua is a second language in the codebase, and a worse one to debug — there is no
stepping through an `EVALSHA`. The scripts live in their own files under
[`internal/cache/scripts`](internal/cache/scripts) rather than as Go string
literals, so at least they read as code.

And a script may only touch keys in one Redis Cluster slot. The keys carry hash
tags (`campaign:{id}:stock`, `campaign:{id}:buyers`) so both always land on the
same node. On a single instance this changes nothing; without it, the first
attempt to run behind a cluster would fail.

### Why the stock is rebuilt from PostgreSQL on every start

**Context.** Redis holds the invariant, and Redis holds it in memory. Something
has to put the number there when a process starts.

**Options.** `SET stock 100` at boot, or compute it from the source of truth.

**Decision.** Compute it — and treat the one-line version as a bug rather than a
simplification, because that is what it is. Writing the campaign size at boot
means every deploy, crash, OOM kill and rolling restart refills the shelf, and
the tickets that come back out of it have already been sold. A campaign is
decided in a few minutes; the odds of no process restarting during it are not
odds worth taking.

So at startup, under a distributed lock, the service reads the campaign size and
every live purchase from PostgreSQL and writes both derived values into Redis in
a single script. PostgreSQL is the source of truth; Redis is a fast projection
of it, and this is the thing that makes that sentence true rather than
aspirational.

**Live means not cancelled, not "confirmed".** Since phase 3 a sale is recorded
before its ticket is generated, so at any instant some purchases are `pending`
and, if a worker gave up, some are `failed`. Every one of them is a seat
somebody is holding. Asking `status = 'confirmed'` instead would undercount by
however far the workers happen to be behind and hand those seats to other
people — which is why the query, the partial unique index and
`domain.Status.Live` all phrase it the same way. It is also the reason the API
writes the row itself rather than leaving it to the worker; that argument has
[its own section](#why-the-api-writes-the-purchase-not-the-worker).

**Rebuilding the buyer set is half of this, and the half that gets forgotten.**
The brief asks only for the stock, and a service that restores the count but not
the holders looks completely healthy: the counter is right, the arithmetic
works, nothing logs an error. What it has lost is its memory. Every one of the
forty people who already bought can buy again, the campaign quietly sells more
tickets than it has to fewer people than it should, and the one-per-user rule
stops existing halfway through with no symptom that points at it. Both halves
are restored here, in one script, so the state is never observably
half-rebuilt — a `DEL` followed by a separate `SADD` leaves a window in which
the set is empty and a purchase landing in it is granted to somebody who already
holds a ticket.

**Verified against the running stack, not only in a test.** Sell 40 of 100, wipe
Redis completely — a harder failure than a restart — and restart the API:

```
--- sell 40 ---
tickets_sold ......... 40
--- redis after 40 sales ---
stock=60  buyers=40
--- FLUSHALL, then restart the api ---
stock=60  buyers=40
```

```json
{"msg":"reconciled redis from postgres","campaign_id":"queima-2026",
 "remaining":60,"holders":40,"redis_had_state":false,"previous_remaining":0}
```

Sixty, not one hundred. And `student-1`, who bought before the wipe, is still
refused with `already_purchased` afterwards — which is the half that has no
symptom when it is missing.

**Re-run in the hardest case phase 3 introduces.** The forty sales above were
made with the worker *stopped*, so every one of them was still `pending` when
Redis was wiped — not one had been confirmed:

```
--- postgres after 40 sales, worker stopped ---
 pending | 40
--- FLUSHALL, then restart the api ---
stock=60  buyers=40
```

This is the measurement that decides the design. Had the worker been the one to
write these rows, PostgreSQL would have held *nothing*, reconciliation would
have computed a stock of one hundred and a buyer set of zero, and all forty
seats would have gone on sale again to people who could not have them. Starting
the worker afterwards confirmed all forty from messages that had outlived the
Redis wipe and the API restart, because the queue is durable and its messages
persistent.

**Consequences.** Startup now depends on PostgreSQL being reachable, which is a
real cost: the service cannot come up during a database outage. In exchange it
survives restarts mid-campaign without violating the invariant, which is the
trade worth making — a service that starts during an outage and sells tickets
twice is not usefully available.

The lock is a single `SET NX PX` on one Redis instance, and that is not Redlock.
If Redis failed over to a replica that had not yet received the key, two holders
could exist at once. It is enough here because the critical section is
idempotent — several instances recomputing the same numbers from the same source
of truth converge on the same answer — so the failure mode is duplicated work,
not a broken invariant. A lock protecting something that is not idempotent would
need more than this, and it is worth knowing which kind you have.

### What Redis persistence buys, and what it does not

**Context.** Redis is the only place the stock invariant is enforced. If it
restarts, what comes back?

**Options.** RDB snapshots, AOF, or both.

**Decision.** Both, with `appendfsync everysec`, configured explicitly in
[`docker-compose.yml`](docker-compose.yml) rather than left at the image
default.

The default is RDB alone, and the default schedule measures the gap between
snapshots in minutes. For a campaign decided in the first few seconds, a
snapshot from minutes ago is indistinguishable from no snapshot. AOF at
`everysec` bounds the loss at roughly one second of writes. `always` would bound
it at zero, and would put an `fsync` on the critical path of every purchase —
the one path this whole design spends effort keeping fast. One second of tickets
is recoverable. The latency is not.

Both are on because they answer different questions: the AOF is what replays
after a crash, and the RDB is the compact file worth copying somewhere else.

**Consequences, and the honest part.** None of this is what makes the system
correct after Redis dies. Losing a second of writes would leave Redis claiming
tickets that PostgreSQL says are sold — and the startup reconciliation above
overwrites it from PostgreSQL anyway, which it would do just as correctly if the
AOF were empty. The persistence narrows the window during which a *running*
Redis is wrong; the reconciliation is what closes it. Configuring persistence
and calling the durability problem solved would be the mistake here.

### Rate limiting, and which way each control fails

**Context.** Five thousand people arrive in a few seconds, and some of them are
scripts.

**Decision.** A sliding window over a sorted set of request timestamps
([`ratelimit.lua`](internal/cache/scripts/ratelimit.lua)), applied per account
and per address, answering `429` with a `Retry-After` header.

A fixed window counter is cheaper — one `INCR` and an expiry — but it allows a
full allowance at the end of one window and another immediately at the start of
the next, so the effective limit doubles across the boundary. On a campaign that
is decided in its first seconds, that boundary is the only part of the timeline
that matters, so the cheap option is cheap in exactly the wrong place. The cost
of the sliding window is memory proportional to the limit per caller, which is
why the key expires with the window.

**The per-address limit is deliberately loose, and per-address limiting is the
weaker of the two controls here.** The audience is a university: thousands of
students share a handful of NAT addresses, so a tight per-IP limit does not stop
an attacker, it stops a hall of residence. It is set to absorb the entire
expected burst from one address and to catch only a single machine going orders
of magnitude beyond human speed. This interacts with the load test, which drives
every virtual user from one container and therefore one address — raise `VUS`
above `RATE_LIMIT_IP` and the generator starts rate limiting itself. That is the
limiter working, and the k6 output counts the two refusal reasons separately so
the difference is visible rather than mysterious.

**The two controls fail in opposite directions, on purpose.** If Redis is
unreachable the rate limiter allows the request, and the purchase behind it
refuses. A rate limit protects the system from load; it enforces no invariant,
so turning a Redis blip into a blanket `429` for every caller invents a second
outage on top of the first. The purchase does enforce an invariant, and Redis is
the only place it is enforced, so a purchase that cannot consult Redis is one
nobody can prove is safe — it fails closed. Getting these the same way round
would be wrong twice.

`X-Forwarded-For` is ignored. It is trivially forged, and a limiter that a
caller can opt out of by inventing a new address every request is worse than no
limiter, because it is believed. Behind a real proxy this would have to read the
header at a hop count the proxy guarantees.

### Why the API writes the purchase, not the worker

**Context.** Phase 3 answers `202` and hands fulfilment to a worker. Somebody
has to write the row in PostgreSQL, and there are two readings of the brief.
Its own architecture diagram shows the worker doing it — the API touches Redis
and the queue, nothing else.

**Options.**

*The worker inserts.* The database stays entirely off the critical path, which
is the purest expression of what phase 2 spent Redis to achieve.

*The API inserts as `pending`, the worker settles it.* One synchronous `INSERT`
before the publish.

**Decision.** The API writes it, against the diagram.

Startup reconciliation reads this table to decide how much stock is left. If the
API writes nothing, restarting with N messages in flight recomputes the stock as
`total − confirmed` and hands back N tickets that are already sold, re-admitting
N people who already bought — which is exactly the phase 2 bug, re-entering
through the back door. A source of truth that will not hear about a sale for two
seconds is not one.

Two smaller things point the same way. The `202` hands the client a status URL,
and if the row does not exist yet the first thing they do with it is a `404` for
a ticket that does exist. And `purchases_one_live_ticket_per_user_idx` only
backstops the fairness rule while a live row exists — with worker-inserts, Redis
is the *sole* enforcement of one-per-user for the whole queue latency, with no
second opinion if the script or its keys ever went wrong.

**Consequences.** One `INSERT` on the critical path, and it is a smaller number
than it looks. The requirement is that the database not be *flooded with
thousands of simultaneous writes*; the Lua script refuses 4,900 of 5,000
requests without PostgreSQL ever seeing them, so this pool serves about 100
inserts across the entire campaign. What phase 3 actually removes from the
request path is the two-second render, which is the responsiveness requirement
as written. The cost is real but bounded, and the alternative trades away a
correctness property to avoid it.

### Why the client generates the idempotency key

**Context.** A client whose request times out has no way to tell whether it
succeeded. Sending it again is the only thing it can do.

**Options.** A key minted by the server per request, or one minted by the client
and sent in a header.

**Decision.** The client generates a UUID and sends `Idempotency-Key`. It is
required, not optional.

A server-generated key is a different key on every attempt, so it can only
recognise a message the *broker* redelivered — never a request the *client*
resent. That is the wrong failure to guard. The one worth protecting against is
the one the client can actually see.

Required rather than optional because this endpoint consumes a finite thing. A
request without a key is a request nobody can retry safely, and making it
optional leaves the guarantee depending on whether the client remembered to ask
for it.

The claim is a Lua script for the same reason the purchase is. Two attempts with
one key are routinely in flight together — the first was never cancelled, only
slow — and `GET` followed by `SET` leaves a gap where both read "nothing here".
That is the phase 1 race with different nouns.

**Consequences.** Three answers, treated differently on purpose. A claimed key
does the work and records what came out. A replayed key returns the recorded
response byte for byte, so a retry sees its own `202` rather than a plausible
reconstruction of one. A key still in flight gets `409`: there is no correlation
id to hand over yet, and asking again shortly is safe precisely because the key
makes it safe.

What is *kept* matters as much. A refusal is an answer and is stored — a retry
should hear the same decision, not race for a different one. A `5xx` is not, and
releases the key, because the caller's only recourse is to send the same request
again with the only key that could be recognised.

Unlike the rate limiter, this fails closed. A limiter that cannot answer costs
some protection from load; an idempotency store that cannot answer cannot tell a
first attempt from a retry, and letting the request through is a coin toss on
whether somebody is charged twice. Removing that coin toss is the entire point
of the mechanism.

Records are scoped to the user as well as the campaign, so nobody can read a
stranger's purchase by learning their key. The database holds the stricter rule
— a key names at most one purchase in the campaign at all — as a partial unique
index, which is what catches a replay after the cached response has expired.

### Why exactly-once delivery does not exist

**Context.** RabbitMQ can deliver the same message twice. The obvious wish is to
configure that away.

**Options.** There is no third option to weigh here, and that is the point: no
broker setting turns at-least-once into exactly-once, and any that claimed to
would be lying.

**Decision.** At-least-once delivery plus an idempotent consumer, which produces
the effect people mean when they say exactly-once.

Delivering a message and recording that it was handled are two writes to two
systems. Whichever order they happen in, a crash between them leaves the pair
inconsistent: acknowledge first and a crash loses the message, do the work first
and a crash redelivers it. No ordering closes the gap, because closing it would
need a distributed transaction spanning the broker and the database — which is
the problem, not the solution.

So the only real choice is *which* failure to have, and losing a ticket is worse
than doing the same harmless thing twice. This system acknowledges last,
everywhere: the worker settles the purchase and *then* acks; the consumer
publishes a retry and *then* acks. A crash in either window costs a duplicate.

**Consequences.** Everything downstream has to survive duplicates, and that has
to be a property of the *write* rather than of care taken by the caller.
`SettlePurchase` moves a row only if it is still pending, in a single statement,
so a second delivery finds nothing to do and reports success rather than
failure — which is what lets the consumer acknowledge it instead of retrying
forever. Two workers racing the same message reach the same place. An
integration test publishes one message twice, asserts both copies really were
delivered, and asserts one confirmed ticket came out.

The client-side `Idempotency-Key` is the same idea at the other end of the
system, and the two are not redundant: one protects against the broker
redelivering a message, the other against a person pressing Buy again.

### How a failed message is retried, and where it stops

**Context.** Fulfilment can fail for reasons that pass — a database failing
over, a pool briefly exhausted — and for reasons that never will.

**Options.** Requeue immediately; requeue after a delay; or give up at once.

**Decision.** Three attempts with growing waits, then a dead letter queue — plus
a way to skip straight to the end.

An immediate requeue is a busy loop: a dependency that is down stays down for
longer than the round trip it takes to fail again, so all three attempts are
spent inside a millisecond and the retry budget means nothing. RabbitMQ has no
delayed delivery without a plugin, so each wait is an ordinary queue with a
message TTL and no consumer, dead-lettering back onto the processing queue when
the time is up.

One queue per delay rather than one queue with per-message TTLs. A single queue
expires messages strictly in publication order, so one message asking for thirty
seconds parks at the head and every five-second message behind it inherits the
longer wait.

The attempt counter is ours rather than RabbitMQ's `x-death`. `x-death` counts
dead-letterings per queue, so with two tiers it holds two entries that each say
`1`, and the number this code wants is not in there without summing them and
knowing which queues to sum.

**Consequences.** A worst-case failure takes about thirty-five seconds to reach
the dead letter queue, which is the price of not hammering a dependency that is
already struggling. Some failures skip the waits entirely: a message naming a
purchase that does not exist, or one already cancelled, fails identically every
time, so it is marked permanent and parked immediately rather than spending two
waits proving what the first attempt already knew. Nothing consumes the dead
letter queue — its purpose is to stop and be looked at, and phase 5.3 is where a
compensation saga drains it.

### Graceful shutdown, and the number that overrides it

**Context.** The brief asks that a deploy not corrupt state: on `SIGTERM` the
API should stop accepting new requests and finish the ones in flight, and the
worker should finish the message in its hand before closing. Both processes do
exactly that. The API traps the signal before it opens a single dependency,
calls `srv.Shutdown` with `SHUTDOWN_TIMEOUT`, and then waits for the sweeper
pass to end rather than cutting it off holding the reconciliation lock. The
worker's consumer stops pulling deliveries and lets the message already
dispatched run to completion, because the handler's context is deliberately
detached from the one the signal cancels.

**The part that is easy to miss.** None of that is worth anything if something
kills the process first, and something always will. Whatever runs the
container sends `SIGTERM`, waits, and then sends `SIGKILL`; Docker's default
wait is **ten seconds**. Both processes here are configured to need more than
that — the API has fifteen seconds of drain budget before the sweeper wait, and
the worker's per-message budget is `FULFILMENT_DELAY + POSTGRES_TIMEOUT` plus a
margin, twelve seconds with the defaults. So the graceful shutdown was correct
in the code and unreachable in practice, on every `compose stop`, every
`restart`, and every recreate — including the API restart in `scripts/reset.sh`
that runs before each load test.

**Decision.** `stop_grace_period` is set explicitly on both services, above
what either process can take. It is a number to keep in step: raising
`SHUTDOWN_TIMEOUT` or `FULFILMENT_DELAY` without raising it puts the guarantee
back out of reach, silently.

**Consequences.** Being generous costs nothing, because a process that has
finished exits immediately and an idle one exits at once — the grace period is
a ceiling, not a delay. And the failure it prevents was survivable rather than
catastrophic: a killed worker leaves its message unacknowledged, so the broker
redelivers it and the idempotent handler absorbs the duplicate. A killed API is
worse, because a request cut between the Redis decrement and the PostgreSQL
write is the lost ticket this project is about — recovered by the sweeper or by
startup reconciliation, but caused by the very deploy that graceful shutdown was
there to make clean.

### Sizing the connection pool, with the right knobs

**Context.** The brief asks for an explicitly configured pool with its values
justified, and names `MaxOpenConns`, `MaxIdleConns` and `ConnMaxLifetime`.

**Decision.** Those are `database/sql` settings, and this project uses
`pgxpool`, so the knobs are `MaxConns`, `MinConns`, `MaxConnLifetime` and
`MaxConnIdleTime`. They do not mean quite the same things either: `MinConns` is
a floor the pool actively maintains, where `database/sql`'s idle count is only a
ceiling on what it keeps around.

| Setting | API | Worker | Why |
|---|---|---|---|
| `MaxConns` | 16 | 8 | See below |
| `MinConns` | 4 | 2 | The campaign starts at midnight with no warm-up |
| `MaxConnLifetime` | 30m | 30m | Recycle past a failover or a load balancer |
| `MaxConnIdleTime` | 5m | 5m | An idle connection is a backend process doing nothing |

`MaxConns` is small on purpose, and the instinct to raise it under load is the
wrong one. PostgreSQL serves each connection with a backend process, so past the
point where connections outnumber what the machine can genuinely run at once,
more of them buys more context switching and more lock contention for the same
throughput. What makes 16 generous here is upstream: Redis has already refused
everyone who was going to lose, so this pool sees roughly 100 inserts across a
whole campaign, plus the reconciliation reads at startup.

`MinConns` keeps four warm because a pool that dials on demand meets the
midnight burst with a handshake and a round trip per connection, inside the
first requests — exactly when the latency is being measured.

The worker's pool is smaller because it is not contended: it holds at most
`WORKER_PREFETCH` messages at a time and does one short write for each, so
anything above that number is connections that exist in order to be idle.

**Consequences.** These numbers are chosen for this campaign, not derived from
the machine. A deployment with a different ratio of instances to database would
have to revisit them, which is the honest state of any pool setting.

### Why the histogram buckets are chosen rather than inherited

**Context.** The load test fails CI when the p99 of a purchase exceeds 200 ms,
and the Grafana panel draws a line at the same place.

**Decision.** Explicit buckets, with 0.2 as a boundary.

Prometheus interpolates a quantile *within* whichever bucket it falls into. The
client library's default buckets jump straight from 0.1 to 0.25, so a p99 near
the threshold would be a straight-line guess across a gap wider than the
threshold itself — and the number deciding whether the build goes red would be
an estimate with a ±75 ms shrug in it.

**Consequences.** Fourteen buckets instead of eleven, and three sets of them,
because the questions are on different scales: HTTP requests in milliseconds,
fulfilment in seconds around a two-second render, and queue latency spanning
milliseconds to minutes. Sharing one set would put every fulfilment in the same
overflow bucket and report nothing at all.

### Why the load test disables the per-address rate limit

**Context.** Every virtual user comes from one container and therefore one
address — which is exactly the traffic the per-address limit exists to stop.

**Decision.** `make load-test-ramp` sets `RATE_LIMIT_IP=0` and leaves the
per-account limit on.

The first campaign-scale run with the limit in place produced **250,917
rate-limited responses against 3,900 genuine sold-out refusals**. It measured
the rate limiter, and almost nothing else. The limiter was working correctly; it
was pointed at the generator.

**Consequences.** This is the kind of change that looks like tuning until a
threshold goes green, so it is worth saying what makes it not that. The
invariant thresholds are untouched — the run still has to sell exactly one
hundred tickets, produce no 5xx and answer every request with a documented
code — and only the load *shaping* changed. The per-account limit, which the
README has called the one doing the real work since phase 2, stays on
throughout.

The honest cost: this run no longer exercises the per-address limiter at all.
It has its own integration test, which is where that behaviour is actually
checked.

### Why every threshold is paired with a count

**Context.** A k6 threshold over a metric with no samples **passes**.

**Decision.** Every threshold that could be satisfied by an empty metric sits
next to a counter assertion that cannot be.

This is not hypothetical. An earlier version of the load test tagged requests by
mutating `res.request.tags` after the response arrived — which throws, because
k6 fixes a request's tags when it is made. Every iteration aborted before its
checks ran, no samples were recorded, and the run reported a **clean green sweep
across every threshold**, including a p99 that had never seen a single request.

**Consequences.** `tickets_sold: count==100` guards the sale latency threshold,
`refusals: count>1000` guards the refusal one, `http_reqs: count>1000` proves
the script ran at all, and `undocumented_answers: count==0` is a counter this
project controls rather than a metric whose semantics k6 might rename — as it
did with `checks`, where the old name silently watches nothing.

The latency thresholds themselves are on a `Trend` recorded by hand rather than
on `http_req_duration`, because the outcome of a request is not knowable until
the response arrives and k6 will not accept a `count` threshold on a trend
anyway.

### Why coverage is measured in the integration job

**Context.** The reported figure was **31.2%**, and it was measured without
`-tags=integration`.

**Decision.** Measure it where the tag is on, with `-coverpkg=./...`. The number
is now **61.0%**.

The old figure was not merely low, it was upside down: `store`, `cache`,
`purchase`, `queue` and `fulfilment` carry the heaviest tests in the project and
every one of them reported **zero**, so the packages with the most testing
looked like the ones with none. `-coverpkg` is the other half — without it,
coverage credits only the package a test lives in, so the end-to-end suite that
drives the store and the queue through a real broker would count toward neither.

**Consequences.** Coverage now needs Docker, which is why it lives in the
integration job rather than in `verify`. There is still no badge, and there will
not be one until it is worth trusting: 61% is a description of what is covered,
not a target to raise.

### The lost ticket, and the three ways out of it

**Context.** The most interesting failure in the system, and the one the whole
project has been narrowing since phase 2. A sale is three writes to three
systems that fail independently:

```
1. The Lua script decrements the stock in Redis.   ✓ committed
2. The API records the purchase in PostgreSQL.     ✗ the process dies here
3. The API publishes to RabbitMQ.
```

The ticket has left the shelf, no record of it exists, and the client never got
an answer. Nothing in the system is looking for it. With a hundred tickets, a
handful of well-placed crashes closes the campaign having sold nothing.

**Options.**

*A — reservation with a TTL.* The script records that a ticket left the shelf,
and a job returns any reservation nobody came back for. Cheap, and needs care:
returning a ticket whose sale actually completed sells one seat twice.

*B — transactional outbox.* The intent to buy is written to PostgreSQL in the
same transaction as the purchase, and a separate process reads that table and
publishes. Removes the dual write outright — there is only one write, and either
both rows commit or neither does. The cost is a synchronous database write on
the critical path *before* the sale is decided, which is much of what phase 2
spent Redis to avoid.

*C — Redis Streams as the queue.* The decrement and the enqueue happen in the
same Lua script, so they are atomic by construction. Eliminates the problem
rather than recovering from it, and gives up RabbitMQ — the dead letter queue,
the routing, the management UI, the operational familiarity — to do it.

**Decision.** A, with the care spelled out.

B is the better answer to a different question. It removes a failure this system
can already detect, at the cost of putting the database back on the path phase 2
worked to keep it off, and the outbox poller has to be built and run either way.
C is genuinely elegant and the trade is too large: it would mean this project no
longer demonstrates a message broker, which is a stated goal, and Redis Streams
would then be holding both the invariant and the work queue with no second
system to reconcile against.

**Consequences, and where the care goes.**

The reservation is a sorted set scored by time, not a key per reservation with a
TTL. The second is the design Redis looks purpose-built for and it does not
work: keyspace notifications are fire and forget, published to whoever is
subscribed at that instant and recorded nowhere. A subscriber that is
restarting, briefly disconnected or merely slow never learns the key expired,
and neither does anyone else, ever — and the event that goes missing is the one
saying "this ticket was never sold, put it back". The failure mode of the
mechanism is exactly the failure it was chosen to fix, made permanent and
silent. Polling a sorted set is less elegant and cannot lose anything.

**Redis scores the reservation from its own clock**, not the caller's. The
comparison against the cutoff is what decides whether a ticket is taken back,
and timestamps from several API instances would be several clocks: a machine
running a few seconds fast would have its reservations swept early, releasing a
seat whose sale is still in flight.

**The database is asked before anything is released, every time.** A reservation
only says a ticket *left* the shelf; whether the sale completed is a fact only
the source of truth holds. And an unanswered query is not a no — a failed lookup
leaves the reservation exactly where it is, because "PostgreSQL has never heard
of this sale" and "PostgreSQL did not reply" are indistinguishable if the error
is ignored, and acting on the second hands back seats people are holding.

`RESERVATION_AGE` is the one setting in this project that can cause an oversell,
so the service **refuses to start** unless it is at least `REQUEST_TIMEOUT` plus
`POSTGRES_TIMEOUT`. Outlasting the request alone is not enough: a `COMMIT`
abandoned when the budget expired may still be applied by a server that never
heard the caller give up, and until it lands the sweeper's question — does
PostgreSQL know about this sale? — answers no about a sale that is about to
exist. The margin has to cover the database finishing work the request gave up
on, not just the request.

There is a second window — the row committed and the publish did not — and it is
swept differently. That purchase is a real sale whose seat is genuinely taken;
what it is missing is a message. So it is **republished, not released**, which
is safe because settling is already idempotent.

**Proven with fault injection, against the running stack.** `FAULT_INJECTION`
arms either window, and `FAULT_KILL` decides whether the sale is merely
abandoned or the process actually dies. The two are separated because they
demonstrate *different* recovery paths, which is the thing running this
exercise made obvious:

```bash
make up
FAULT_INJECTION=after-decrement REQUEST_TIMEOUT=5s   RESERVATION_AGE=15s SWEEP_INTERVAL=5s docker compose up -d --wait api

# three sales that die between the decrement and the record
purchase 1 -> http 500
purchase 2 -> http 500
purchase 3 -> http 500

stock: 97   reservations: 3   rows in postgres: 0
```

Fifteen seconds later, without anything being restarted:

```json
{"msg":"returned a ticket whose sale never completed","user_id":"doomed-2","open_for":17817575162}
{"msg":"sweeper recovered tickets that were stuck","examined":3,"released":3,"closed":0,"republished":0}
```

```
stock: 100   reservations: 0
```

**Now the same fault with `FAULT_KILL=true`, which is the harder one the brief
asks for — and it recovers by a different route.** The process really exits, and
it takes the sweeper with it, because the sweeper runs inside the API. With a
single instance nothing sweeps at all until it comes back:

```
{"level":"ERROR","msg":"fault injection: killing the process mid-sale","point":"after-decrement"}
api  Exited (1)
stock: 99   reservations: 1
```

The ticket stays stranded for as long as the API is down. What recovers it is
startup reconciliation, which rebuilds Redis from PostgreSQL before serving
anything:

```json
{"msg":"reconciled redis from postgres","remaining":100,"holders":0,
 "redis_had_state":true,"previous_remaining":99}
```

Two mechanisms for two failures, and worth being explicit about which covers
what: the sweeper handles a sale that died while the process lived — a recovered
panic, a lost reply, one instance crashing among several — and reconciliation
handles the process itself dying. Running more than one API instance collapses
the difference, since the survivors keep sweeping.

### Why the dead letter queue has a consumer

**Context.** A ticket whose fulfilment failed every attempt sat in the dead
letter queue holding its seat. The row said the seat was taken, so
reconciliation kept it taken, and nothing was going to change that: the person
who bought it could not buy again and nobody else could have it.

**Decision.** A compensation saga drains it — which is normally a bad idea, and
worth defending.

Draining a dead letter queue automatically is how a poisonous message gets
retried forever. It is safe here only because this **does not retry anything**.
The message is already known not to work; the ticket attached to it is what is
worth rescuing.

**Consequences.** Three writes in a deliberate order: PostgreSQL, then Redis,
then the notification. The same order the purchase path reverses in, and for the
same reason — if the second step fails, Redis is one ticket short of the truth
and reconciliation fixes it, where the other order leaves the ticket back on the
shelf while a live row still says whose it is.

Releasing is **not conditional** on the cancellation having changed anything. An
earlier attempt may have cancelled the row and then died before returning the
ticket, and skipping the release because the row was "already done" would strand
that ticket permanently — the one state nothing else revisits.

The notification is last and its failure is not the saga's failure. The ticket
is back and the row is cancelled; returning an error for a lost log line would
have the message redelivered and the whole thing run again.

Consumers take a settlement policy, because this queue needs the opposite of the
processing queue's. Sending a failure through the retry tiers would dead-letter
it back onto the *processing* queue and hand a message known not to work to the
workers all over again.

And the whole thing is idempotent, because the brief says an interviewer will
look for exactly this: **twenty simultaneous compensations of the same sale
return one ticket, not twenty.** A campaign of ten does not quietly become a
campaign of eleven.

### What happens when Redis disappears

**Context.** Redis is the only place the stock invariant is enforced. If it goes
away mid-campaign, the API can refuse everybody or sell on the assumption that
stock probably remains.

**Decision.** Fail closed, and it always has — this phase added the test that
proves it by taking the container away rather than asserting it.

For a campaign oversubscribed fifty to one, "probably" is a hundred angry people
and a refund process. Refusing everybody is a bad afternoon that ends when Redis
comes back.

**Consequences.** The refusal must not masquerade as a decision. `stock_exhausted`,
`already_purchased` and `campaign_not_found` all tell a caller that the system
considered their request and said no; an outage considered nothing, and dressing
it up as one of those is a lie a client acts on. It is a `500`, and the test
asserts it is none of the other three.

Note what this is *not*. The rate limiter fails **open** on the same outage,
because it protects the system from load rather than enforcing an invariant, and
turning a Redis blip into a blanket `429` invents a second outage on top of the
first. Two components, one dependency, opposite failure directions — decided by
what each is actually for.

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
- **Reconciliation can overwrite a purchase made during a rolling deploy.** It
  reads PostgreSQL and writes Redis under a lock, but purchases do not take
  that lock, so a sale committing on an already-running instance inside that
  window is not reflected in what gets written. The window is one database read
  wide, and the database read happens inside the lock to keep it that way. It
  is not zero.
- **The reconciliation lock is not Redlock.** One `SET NX PX` against one Redis
  instance; a failover to a replica that had not received the key would allow
  two holders. It is safe here only because the critical section is idempotent.
- **Per-address rate limiting is weak for this audience.** Thousands of students
  behind a handful of university NAT addresses means the limit has to be loose
  enough to be nearly inert. The per-account limit does the real work.
- **A retry is only recognised for as long as its response is cached.** Twenty
  four hours by default. Past that, the partial unique index on
  `(campaign_id, idempotency_key)` still catches the replay and answers
  `idempotency_key_replayed` — which is honest but less useful than the original
  `202`, because the response it refers to no longer exists anywhere.
- **An unconfirmed publish leaves the purchase pending until the sweeper
  republishes it.** If the broker never answers, the sale is deliberately not
  undone — it may have taken the message and been slow to say so. The sweeper
  republishes after `PENDING_AGE`, and the worker's idempotency absorbs the
  duplicate if the original message did arrive.
- **The compensation queue has no consumer.** The saga publishes to it and
  nothing reads it. That is the boundary of this project rather than an
  oversight: what belongs there is a notification service telling somebody their
  ticket failed, and inventing one would be scope with no design behind it.
- **The API does not reconnect to RabbitMQ.** The worker does, and loops until
  it succeeds; the API dials once at startup and, if the connection drops,
  publishes fail and purchases are reversed until it is restarted. The worker is
  the process where this matters — one that never reconnects is
  indistinguishable from a healthy one with an empty queue — and the API at
  least fails loudly rather than silently.
- Phase 1's naive purchase path is still in the tree, unused by the API. It is
  the baseline the table above is measured against, and an integration test
  asserts that it still oversells — if that ever stops reproducing, the
  comparison is measuring nothing and should fail loudly rather than quietly
  pass.
- **Load figures come from one laptop, and vary between runs on it.** The
  environment and its container limits are declared above, which makes the
  numbers comparable across phases on the same hardware and still meaningless as
  an absolute capacity claim. The p99 at 100 virtual users moved from 153 ms to
  333 ms purely because Prometheus and Grafana were running the second time.
- **A p99 over a hundred samples is barely a percentile.** The campaign is a
  hundred tickets, so the sale-latency threshold is close to an assertion about
  the single slowest sale. There is no larger sample to be had without changing
  the campaign, and the number should be read with that in mind.
- **CI runs the load test at a lower load than the declared measurement.** A
  hosted runner is a shared machine with invisible neighbours, so that job
  exists to catch a regression in the invariant — exactly 100 sold, no 5xx, no
  undocumented answer — rather than to publish a latency figure.
- **Coverage is 61%, and the number is a description rather than a target.**
  The commands, the wiring in `cmd/`, and the metrics package are largely
  uncovered; the domain logic and every failure path that can be provoked are
  not. There is no badge, and there will not be one until it says something
  worth trusting.
- **The sweeper is the one component that can cause an oversell.** Everything
  else here errs towards keeping a ticket off the shelf; this is the only thing
  whose job is to put one back. It is guarded by asking the source of truth
  before every release, by treating an unanswered query as "do not touch", and
  by a startup check that refuses a `RESERVATION_AGE` which does not outlast the
  request budget *and* one more database budget — but it is the piece to read
  first if a ticket is ever sold twice.
- **Fault injection is a test tool that ships in the binary.** `FAULT_INJECTION`
  is empty everywhere except a deliberate demonstration, and an unrecognised
  value refuses to start rather than disarming quietly. It is still a switch
  that makes production lose sales, and a system with real users would put it
  behind a build tag.
- **No circuit breaker.** The brief lists one as a bonus: if PostgreSQL is
  unavailable, the API should answer `503` immediately rather than accumulating
  timeouts. Today every call has its own budget, so a database outage produces
  slow failures rather than hung ones — bounded, but not fast. It is in the
  roadmap rather than done, and the honest reason is that the timeouts already
  bound the damage and a breaker would be the next improvement rather than a
  missing guarantee.
- **The dashboard has no alerting.** It shows the panels an incident would be
  read from, and nothing pages anybody. That is a deliberate stopping point for
  a project with no on-call rota, not an oversight.

## Why there is no public URL

There isn't one, and that is a decision rather than an omission.

This service has no user interface. The only route a browser can usefully open
is `/health`, which returns a JSON object; buying a ticket needs two headers, so
nobody is exercising the interesting path from an address bar. A link on a CV
would lead to a health check.

It would also sell out. The campaign is a hundred tickets and the entire point
is that it never sells a hundred and one — so the first person or crawler to
poke the endpoint starts draining it, and once the stock is gone the instance
answers `stock_exhausted` for good. Keeping a public demo alive would mean
either an unauthenticated reset endpoint, which is not a thing to put on the
internet, or a scheduled reset job existing only to prop up a link.

And the things worth showing cannot be seen from outside it. The Lua script
holding under five hundred concurrent buyers, reconciliation surviving a
`FLUSHALL`, the sweeper returning tickets a crash stranded, a p99 of 77 ms with
exactly a hundred sold, the dashboard — all of that lives in this README, the
test suite and the commit history, which is where anyone evaluating a backend
looks anyway.

What a deployment is normally *evidence of* — that the pipeline works end to
end — is already visible and continuously re-proven: every push runs four CI
jobs, including integration tests against real PostgreSQL, Redis and RabbitMQ
containers, and a load test that fails the build unless the campaign sells
exactly one hundred tickets and the invariants hold in both datastores
afterwards.

Running it is one command, and the load test, the fault injection and the
dashboard are all reproducible locally. That seemed a better use of the effort
than a sleeping free-tier instance cold-starting for fifty seconds to show
somebody a health check.

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
