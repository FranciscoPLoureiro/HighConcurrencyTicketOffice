# Measurement

How the numbers in the README were produced, on which machines, and what
moved when the machine changed.

## Measure the generator before believing the numbers

On Docker Desktop for Windows, driving the API through its published port
refused **57% of connections** at 500 virtual users — before a single request
reached the application. The API logged nothing, because nothing arrived.

Worse, the first version of the load test scored that run as a pass: k6 reports
status `0` for a request that never got a response, and the check was
`status < 500`. Both the measurement and the thing measuring it were wrong.

`make load-test-campaign-internal` runs the generator on the same Docker network
as the API, which removes the host's port forwarding from the path: 0% failures,
same hardware. Any number quoted in the README comes from that path.

## The test environment, declared

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
once it would be in the README as the operating point. Repeating it produced
291 ms and 333 ms — the difference being that Prometheus and Grafana were by
then running on the same laptop, scraping every five seconds.

**The variance is the finding.** A figure that passes on a quiet machine and
fails on a busy one is not a capacity number, it is a coin toss, and a CI gate
built on one teaches everybody to re-run the build until it goes green. 50 is
where the result repeats, so 50 is what is declared and what CI runs.

One honest consequence stands whatever the cause: a p99 over exactly one
hundred samples is close to "the slowest sale of the hundred", which is a weak
statistic however it comes out. The campaign is a hundred tickets, so there is
no larger sample to be had, and the number should be read with that in mind
rather than as a percentile in the usual sense.

### Where those milliseconds actually went

This README used to say that adding observability cost roughly half the
headroom on this machine, and stop there. That was a guess wearing the clothes
of a conclusion. The correlation was solid — the same load, the same laptop,
Prometheus and Grafana the only difference — and the mechanism behind it was
never measured. "The observability stack took the CPU and the API got less" is
what everybody assumes, this project included.

It has since been measured, with an eBPF wall-clock profiler written to answer
this question. It attaches to the scheduler tracepoints and splits the time a
thread spent into on-CPU, ready but waiting for a CPU, ready but stopped by its
own cgroup quota, and blocked. The decomposition closes to 100%, which is what
makes it an answer rather than a hint: the time has nowhere left to hide.

The API at 100 virtual users, with and without Prometheus and Grafana, the same
profiling protocol in both, busiest of seven 25-second windows in each:

| | without Prometheus & Grafana | with |
|---|---|---|
| threads | 5 | 6 |
| thread-time | 1m58.08s | 2m15.49s |
| on-CPU | 20.7% | 21.5% |
| **runqueue** (ready, no CPU) | **0.4%** | **0.5%** |
| **throttled** (quota exhausted) | **0.1%** | **0.1%** |
| blocked | 78.9% | 77.9% |

**The API was neither queued nor throttled.** Both are under one per cent in
both conditions, and on-CPU barely moves. The assumption was wrong: whatever
changed, it did not change how much CPU this service got, nor how long it spent
waiting for one.

What did change is every dependency it talks to. Measuring the interval from a
send to the next receive on the same socket, per destination:

| destination | without | with | change |
|---|---|---|---|
| redis, mean | 1.2 ms | 1.4 ms | +17% |
| postgres, mean | 5.4 ms | 6.9 ms | +28% |
| postgres, p99 | 81.9 ms | 114.7 ms | +40% |
| rabbitmq, mean | 1.6 ms | 2.5 ms | +56% |
| rabbitmq, p99 | 6.1 ms | 20.5 ms | **+236%** |

So the observability stack was paid for by PostgreSQL, Redis and RabbitMQ
rather than by the API, and that points somewhere different from where the
original sentence pointed: at where the dependencies run, not at how much CPU
the service is given.

**What this does not establish, said plainly.** The per-round-trip differences
are fractions of a millisecond, and they do not add up to the end-to-end gap.
Part of the increase is still unaccounted for. The honest reading is "the API
is not the bottleneck and its dependencies are measurably slower", not a
complete arithmetic of the extra milliseconds.

None of this changes the operating point. 50 is still where the result repeats
and still what CI runs — a number that moves when a neighbour starts is not a
capacity claim regardless of which component the neighbour slowed down. What
changes is that the sentence explaining it is now a measurement.

The tool, the method, and the several things that turned out to be measuring
the wrong thing, are in
[wallclock](https://github.com/FranciscoPLoureiro/wallclock).

## A second machine, and what it moved

Every figure above this point comes from one laptop, which the README has
always called the weakest thing about them. The same profile has now been run on a
second machine, unedited, and the point of doing it was not to publish a faster
p99 — a faster p99 on better hardware is the decoration the README opens by
warning about. The point was to find out which of these numbers describe the
system and which describe the laptop.

| | laptop (declared above) | desktop |
|---|---|---|
| CPU | Ryzen 7 5700U — 8c/16t, 15 W mobile | Ryzen 5 7600X — 6c/12t, 105 W desktop |
| Memory | 16 GB, 8 GB to Docker | 31 GB, ~15.2 GB to the WSL2 VM |
| OS | Windows 11 Home, Docker Desktop 29.7.2 | Windows 11 Pro, Docker Desktop 29.1.3 |
| Generator | k6 in a container on the compose network | same |

The container limits are unchanged, so each service gets the same *quantity* of
CPU on both machines and a faster one on the desktop. Two differences are not
controlled and are stated rather than hidden: the WSL2 VM was left at its
default share of memory instead of being capped to match, and the desktop has
twelve threads against the laptop's sixteen — with the compose limits summing to
9.0 and the k6 container uncapped, the desktop is the *tighter* of the two in
thread count while being much faster per thread. That combination turns out to
matter, below.

**Calibration.** `make load-test-calibrate`: 3,284 req/s at a p95 of 276 ms
against `/health`, no dropped requests, against 1,044 req/s and a p95 of 1.1 s
on the laptop. The generator is further from its limit here than there, so
everything below measures the API.

**The matrix.** `make load-test-ramp` at 50, 100 and 150 virtual users, three
runs at each, with and without Prometheus and Grafana, every run followed by
`make verify-campaign`. Twenty-five runs. The invariants held in all of them:
exactly 100 live tickets, nobody holding two, both stores agreeing.

| Peak VUs | p99 sale | p99 refusal | throughput | laptop verdict | desktop verdict |
|---|---|---|---|---|---|
| 50 | 7.05–50.62 ms | 13.3–23.3 ms | 5,749–6,382 req/s | passes | passes, 7/7 |
| 100 | 8.37–18.50 ms | 28.5–63.8 ms | 4,579–6,421 req/s | passed once in three | passes, 9/9 |
| 150 | 16.54–65.84 ms | 66.5–71.5 ms | 6,015–6,328 req/s | fails at 451 ms | passes, 9/9 |

**So the operating point was a property of the laptop.** 150 virtual users fail
there and pass nine times out of nine here. That is the least surprising thing
in this section and it is worth stating anyway, because the alternative — that
50 was a property of the code — is what a single-machine measurement leaves open.

### The observability result did not reproduce, and the reason is the interesting part

On the laptop, starting Prometheus and Grafana moved the p99 at 100 virtual
users from 153 ms to 291 ms and 333 ms. On the desktop, at 50, 100 and 150
virtual users, it moved nothing outside run-to-run noise.

The first version of this measurement said the opposite, and how it was wrong is
worth recording. Running all nine "without" runs first and all nine "with" runs
afterwards produced a clean-looking two-fold *improvement* with observability at
100 VUs — 62 ms of refusal p99 becoming 31 ms. Condition and run order were
perfectly aliased, and the system warms: throughput climbed across the session
regardless of condition. Repeating the "without" condition at the end, on a warm
machine, landed on 31.6, 32.2 and 36.7 ms — the "with" numbers. The effect was
the ordering.

| 100 VUs, p99 refusal | when | result |
|---|---|---|
| without observability | runs 5–7, cold | 61.5, 61.7, 63.8 ms |
| with observability | runs 14–16 | 28.5, 30.8, 32.9 ms |
| without observability, repeated | runs 23–25, warm | 31.6, 32.2, 36.7 ms |

This does not overturn the laptop measurement, and it lines up with the half of
the wallclock profile that was a *negative* result. That profile found the API
neither queued nor throttled — runqueue and cgroup throttling both under one per
cent, with and without the observability stack — and concluded the mechanism was
not CPU starvation. The desktop is the test of that conclusion: it has a tighter
thread budget than the laptop, and if the mechanism had been threads running out,
the effect should be *worse* here. It is absent. What the profiler measured
instead, dependencies answering more slowly, is what does not reproduce on
hardware where those dependencies have this much headroom.

### The gate is built on the noisier of the two metrics

Across all twenty-five runs the p99 of a sale ranged from 7.05 ms to 65.84 ms
with no relationship to the load or the condition — a run of 17.5, 65.8, 20.5 ms
at 150 VUs sits next to a run of 9.5, 7.1, 11.1 ms at 50. The p99 of a refusal,
over two hundred thousand samples instead of a hundred, tracks the load and
repeats inside five per cent.

The same pattern is visible in the laptop's own figures — 77 to 126 ms for sales
at 50 VUs against 96 to 120 ms for refusals at 100 — and it was easy to miss
there, because everything was slow enough that the sale p99 looked like a
signal. `purchase_duration{outcome:sold}` is the threshold that gates CI.

Faster hardware makes this worse rather than better, which is not obvious. The
hundred tickets are consumed in the first fraction of a second of the ramp, and
the quicker the machine the earlier that happens and the less concurrency those
hundred samples ever see. On the desktop the sale p99 comes in *below* the
refusal p99 at 50 VUs — 8.8 ms against 23.3 ms — which is backwards for a path
that writes to PostgreSQL and publishes to RabbitMQ against one that reads a
Redis key. It is not a fast write path; it is a sample taken before the load
arrives.

### What the desktop can say about capacity, which the p99 cannot

Throughput is flat from 50 virtual users upward and mean latency is linear in
concurrency:

| Peak VUs | throughput | mean latency | Little's Law, N/λ |
|---|---|---|---|
| 50 | 6,306 req/s | 6.78 ms | 7.93 ms |
| 100 | 6,305 req/s | 13.73 ms | 15.86 ms |
| 150 | 6,111 req/s | 21.30 ms | 24.55 ms |

Measured over predicted is 0.855, 0.866 and 0.868 — constant to within two per
cent, the gap being k6's per-iteration overhead, which sits outside
`http_req_duration`. A system at constant throughput whose mean latency rises in
proportion to the offered concurrency is saturated, and this one is saturated
before 50 virtual users. Its ceiling is roughly 6,300 requests per second with
the API capped at 2.0 CPUs, so that figure describes two Zen 4 cores rather than
the machine.

That is a capacity statement over a quarter of a million samples. Everything the
p99 of a sale has to say is over one hundred.

## The load test turns the per-address rate limit off, on purpose

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

## The run that gates the build

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

## What the dashboard shows

`make up` brings up Prometheus and Grafana with the datasource and dashboard
provisioned from files — no clicking, and nothing saved in anybody's browser.
Grafana is on <http://localhost:3000> and opens straight onto this:

![The Grafana dashboard during a campaign](grafana-dashboard.png)

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

## The Go toolchain runs in a container

`make test` and friends execute the Go toolchain inside `golang:1.26` rather
than on the host, so local builds and CI compile in the same environment. It is
also a hard requirement on the machine this was developed on, where Windows
Smart App Control blocks the unsigned Go toolchain binaries outright. Pass
`GO_LOCAL=1` to use a host toolchain instead:

```bash
make test GO_LOCAL=1
```

