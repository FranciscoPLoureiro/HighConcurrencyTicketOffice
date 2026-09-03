# Known limitations

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
  it succeeds; the API dials once at startup, and if that connection drops every
  publish fails until the process is restarted — including the sweeper's, since
  it runs inside the API and shares the connection. Those sales are left
  **pending**, not reversed: an unconfirmed publish may have reached the broker,
  so undoing it is the one thing that could sell a seat twice. The tickets stay
  off the shelf, the callers get a `500`, and a restart is what puts it right —
  reconciliation reads the pending rows as live and the sweeper republishes them
  on a working connection. Measured, with the broker stopped and restarted
  underneath a running API: fifteen sales left pending and still failing after
  the broker came back.
- Phase 1's naive purchase path is still in the tree, unused by the API. It is
  the baseline the comparison in the decisions is measured against, and an
  integration test asserts that it still oversells — if that ever stops reproducing, the
  comparison is measuring nothing and should fail loudly rather than quietly
  pass.
- **Load figures come from two machines, and vary between runs on both.** Each
  environment and its container limits are declared here, which makes the
  numbers comparable across phases on the same hardware and still meaningless as
  an absolute capacity claim. The p99 at 100 virtual users moved from 153 ms to
  333 ms on the laptop purely because Prometheus and Grafana were running the
  second time — [decomposed](MEASUREMENT.md#where-those-milliseconds-actually-went), and
  not in the way this project first assumed: the API was neither starved of CPU
  nor throttled, and every dependency it calls got slower instead. On the
  [desktop](MEASUREMENT.md#a-second-machine-and-what-it-moved) that effect is absent under a
  tighter thread budget, which narrows the mechanism without settling it.
- **A p99 over a hundred samples is barely a percentile, and it is taken at the
  wrong moment.** The campaign is a hundred tickets, so the sale-latency
  threshold is close to an assertion about the single slowest sale. Worse, those
  hundred sales are gone in the first fraction of a second of the ramp, before
  the plateau arrives — so the figure describes an almost unloaded system, and
  the faster the machine the truer that becomes. On the desktop it comes in
  *below* the refusal p99, which is backwards for a path that writes to
  PostgreSQL and publishes to RabbitMQ. Read it as a smoke check that the
  campaign sold out rather than as a latency gate; the refusal p99 beside it,
  over two hundred thousand samples, is the one that tracks load. There is no
  larger sale sample to be had without changing the campaign, and changing it
  would mean choosing a stock size calibrated to the speed of whichever machine
  happens to run the test.
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
exactly a hundred sold, the dashboard — all of that lives in these docs, the
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

