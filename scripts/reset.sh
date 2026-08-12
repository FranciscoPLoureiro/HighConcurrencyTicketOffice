#!/usr/bin/env sh
# Return the running stack to the state a campaign starts in.
#
# Without this the second load test run sells zero tickets — the campaign is
# already gone — and CI goes red for no reason anybody can act on. That failure
# is worse than useless: it is indistinguishable from a real regression until
# somebody reads the output carefully.
#
# Deliberately *not* `compose down -v`. Destroying the volumes works and takes
# the better part of a minute, most of it spent initialising a PostgreSQL that
# was perfectly good. This truncates instead, and is quick enough to run before
# every measurement rather than once an afternoon.
#
# Order matters. PostgreSQL is emptied first, then Redis, then the queues, and
# only then does the API restart — because startup reconciliation reads
# PostgreSQL and writes Redis, so the source of truth has to be right before
# anything rebuilds a projection of it.
set -eu

COMPOSE="${COMPOSE:-docker compose}"

say() { printf '\033[36m==>\033[0m %s\n' "$1"; }

say "truncating the purchase history and restoring the stock counter"
# TRUNCATE rather than DELETE: no per-row work, and it resets in constant time
# regardless of how many campaigns have been run. The tickets row is updated
# rather than removed so that the campaign keeps its identity and its size.
$COMPOSE exec -T postgres psql -q -U tickets -d tickets <<'SQL'
TRUNCATE TABLE purchases;
UPDATE tickets SET available = total;
SQL

say "flushing redis"
# FLUSHDB, not FLUSHALL: this touches the database the service uses and leaves
# anything else on the instance alone. The stock and buyer set are rebuilt from
# PostgreSQL when the API restarts below, so wiping them here is safe by
# construction rather than by luck.
$COMPOSE exec -T redis redis-cli --no-raw FLUSHDB > /dev/null

say "purging the queues"
# Messages from a previous run name purchases that no longer exist. Left in
# place they would be delivered, fail as permanent, and fill the dead letter
# queue with confusing evidence during the next measurement.
for queue in ticket_processing_queue ticket_retry_5s ticket_retry_30s ticket_dead_letter_queue; do
  $COMPOSE exec -T rabbitmq rabbitmqctl purge_queue "$queue" > /dev/null 2>&1 ||
    say "  (queue $queue does not exist yet, skipping)"
done

say "restarting the api so it reconciles the stock from an empty database"
$COMPOSE restart api > /dev/null

# Waiting for healthy rather than sleeping. The API migrates, ensures the
# campaign and reconciles Redis before it listens, and a load test starting
# inside that window measures the startup rather than the campaign.
#
# Polled rather than `compose up --wait`, which is what this used to do and was
# wrong in a way that took a load test to notice: `up` re-resolves the
# environment and recreates the container if anything differs. Run from a shell
# without RATE_LIMIT_IP set, it quietly rebuilt the API with the default limit
# and put the rate limiter back in front of the load generator — so the run
# measured the limiter, and the only visible symptom was a suspiciously large
# number of 429s.
say "waiting for the api to come back"
for _ in $(seq 1 60); do
  if $COMPOSE ps --format '{{.Service}} {{.Health}}' 2>/dev/null | grep -q '^api healthy$'; then
    break
  fi
  sleep 1
done

say "restarting the worker so it reconnects to purged queues"
$COMPOSE restart worker > /dev/null

say "ready"
$COMPOSE exec -T redis redis-cli GET "campaign:{${CAMPAIGN_ID:-queima-2026}}:stock" |
  sed 's/^/    redis stock = /'
